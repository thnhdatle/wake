// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
	"github.com/thnhdatle/wake/internal/testutil/fakestate"
)

// stKeeperState returns a state with one unfinished run (two tables) and a
// queue of one table.
func stKeeperState() pipeline.State {
	st := pipeline.NewState(stSlot, stSystem)
	st.ConsistentPoint = lsn.LSN(0x100)
	st.RunCounter = 3
	st.Snapshot = pipeline.SnapshotState{
		Runs: []pipeline.SnapshotRun{{
			ID: 3, Kind: pipeline.SnapshotKindTable, ChunkRows: 50, NextChunk: 4,
			Tables: []pipeline.TableProgress{{Table: "public.x"}, {Table: "public.y", Rows: 2}},
		}},
		Queue: []string{"public.a"},
	}
	return st
}

// stNewKeeper returns a keeper over a store holding st.
func stNewKeeper(t *testing.T, st *pipeline.State) (*StateKeeper, *fakestate.Store) {
	t.Helper()
	store := fakestate.NewWith(*st)
	k, err := NewStateKeeper(store, st)
	require.NoError(t, err)
	return k, store
}

// stSignaled reports, without blocking, whether ch holds a signal.
func stSignaled(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestNewStateKeeper_NilArguments(t *testing.T) {
	t.Parallel()
	st := pipeline.NewState(stSlot, stSystem)
	tests := []struct {
		name    string
		store   pipeline.StateStore
		initial *pipeline.State
	}{
		{name: "nil store", initial: &st},
		{name: "nil state", store: fakestate.New()},
		{name: "both nil"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, err := NewStateKeeper(tc.store, tc.initial)
			require.ErrorIs(t, err, ErrInvalidOption)
			assert.Nil(t, k)
		})
	}
}

func TestNewStateKeeper_CopiesInitialState(t *testing.T) {
	t.Parallel()
	st := stKeeperState()
	k, _ := stNewKeeper(t, &st)
	st.Snapshot.Queue[0] = "mutated"
	st.Snapshot.Runs[0].Tables[0].Table = "mutated"
	assert.Equal(t, []string{"public.a"}, k.State().Snapshot.Queue)
	assert.Equal(t, "public.x", k.State().Snapshot.Runs[0].Tables[0].Table)
}

func TestStateKeeper_State_ReturnsCopy(t *testing.T) {
	t.Parallel()
	st := stKeeperState()
	st.Schemas["public.x"] = pipeline.TableSchema{
		Version: 1, ReplicaIdentity: "d", Columns: []pipeline.SchemaColumn{{Name: "id"}},
	}
	k, _ := stNewKeeper(t, &st)
	got := k.State()
	assert.Equal(t, st, got)
	got.Snapshot.Queue[0] = "mutated"
	got.Snapshot.Runs[0].Tables[0].Rows = 99
	got.Schemas["public.x"].Columns[0].Name = "mutated"
	got.Schemas["other"] = pipeline.TableSchema{}
	assert.Equal(t, st, k.State())
}

func TestStateKeeper_SaveSchemas_RoundTrip(t *testing.T) {
	t.Parallel()
	st := stKeeperState()
	k, store := stNewKeeper(t, &st)
	rs := schema.RegistryState{
		Floor: 1_760_000_000,
		Tables: []schema.TableState{
			{
				Table: "public.a", Version: 4, Identity: schema.IdentityFull,
				Columns: []schema.Column{
					{Name: "id", TypeOID: 23, TypeMod: -1, TypeName: "int4", Key: true},
					{Name: "name", TypeOID: 1043, TypeMod: 36, TypeName: "varchar"},
				},
			},
			{Table: "public.b", Version: 9},
		},
	}
	require.NoError(t, k.SaveSchemas(t.Context(), rs))
	assert.Equal(t, 1, store.Saves())

	for name, got := range map[string]pipeline.State{"keeper": k.State(), "store": mustStoreState(t, store)} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out := RegistryState(&got)
			slices.SortFunc(out.Tables, func(a, b schema.TableState) int { return strings.Compare(a.Table, b.Table) })
			assert.Equal(t, rs, out)
			assert.Equal(t, st.Snapshot, got.Snapshot, "snapshot state is untouched")
		})
	}
}

func mustStoreState(t *testing.T, s *fakestate.Store) pipeline.State {
	t.Helper()
	st, ok := s.State()
	require.True(t, ok)
	return st
}

func TestStateKeeper_EnqueueTables(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		initial   []string
		tables    []string
		wantQueue []string
		wantSaves int
		wantSig   bool
	}{
		{
			name: "adds new tables", initial: nil, tables: []string{"public.a", "public.b"},
			wantQueue: []string{"public.a", "public.b"}, wantSaves: 1, wantSig: true,
		},
		{
			name: "dedupes within the call", tables: []string{"public.a", "public.b", "public.a"},
			wantQueue: []string{"public.a", "public.b"}, wantSaves: 1, wantSig: true,
		},
		{
			name: "skips already queued", initial: []string{"public.a"}, tables: []string{"public.a", "public.b"},
			wantQueue: []string{"public.a", "public.b"}, wantSaves: 1, wantSig: true,
		},
		{
			name: "nothing added", initial: []string{"public.a"}, tables: []string{"public.a"},
			wantQueue: []string{"public.a"}, wantSaves: 0, wantSig: false,
		},
		{name: "empty list", tables: nil, wantQueue: nil, wantSaves: 0, wantSig: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := pipeline.NewState(stSlot, stSystem)
			st.Snapshot.Queue = tc.initial
			k, store := stNewKeeper(t, &st)
			require.NoError(t, k.EnqueueTables(t.Context(), tc.tables))
			assert.Equal(t, tc.wantQueue, k.State().Snapshot.Queue)
			assert.Equal(t, tc.wantSaves, store.Saves())
			assert.Equal(t, tc.wantSig, stSignaled(k.Changed()))
			if tc.wantSaves > 0 {
				assert.Equal(t, tc.wantQueue, mustStoreState(t, store).Snapshot.Queue)
			}
		})
	}
}

func TestStateKeeper_EnqueueTables_CoalescesSignals(t *testing.T) {
	t.Parallel()
	st := pipeline.NewState(stSlot, stSystem)
	k, _ := stNewKeeper(t, &st)
	require.NoError(t, k.EnqueueTables(t.Context(), []string{"public.a"}))
	require.NoError(t, k.EnqueueTables(t.Context(), []string{"public.b"}))
	assert.True(t, stSignaled(k.Changed()))
	assert.False(t, stSignaled(k.Changed()), "two additions leave one pending signal")
}

func TestStateKeeper_EnqueueRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		triggerLSN  lsn.LSN
		queue       []string
		tables      []string
		commit      lsn.LSN
		wantQueue   []string
		wantTrigger lsn.LSN
		wantSaves   int
		wantSig     bool
	}{
		{
			name: "first request", tables: []string{"public.a"}, commit: 0x10,
			wantQueue: []string{"public.a"}, wantTrigger: 0x10, wantSaves: 1, wantSig: true,
		},
		{
			name: "later request appends", triggerLSN: 0x10, queue: []string{"public.a"},
			tables: []string{"public.b", "public.c"}, commit: 0x20,
			wantQueue: []string{"public.a", "public.b", "public.c"}, wantTrigger: 0x20, wantSaves: 1, wantSig: true,
		},
		{
			name: "same table again is queued again", triggerLSN: 0x10, queue: []string{"public.a"},
			tables: []string{"public.a"}, commit: 0x20,
			wantQueue: []string{"public.a", "public.a"}, wantTrigger: 0x20, wantSaves: 1, wantSig: true,
		},
		{
			name: "replay at the trigger LSN", triggerLSN: 0x20, queue: nil,
			tables: []string{"public.a"}, commit: 0x20, wantTrigger: 0x20,
		},
		{
			name: "replay before the trigger LSN", triggerLSN: 0x20, queue: nil,
			tables: []string{"public.a"}, commit: 0x10, wantTrigger: 0x20,
		},
		{
			name: "empty table list", triggerLSN: 0x10, tables: nil, commit: 0x20, wantTrigger: 0x10,
		},
		{
			name: "empty slice", triggerLSN: 0x10, tables: []string{}, commit: 0x20, wantTrigger: 0x10,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := pipeline.NewState(stSlot, stSystem)
			st.Snapshot.Queue, st.Snapshot.TriggerLSN = tc.queue, tc.triggerLSN
			k, store := stNewKeeper(t, &st)
			require.NoError(t, k.EnqueueRequests(t.Context(), tc.tables, tc.commit))
			got := k.State().Snapshot
			assert.Equal(t, tc.wantQueue, got.Queue)
			assert.Equal(t, tc.wantTrigger, got.TriggerLSN)
			assert.Equal(t, tc.wantSaves, store.Saves())
			assert.Equal(t, tc.wantSig, stSignaled(k.Changed()))
		})
	}
}

func TestStateKeeper_NextWork(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		st := pipeline.NewState(stSlot, stSystem)
		k, _ := stNewKeeper(t, &st)
		w, ok := k.nextWork()
		assert.False(t, ok)
		assert.Equal(t, work{}, w)
	})
	t.Run("runs before queue", func(t *testing.T) {
		t.Parallel()
		st := stKeeperState()
		k, _ := stNewKeeper(t, &st)
		w, ok := k.nextWork()
		require.True(t, ok)
		require.NotNil(t, w.run)
		assert.Equal(t, st.Snapshot.Runs[0], *w.run)
		assert.Empty(t, w.queued)
	})
	t.Run("queue head without runs", func(t *testing.T) {
		t.Parallel()
		st := pipeline.NewState(stSlot, stSystem)
		st.Snapshot.Queue = []string{"public.a", "public.b"}
		k, _ := stNewKeeper(t, &st)
		w, ok := k.nextWork()
		require.True(t, ok)
		assert.Nil(t, w.run)
		assert.Equal(t, "public.a", w.queued)
	})
	t.Run("run is a copy", func(t *testing.T) {
		t.Parallel()
		st := stKeeperState()
		k, _ := stNewKeeper(t, &st)
		w, ok := k.nextWork()
		require.True(t, ok)
		w.run.Tables[0].Rows = 99
		assert.Equal(t, st, k.State())
	})
}

func TestStateKeeper_DropQueued(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		queue     []string
		table     string
		wantQueue []string
		wantSaves int
	}{
		{
			name: "head matches", queue: []string{"public.a", "public.b"}, table: "public.a",
			wantQueue: []string{"public.b"}, wantSaves: 1,
		},
		{name: "last entry", queue: []string{"public.a"}, table: "public.a", wantQueue: []string{}, wantSaves: 1},
		{
			name: "not the head", queue: []string{"public.a", "public.b"}, table: "public.b",
			wantQueue: []string{"public.a", "public.b"},
		},
		{name: "not queued", queue: []string{"public.a"}, table: "public.z", wantQueue: []string{"public.a"}},
		{name: "empty queue", table: "public.a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := pipeline.NewState(stSlot, stSystem)
			st.Snapshot.Queue = tc.queue
			k, store := stNewKeeper(t, &st)
			require.NoError(t, k.dropQueued(t.Context(), tc.table))
			assert.ElementsMatch(t, tc.wantQueue, k.State().Snapshot.Queue)
			assert.Len(t, k.State().Snapshot.Queue, len(tc.wantQueue))
			assert.Equal(t, tc.wantSaves, store.Saves())
		})
	}
}

func TestStateKeeper_StartRun(t *testing.T) {
	t.Parallel()
	t.Run("pops the head", func(t *testing.T) {
		t.Parallel()
		st := pipeline.NewState(stSlot, stSystem)
		st.RunCounter = 4
		st.Snapshot.Queue = []string{"public.a", "public.b"}
		k, store := stNewKeeper(t, &st)
		run, err := k.startRun(t.Context(), "public.a", 250)
		require.NoError(t, err)
		assert.Equal(t, pipeline.SnapshotRun{
			ID: 5, Kind: pipeline.SnapshotKindTable, ChunkRows: 250,
			Tables: []pipeline.TableProgress{{Table: "public.a"}},
		}, run)
		got := k.State()
		assert.Equal(t, []string{"public.b"}, got.Snapshot.Queue)
		assert.Equal(t, []pipeline.SnapshotRun{run}, got.Snapshot.Runs)
		assert.Equal(t, uint64(5), got.RunCounter)
		assert.Equal(t, got, mustStoreState(t, store))
	})
	t.Run("keeps a queue whose head differs", func(t *testing.T) {
		t.Parallel()
		st := pipeline.NewState(stSlot, stSystem)
		st.Snapshot.Queue = []string{"public.a"}
		k, _ := stNewKeeper(t, &st)
		run, err := k.startRun(t.Context(), "public.z", 10)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), run.ID)
		assert.Equal(t, []string{"public.a"}, k.State().Snapshot.Queue)
	})
	t.Run("appends to existing runs", func(t *testing.T) {
		t.Parallel()
		st := stKeeperState()
		k, _ := stNewKeeper(t, &st)
		run, err := k.startRun(t.Context(), "public.a", 10)
		require.NoError(t, err)
		assert.Equal(t, uint64(4), run.ID)
		runs := k.State().Snapshot.Runs
		require.Len(t, runs, 2)
		assert.Equal(t, uint64(3), runs[0].ID)
		assert.Equal(t, uint64(4), runs[1].ID)
		assert.Empty(t, k.State().Snapshot.Queue)
	})
}

func TestStateKeeper_AllocChunk(t *testing.T) {
	t.Parallel()
	st := stKeeperState()
	k, store := stNewKeeper(t, &st)
	for want := uint64(4); want < 7; want++ {
		got, err := k.allocChunk(t.Context(), 3)
		require.NoError(t, err)
		assert.Equal(t, want, got, "returns the previous counter")
		assert.Equal(t, want+1, k.State().Snapshot.Runs[0].NextChunk)
		assert.Equal(t, want+1, mustStoreState(t, store).Snapshot.Runs[0].NextChunk, "persisted before use")
	}
}

func TestStateKeeper_AllocChunk_Concurrent(t *testing.T) {
	t.Parallel()
	st := stKeeperState()
	k, _ := stNewKeeper(t, &st)
	const n = 24
	got := make([]uint64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			c, err := k.allocChunk(t.Context(), 3)
			assert.NoError(t, err)
			got[i] = c
		})
	}
	wg.Wait()
	slices.Sort(got)
	for i, c := range got {
		assert.Equal(t, uint64(4+i), c)
	}
	assert.Equal(t, uint64(4+n), k.State().Snapshot.Runs[0].NextChunk)
}

func TestStateKeeper_SetProgress(t *testing.T) {
	t.Parallel()
	st := stKeeperState()
	k, store := stNewKeeper(t, &st)
	pos := json.RawMessage("[42]")
	require.NoError(t, k.setProgress(t.Context(), 3, "public.y", pos, 40, true))
	pos[1] = '9' // the keeper must have cloned the position

	got := k.State().Snapshot.Runs[0].Tables
	assert.Equal(t, pipeline.TableProgress{Table: "public.x"}, got[0], "other tables are untouched")
	assert.Equal(t, pipeline.TableProgress{
		Table: "public.y", Position: json.RawMessage("[42]"), Rows: 40, Done: true,
	}, got[1])
	assert.Equal(t, got, mustStoreState(t, store).Snapshot.Runs[0].Tables)
	assert.Equal(t, uint64(4), k.State().Snapshot.Runs[0].NextChunk, "chunk counter is untouched")
}

func TestStateKeeper_FinishRun(t *testing.T) {
	t.Parallel()
	st := stKeeperState()
	st.Snapshot.Runs = append(st.Snapshot.Runs, pipeline.SnapshotRun{ID: 4, Kind: pipeline.SnapshotKindTable})
	k, store := stNewKeeper(t, &st)
	require.NoError(t, k.finishRun(t.Context(), 3))
	runs := k.State().Snapshot.Runs
	require.Len(t, runs, 1)
	assert.Equal(t, uint64(4), runs[0].ID)
	assert.Equal(t, runs, mustStoreState(t, store).Snapshot.Runs)
	require.NoError(t, k.finishRun(t.Context(), 4))
	assert.Empty(t, k.State().Snapshot.Runs)
}

func TestStateKeeper_UnknownRunOrTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		call func(k *StateKeeper) error
	}{
		{name: "allocChunk", call: func(k *StateKeeper) error {
			_, err := k.allocChunk(t.Context(), 99)
			return err
		}},
		{name: "setProgress unknown run", call: func(k *StateKeeper) error {
			return k.setProgress(t.Context(), 99, "public.x", nil, 1, false)
		}},
		{name: "setProgress unknown table", call: func(k *StateKeeper) error {
			return k.setProgress(t.Context(), 3, "public.zzz", nil, 1, false)
		}},
		{name: "finishRun", call: func(k *StateKeeper) error { return k.finishRun(t.Context(), 99) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := stKeeperState()
			k, store := stNewKeeper(t, &st)
			require.ErrorIs(t, tc.call(k), ErrUnknownRun)
			assert.Equal(t, st, k.State())
			assert.Zero(t, store.Saves())
		})
	}
}

func TestStateKeeper_SaveFailureLeavesStateUnchanged(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		call func(k *StateKeeper) error
	}{
		{name: "SaveSchemas", call: func(k *StateKeeper) error {
			rs := schema.RegistryState{Floor: 5, Tables: []schema.TableState{{Table: "public.q", Version: 1}}}
			return k.SaveSchemas(t.Context(), rs)
		}},
		{name: "EnqueueTables", call: func(k *StateKeeper) error {
			return k.EnqueueTables(t.Context(), []string{"public.new"})
		}},
		{name: "EnqueueRequests", call: func(k *StateKeeper) error {
			return k.EnqueueRequests(t.Context(), []string{"public.new"}, 0x50)
		}},
		{name: "dropQueued", call: func(k *StateKeeper) error { return k.dropQueued(t.Context(), "public.a") }},
		{name: "startRun", call: func(k *StateKeeper) error {
			_, err := k.startRun(t.Context(), "public.a", 10)
			return err
		}},
		{name: "allocChunk", call: func(k *StateKeeper) error {
			_, err := k.allocChunk(t.Context(), 3)
			return err
		}},
		{name: "setProgress", call: func(k *StateKeeper) error {
			return k.setProgress(t.Context(), 3, "public.x", json.RawMessage("[1]"), 1, false)
		}},
		{name: "finishRun", call: func(k *StateKeeper) error { return k.finishRun(t.Context(), 3) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := stKeeperState()
			k, store := stNewKeeper(t, &st)
			store.FailSave(errStBoom)
			require.ErrorIs(t, tc.call(k), errStBoom)
			assert.Equal(t, st, k.State(), "a failed save does not change the current state")
			assert.False(t, stSignaled(k.Changed()), "no signal without a save")
			assert.Equal(t, st, mustStoreState(t, store))

			store.FailSave(nil)
			require.NoError(t, tc.call(k), "the keeper recovers once saves work")
			assert.NotEqual(t, st, k.State())
		})
	}
}
