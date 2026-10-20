// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/fakecatalog"
	"github.com/thnhdatle/wake/internal/testutil/fakechunk"
	"github.com/thnhdatle/wake/internal/testutil/fakerepl"
	"github.com/thnhdatle/wake/internal/testutil/fakestate"
)

var (
	errStBoom = errors.New("snapshot test: boom")

	stNow       = time.UnixMilli(1_760_000_123_456)
	stClockTime = time.Unix(1_760_000_000, 0)
)

const (
	stConsistent = lsn.LSN(0x300)
	stConfirmed  = lsn.LSN(0x250)
	stSnapName   = "00000003-0000001B-1"
)

// stSaved is one successful Save and how many slots had been created by then.
type stSaved struct {
	state   pipeline.State
	created int
}

// stRecStore records every saved state in order; failOn makes the n-th Save
// (1-based) fail.
type stRecStore struct {
	inner *fakestate.Store
	repl  *fakerepl.Source

	mu     sync.Mutex // protects the fields below
	saves  []stSaved
	calls  int
	failOn int
}

func (s *stRecStore) Load(ctx context.Context) (pipeline.State, error) {
	return s.inner.Load(ctx) //nolint:wrapcheck // test double
}

func (s *stRecStore) Save(ctx context.Context, st pipeline.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failOn == s.calls {
		return errStBoom
	}
	if err := s.inner.Save(ctx, st); err != nil {
		return err //nolint:wrapcheck // test double
	}
	created, _ := s.repl.Slots()
	s.saves = append(s.saves, stSaved{state: st.Clone(), created: len(created)})
	return nil
}

func (s *stRecStore) Saved() []stSaved {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stSaved(nil), s.saves...)
}

// stTables is a TableSource returning fixed tables.
type stTables struct {
	tables []Table
	err    error
	calls  int
}

func (s *stTables) CapturedTables(context.Context) ([]Table, error) {
	s.calls++
	return s.tables, s.err
}

// stFlakyChunks fails the failAt-th OpenExported call (1-based).
type stFlakyChunks struct {
	pipeline.ChunkReader
	failAt int
	n      int
}

func (c *stFlakyChunks) OpenExported(ctx context.Context, name string) (pipeline.ChunkSession, error) {
	c.n++
	if c.n == c.failAt {
		return nil, errStBoom
	}
	return c.ChunkReader.OpenExported(ctx, name) //nolint:wrapcheck // test double
}

func stTable(name string) Table {
	return Table{Name: "public." + name, Schema: "public", Table: name}
}

// stEnv bundles the fakes of a Prepare call.
type stEnv struct {
	repl   *fakerepl.Source
	cat    *fakecatalog.Catalog
	inner  *fakestate.Store
	store  *stRecStore
	chunks *fakechunk.Reader
	tables *stTables
	clk    *clock.Fake
	logger *slog.Logger
	logs   *bytes.Buffer
}

// newStEnv returns an environment without slot and without state file whose
// slot creation yields stConsistent and stSnapName.
func newStEnv() *stEnv {
	e := &stEnv{
		repl:   fakerepl.New(1),
		cat:    fakecatalog.New(),
		inner:  fakestate.New(),
		chunks: fakechunk.New(),
		tables: &stTables{tables: []Table{stTable("orders"), stTable("items"), stTable("users")}},
		clk:    clock.NewFake(stClockTime),
		logs:   &bytes.Buffer{},
	}
	e.logger = testutil.DiscardLogger()
	e.repl.SetSystem(pipeline.SystemInfo{SystemID: stSystem})
	e.repl.SetSlot(pipeline.SlotInfo{ConsistentPoint: stConsistent, SnapshotName: stSnapName}, nil)
	e.cat.SetCurrentTime(stNow, nil)
	e.store = &stRecStore{inner: e.inner, repl: e.repl}
	return e
}

// withState stores st and a slot at stConfirmed (exists) in the environment.
func (e *stEnv) withState(st *pipeline.State) *stEnv {
	e.inner = fakestate.NewWith(*st)
	e.store.inner = e.inner
	return e
}

func (e *stEnv) withSlot() *stEnv {
	e.cat.SetSlot(pipeline.SlotStatus{Exists: true, ConfirmedFlush: stConfirmed, WALStatus: "reserved"}, nil)
	return e
}

func (e *stEnv) deps() *PrepareDeps {
	return &PrepareDeps{
		Slots: e.repl, Catalog: e.cat, Store: e.store, Chunks: e.chunks, Tables: e.tables,
		Clock: e.clk, Logger: e.logger,
	}
}

func stCfg() PrepareConfig {
	return PrepareConfig{SlotName: stSlot, Mode: ModeInitial, ChunkRows: 500, ParallelTables: 2}
}

// stOwnedState returns a state owned by the test slot and system with a
// consistent point, two schema versions, an unfinished run and a queue.
func stOwnedState() pipeline.State {
	st := pipeline.NewState(stSlot, stSystem)
	st.ConsistentPoint, st.ConsistentPointTsMs = lsn.LSN(0x100), 42
	st.RunCounter = 7
	st.Schemas = map[string]pipeline.TableSchema{
		"public.orders": {
			Version: 5, ReplicaIdentity: "d",
			Columns: []pipeline.SchemaColumn{{Name: "id", Type: "int4", TypeOID: 23, TypeMod: -1, Key: true}},
		},
		"public.items": {Version: 2_000_000_000},
	}
	st.Snapshot = pipeline.SnapshotState{
		Runs: []pipeline.SnapshotRun{{
			ID: 7, Kind: pipeline.SnapshotKindTable, ChunkRows: 100, NextChunk: 3,
			Tables: []pipeline.TableProgress{
				{Table: "public.orders", Position: []byte("[10]"), Rows: 10},
				{Table: "public.done", Rows: 5, Done: true},
			},
		}},
		Queue:      []string{"public.users"},
		TriggerLSN: lsn.LSN(0x90),
	}
	return st
}

func TestPrepare_FirstStartInitial(t *testing.T) {
	t.Parallel()
	e := newStEnv()
	plan, err := Prepare(t.Context(), e.deps(), stCfg())
	require.NoError(t, err)

	assert.Equal(t, ActionCreateSlot, plan.Decision.Action)
	assert.True(t, plan.Decision.Initial)
	assert.Equal(t, stSystem, plan.SystemID)
	assert.Equal(t, stConsistent, plan.StartLSN)

	saved := e.store.Saved()
	require.Len(t, saved, 2)
	first, second := saved[0], saved[1]
	assert.Equal(t, 0, first.created, "the first save precedes CreateSlot")
	assert.Equal(t, 1, second.created, "the second save follows CreateSlot")
	assert.Equal(t, lsn.InvalidLSN, first.state.ConsistentPoint)
	assert.Equal(t, stNow.UnixMilli(), first.state.ConsistentPointTsMs)
	assert.Equal(t, stSlot, first.state.SlotName)
	assert.Equal(t, stSystem, first.state.SystemID)
	require.Len(t, first.state.Snapshot.Runs, 1)
	run := first.state.Snapshot.Runs[0]
	assert.Equal(t, uint64(1), run.ID)
	assert.Equal(t, pipeline.SnapshotKindInitial, run.Kind)
	assert.Equal(t, 500, run.ChunkRows)
	assert.Equal(t, []pipeline.TableProgress{
		{Table: "public.orders"}, {Table: "public.items"}, {Table: "public.users"},
	}, run.Tables)
	assert.Equal(t, uint64(1), first.state.RunCounter)

	assert.Equal(t, stConsistent, second.state.ConsistentPoint)
	assert.Equal(t, stNow.UnixMilli(), second.state.ConsistentPointTsMs)
	assert.Equal(t, first.state.Snapshot, second.state.Snapshot)
	assert.Equal(t, second.state, plan.State)
	stored, ok := e.inner.State()
	require.True(t, ok)
	assert.Equal(t, plan.State, stored)

	created, dropped := e.repl.Slots()
	assert.Equal(t, []string{stSlot}, created)
	assert.Empty(t, dropped)

	require.NotNil(t, plan.Initial)
	assert.Equal(t, run, plan.Initial.Run)
	assert.Equal(t, stConsistent, plan.Initial.ConsistentPoint)
	assert.Equal(t, stNow.UnixMilli(), plan.Initial.TsMs)
	assert.Equal(t, e.tables.tables, plan.Initial.Tables)
	assert.Len(t, plan.Initial.Sessions, 2, "min(parallel_tables, tables)")
	opened, closed := e.chunks.Sessions()
	assert.Equal(t, []string{stSnapName, stSnapName}, opened)
	assert.Zero(t, closed)

	require.NoError(t, plan.Initial.Close(t.Context()))
	_, closed = e.chunks.Sessions()
	assert.Equal(t, 2, closed)
	assert.Empty(t, plan.Initial.Sessions)
	require.NoError(t, plan.Initial.Close(t.Context()), "Close is idempotent")
}

func TestPrepare_FirstStartSessionCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		parallel int
		tables   int
		want     int
	}{
		{name: "fewer parallel than tables", parallel: 2, tables: 5, want: 2},
		{name: "more parallel than tables", parallel: 8, tables: 3, want: 3},
		{name: "default parallelism", parallel: 0, tables: 10, want: DefaultParallelTables},
		{name: "default parallelism with fewer tables", parallel: 0, tables: 2, want: 2},
		{name: "single table", parallel: 4, tables: 1, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newStEnv()
			e.tables.tables = nil
			for i := range tc.tables {
				e.tables.tables = append(e.tables.tables, stTable("t"+string(rune('a'+i))))
			}
			cfg := stCfg()
			cfg.ParallelTables = tc.parallel
			plan, err := Prepare(t.Context(), e.deps(), cfg)
			require.NoError(t, err)
			require.NotNil(t, plan.Initial)
			assert.Len(t, plan.Initial.Sessions, tc.want)
			opened, _ := e.chunks.Sessions()
			assert.Len(t, opened, tc.want)
			require.NoError(t, plan.Initial.Close(t.Context()))
		})
	}
}

func TestPrepare_FirstStartDefaultChunkRows(t *testing.T) {
	t.Parallel()
	e := newStEnv()
	cfg := stCfg()
	cfg.ChunkRows = 0
	plan, err := Prepare(t.Context(), e.deps(), cfg)
	require.NoError(t, err)
	require.NotNil(t, plan.Initial)
	assert.Equal(t, DefaultChunkRows, plan.Initial.Run.ChunkRows)
	require.NoError(t, plan.Initial.Close(t.Context()))
}

func TestPrepare_FirstStartNever(t *testing.T) {
	t.Parallel()
	e := newStEnv()
	cfg := stCfg()
	cfg.Mode = ModeNever
	plan, err := Prepare(t.Context(), e.deps(), cfg)
	require.NoError(t, err)

	assert.Nil(t, plan.Initial)
	assert.Equal(t, stConsistent, plan.StartLSN)
	assert.Zero(t, e.tables.calls, "tables are not listed without an initial snapshot")
	saved := e.store.Saved()
	require.Len(t, saved, 2)
	assert.Equal(t, lsn.InvalidLSN, saved[0].state.ConsistentPoint)
	assert.Equal(t, 0, saved[0].created)
	assert.Equal(t, stConsistent, saved[1].state.ConsistentPoint)
	assert.Equal(t, 1, saved[1].created)
	for _, s := range saved {
		assert.Empty(t, s.state.Snapshot.Runs)
	}
	opened, _ := e.chunks.Sessions()
	assert.Empty(t, opened)
}

func TestPrepare_FirstStartNoCapturedTables(t *testing.T) {
	t.Parallel()
	e := newStEnv()
	e.tables.tables = nil
	plan, err := Prepare(t.Context(), e.deps(), stCfg())
	require.NoError(t, err)

	assert.Nil(t, plan.Initial)
	assert.Equal(t, 1, e.tables.calls)
	saved := e.store.Saved()
	require.Len(t, saved, 2)
	assert.Empty(t, saved[0].state.Snapshot.Runs)
	assert.Equal(t, uint64(0), saved[0].state.RunCounter, "no run was started")
	opened, _ := e.chunks.Sessions()
	assert.Empty(t, opened)
}

func TestPrepare_Resume(t *testing.T) {
	t.Parallel()
	st := stOwnedState()
	e := newStEnv().withState(&st).withSlot()
	plan, err := Prepare(t.Context(), e.deps(), stCfg())
	require.NoError(t, err)

	assert.Equal(t, ActionResume, plan.Decision.Action)
	assert.Equal(t, stConfirmed, plan.StartLSN, "streaming resumes from the slot's confirmed LSN")
	assert.Empty(t, e.store.Saved(), "a plain resume does not save")
	assert.Equal(t, st, plan.State, "state passes through including the unfinished run")
	assert.Nil(t, plan.Initial)
	created, dropped := e.repl.Slots()
	assert.Empty(t, created)
	assert.Empty(t, dropped)
	assert.Zero(t, e.tables.calls)

	assert.Equal(t, schema.RegistryState{Tables: plan.Schemas.Tables}, plan.Schemas)
	versions := map[string]uint64{}
	for _, ts := range plan.Schemas.Tables {
		versions[ts.Table] = ts.Version
	}
	assert.Equal(t, map[string]uint64{"public.orders": 5, "public.items": 2_000_000_000}, versions)
}

func TestPrepare_Recreate(t *testing.T) {
	t.Parallel()
	st := stOwnedState()
	st.ConsistentPoint = lsn.InvalidLSN
	e := newStEnv().withState(&st).withSlot()
	cfg := stCfg()
	cfg.ParallelTables = 1
	plan, err := Prepare(t.Context(), e.deps(), cfg)
	require.NoError(t, err)

	assert.Equal(t, ActionRecreateSlot, plan.Decision.Action)
	created, dropped := e.repl.Slots()
	assert.Equal(t, []string{stSlot}, dropped)
	assert.Equal(t, []string{stSlot}, created)
	assert.Equal(t, stConsistent, plan.StartLSN, "the new slot's consistent point, not the old confirmed LSN")

	saved := e.store.Saved()
	require.Len(t, saved, 2)
	assert.Equal(t, 0, saved[0].created)
	assert.Equal(t, lsn.InvalidLSN, saved[0].state.ConsistentPoint)
	require.Len(t, saved[0].state.Snapshot.Runs, 1, "old runs are discarded")
	run := saved[0].state.Snapshot.Runs[0]
	assert.Equal(t, uint64(8), run.ID, "the run counter is kept and incremented")
	assert.Equal(t, uint64(8), saved[0].state.RunCounter)
	assert.Equal(t, pipeline.SnapshotKindInitial, run.Kind)
	assert.Empty(t, saved[0].state.Snapshot.Queue)
	assert.Zero(t, saved[0].state.Snapshot.TriggerLSN)
	assert.Equal(t, stConsistent, saved[1].state.ConsistentPoint)
	assert.Equal(t, uint64(5), saved[1].state.Schemas["public.orders"].Version, "schema versions are kept")

	require.NotNil(t, plan.Initial)
	assert.Len(t, plan.Initial.Sessions, 1)
	require.NoError(t, plan.Initial.Close(t.Context()))
}

func TestPrepare_Reinit(t *testing.T) {
	t.Parallel()
	otherSystem := stOwnedState()
	otherSystem.SystemID = "7000000000000000002"
	otherSlot := stOwnedState()
	otherSlot.SlotName = "wake_other"
	owned := stOwnedState()
	tests := []struct {
		name  string
		state *pipeline.State
	}{
		{name: "state of another system", state: &otherSystem},
		{name: "state of another slot", state: &otherSlot},
		{name: "owned state", state: &owned},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newStEnv().withState(tc.state).withSlot()
			cfg := stCfg()
			cfg.Reinit = true
			plan, err := Prepare(t.Context(), e.deps(), cfg)
			require.NoError(t, err)

			assert.Equal(t, ActionResume, plan.Decision.Action)
			assert.True(t, plan.Decision.Reinit)
			assert.True(t, plan.Decision.FreshState)
			assert.Equal(t, stConfirmed, plan.StartLSN)
			assert.Nil(t, plan.Initial)
			created, dropped := e.repl.Slots()
			assert.Empty(t, created)
			assert.Empty(t, dropped)

			saved := e.store.Saved()
			require.Len(t, saved, 1)
			got := saved[0].state
			assert.Equal(t, got, plan.State)
			assert.Equal(t, stSlot, got.SlotName)
			assert.Equal(t, stSystem, got.SystemID)
			assert.Equal(t, stConfirmed, got.ConsistentPoint, "the slot's confirmed LSN marks the consistent point")
			assert.Empty(t, got.Snapshot.Runs)
			assert.Empty(t, got.Snapshot.Queue)
			assert.Zero(t, got.Snapshot.TriggerLSN)
			assert.Equal(t, uint64(7), got.RunCounter, "run counter is kept")

			now := uint64(stClockTime.Unix())
			assert.Equal(t, now, got.SchemaFloor)
			assert.Equal(t, now, plan.Schemas.Floor)
			assert.Equal(t, now, got.Schemas["public.orders"].Version, "max(stored, now): stored is older")
			assert.Equal(t, uint64(2_000_000_000), got.Schemas["public.items"].Version, "max(stored, now): stored is newer")
			versions := map[string]uint64{}
			for _, ts := range plan.Schemas.Tables {
				versions[ts.Table] = ts.Version
			}
			assert.Equal(t, map[string]uint64{"public.orders": now, "public.items": 2_000_000_000}, versions)
			assert.Equal(t, "d", got.Schemas["public.orders"].ReplicaIdentity)
			assert.Len(t, got.Schemas["public.orders"].Columns, 1)
		})
	}
}

func TestPrepare_ReinitLogsDiscardedProgress(t *testing.T) {
	t.Parallel()
	st := stOwnedState()
	e := newStEnv().withState(&st).withSlot()
	e.logger = slog.New(slog.NewTextHandler(e.logs, nil))
	cfg := stCfg()
	cfg.Reinit = true
	_, err := Prepare(t.Context(), e.deps(), cfg)
	require.NoError(t, err)
	out := e.logs.String()
	assert.Contains(t, out, "discards an unfinished snapshot")
	assert.Contains(t, out, "table=public.orders")
	assert.NotContains(t, out, "table=public.done", "finished tables are not logged")
	assert.Contains(t, out, "discards a queued snapshot request")
	assert.Contains(t, out, "table=public.users")
}

func TestPrepare_ReinitSlotMissingCreatesSlot(t *testing.T) {
	t.Parallel()
	st := stOwnedState()
	e := newStEnv().withState(&st) // the slot is absent
	cfg := stCfg()
	cfg.Reinit = true
	plan, err := Prepare(t.Context(), e.deps(), cfg)
	require.NoError(t, err)

	assert.Equal(t, ActionCreateSlot, plan.Decision.Action)
	assert.True(t, plan.Decision.Reinit)
	created, dropped := e.repl.Slots()
	assert.Equal(t, []string{stSlot}, created)
	assert.Empty(t, dropped)
	assert.Equal(t, stConsistent, plan.StartLSN)

	saved := e.store.Saved()
	require.Len(t, saved, 2)
	require.Len(t, saved[0].state.Snapshot.Runs, 1)
	assert.Equal(t, uint64(8), saved[0].state.Snapshot.Runs[0].ID)
	assert.Empty(t, saved[0].state.Snapshot.Queue)
	now := uint64(stClockTime.Unix())
	assert.Equal(t, now, saved[1].state.Schemas["public.orders"].Version)
	assert.Equal(t, now, saved[1].state.SchemaFloor)
	require.NotNil(t, plan.Initial)
	require.NoError(t, plan.Initial.Close(t.Context()))
}

func TestPrepare_SlotMissingWithoutReinit(t *testing.T) {
	t.Parallel()
	st := stOwnedState()
	e := newStEnv().withState(&st)
	plan, err := Prepare(t.Context(), e.deps(), stCfg())
	require.ErrorIs(t, err, ErrSlotMissing)
	assert.Nil(t, plan)
	assert.Empty(t, e.store.Saved())
	created, _ := e.repl.Slots()
	assert.Empty(t, created)
}

func TestPrepare_DecideErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(e *stEnv)
		want  error
	}{
		{
			name: "state of another slot",
			setup: func(e *stEnv) {
				st := stOwnedState()
				st.SlotName = "wake_other"
				e.withState(&st).withSlot()
			},
			want: pipeline.ErrStateMismatch,
		},
		{
			name:  "slot without state",
			setup: func(e *stEnv) { e.withSlot() },
			want:  pipeline.ErrStateMismatch,
		},
		{
			name: "invalidated slot",
			setup: func(e *stEnv) {
				e.cat.SetSlot(pipeline.SlotStatus{Exists: true, WALStatus: "lost"}, nil)
			},
			want: ErrSlotInvalidated,
		},
		{
			name: "slot in use",
			setup: func(e *stEnv) {
				e.cat.SetSlot(pipeline.SlotStatus{Exists: true, WALStatus: "reserved", ActivePID: 9}, nil)
			},
			want: ErrSlotInUse,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newStEnv()
			tc.setup(e)
			plan, err := Prepare(t.Context(), e.deps(), stCfg())
			require.ErrorIs(t, err, tc.want)
			assert.Nil(t, plan)
			assert.Empty(t, e.store.Saved())
			created, dropped := e.repl.Slots()
			assert.Empty(t, created)
			assert.Empty(t, dropped)
		})
	}
}

func TestPrepare_InvalidOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(d *PrepareDeps, c *PrepareConfig)
	}{
		{name: "nil slots", mut: func(d *PrepareDeps, _ *PrepareConfig) { d.Slots = nil }},
		{name: "nil catalog", mut: func(d *PrepareDeps, _ *PrepareConfig) { d.Catalog = nil }},
		{name: "nil store", mut: func(d *PrepareDeps, _ *PrepareConfig) { d.Store = nil }},
		{name: "nil chunks", mut: func(d *PrepareDeps, _ *PrepareConfig) { d.Chunks = nil }},
		{name: "nil tables", mut: func(d *PrepareDeps, _ *PrepareConfig) { d.Tables = nil }},
		{name: "nil clock", mut: func(d *PrepareDeps, _ *PrepareConfig) { d.Clock = nil }},
		{name: "nil logger", mut: func(d *PrepareDeps, _ *PrepareConfig) { d.Logger = nil }},
		{name: "empty slot", mut: func(_ *PrepareDeps, c *PrepareConfig) { c.SlotName = "" }},
		{name: "empty mode", mut: func(_ *PrepareDeps, c *PrepareConfig) { c.Mode = "" }},
		{name: "unknown mode", mut: func(_ *PrepareDeps, c *PrepareConfig) { c.Mode = "always" }},
		{name: "negative chunk rows", mut: func(_ *PrepareDeps, c *PrepareConfig) { c.ChunkRows = -1 }},
		{name: "negative parallel tables", mut: func(_ *PrepareDeps, c *PrepareConfig) { c.ParallelTables = -1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newStEnv()
			deps, cfg := e.deps(), stCfg()
			tc.mut(deps, &cfg)
			plan, err := Prepare(t.Context(), deps, cfg)
			require.ErrorIs(t, err, ErrInvalidOption)
			assert.Nil(t, plan)
			assert.Empty(t, e.store.Saved())
		})
	}
	t.Run("nil deps", func(t *testing.T) {
		t.Parallel()
		plan, err := Prepare(t.Context(), nil, stCfg())
		require.ErrorIs(t, err, ErrInvalidOption)
		assert.Nil(t, plan)
	})
}

func TestPrepare_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		setup     func(e *stEnv)
		wantSaves int
		wantSlots int // created slots
	}{
		{
			name:  "slot lookup fails",
			setup: func(e *stEnv) { e.cat.SetSlot(pipeline.SlotStatus{}, errStBoom) },
		},
		{
			name:  "load fails",
			setup: func(e *stEnv) { e.inner.FailLoad(errStBoom) },
		},
		{
			name:  "clock_timestamp fails",
			setup: func(e *stEnv) { e.cat.SetCurrentTime(time.Time{}, errStBoom) },
		},
		{
			name:  "captured tables fail",
			setup: func(e *stEnv) { e.tables.err = errStBoom },
		},
		{
			name:  "first save fails",
			setup: func(e *stEnv) { e.inner.FailSave(errStBoom) },
		},
		{
			name:  "second save fails",
			setup: func(e *stEnv) { e.store.failOn = 2 },
			// The slot exists by then; the next start recreates it from the state file.
			wantSaves: 1, wantSlots: 1,
		},
		{
			name:      "create slot fails",
			setup:     func(e *stEnv) { e.repl.SetSlot(pipeline.SlotInfo{}, errStBoom) },
			wantSaves: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newStEnv()
			tc.setup(e)
			plan, err := Prepare(t.Context(), e.deps(), stCfg())
			require.ErrorIs(t, err, errStBoom)
			assert.Nil(t, plan)
			assert.Len(t, e.store.Saved(), tc.wantSaves)
			created, _ := e.repl.Slots()
			assert.Len(t, created, tc.wantSlots)
			opened, closed := e.chunks.Sessions()
			assert.Len(t, opened, closed, "no session stays open")
		})
	}
}

func TestPrepare_ReinitSaveFails(t *testing.T) {
	t.Parallel()
	st := stOwnedState()
	e := newStEnv().withState(&st).withSlot()
	e.inner.FailSave(errStBoom)
	cfg := stCfg()
	cfg.Reinit = true
	plan, err := Prepare(t.Context(), e.deps(), cfg)
	require.ErrorIs(t, err, errStBoom)
	assert.Nil(t, plan)
}

func TestPrepare_OpenExportedFails(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		failAt     int
		wantOpened int
	}{
		{name: "first session", failAt: 1, wantOpened: 0},
		{name: "second session closes the first", failAt: 2, wantOpened: 1},
		{name: "third session closes the others", failAt: 3, wantOpened: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newStEnv()
			deps := e.deps()
			deps.Chunks = &stFlakyChunks{ChunkReader: e.chunks, failAt: tc.failAt}
			cfg := stCfg()
			cfg.ParallelTables = 3
			plan, err := Prepare(t.Context(), deps, cfg)
			require.ErrorIs(t, err, errStBoom)
			assert.Nil(t, plan)
			opened, closed := e.chunks.Sessions()
			assert.Len(t, opened, tc.wantOpened)
			assert.Equal(t, tc.wantOpened, closed, "no session stays open")
		})
	}
	t.Run("fakechunk FailOpen", func(t *testing.T) {
		t.Parallel()
		e := newStEnv()
		e.chunks.FailOpen(errStBoom)
		plan, err := Prepare(t.Context(), e.deps(), stCfg())
		require.ErrorIs(t, err, errStBoom)
		assert.Nil(t, plan)
		opened, closed := e.chunks.Sessions()
		assert.Empty(t, opened)
		assert.Zero(t, closed)
	})
}

func TestPrepare_EmptyTableNameInStateSchemas(t *testing.T) {
	t.Parallel()
	st := stOwnedState()
	st.Schemas[""] = pipeline.TableSchema{Version: 1}
	e := newStEnv().withState(&st).withSlot()
	plan, err := Prepare(t.Context(), e.deps(), stCfg())
	require.ErrorIs(t, err, schema.ErrInvalidState)
	assert.Nil(t, plan)
	assert.Empty(t, e.store.Saved())
}

func TestPrepare_ContextDone(t *testing.T) {
	t.Parallel()
	e := newStEnv()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plan, err := Prepare(ctx, e.deps(), stCfg())
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, plan)
}

func TestInitialSetup_Close_JoinsSessionErrors(t *testing.T) {
	t.Parallel()
	r := fakechunk.New()
	ctx := t.Context()
	s1, err := r.OpenExported(ctx, "a")
	require.NoError(t, err)
	s2, err := r.OpenExported(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, s1.Close(ctx))
	setup := &InitialSetup{Sessions: []pipeline.ChunkSession{s1, s2}}

	err = setup.Close(ctx)
	require.ErrorIs(t, err, fakechunk.ErrClosed, "the already closed session's error is reported")
	_, closed := r.Sessions()
	assert.Equal(t, 2, closed, "the other session is still closed")
	assert.Empty(t, setup.Sessions)
	require.NoError(t, setup.Close(ctx))
}
