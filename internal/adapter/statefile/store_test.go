// SPDX-License-Identifier: Apache-2.0

package statefile_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/statefile"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakestate"
)

func fullState() pipeline.State {
	s := pipeline.NewState("wake_app", "7300000000000000001")
	s.ConsistentPoint = lsn.LSN(0x16B374D848)
	s.ConsistentPointTsMs = 1_760_000_000_123
	s.RunCounter = 7
	s.SchemaFloor = 1_760_000_000
	s.Schemas["public.orders"] = pipeline.TableSchema{
		Version:         3,
		ReplicaIdentity: "d",
		Columns: []pipeline.SchemaColumn{
			{Name: "id", Type: "int8", TypeOID: 20, TypeMod: -1, Key: true},
			{Name: "note", Type: "text", TypeOID: 25, TypeMod: -1},
		},
	}
	s.Schemas["public.users"] = pipeline.TableSchema{Version: 1, ReplicaIdentity: "f"}
	s.Snapshot = pipeline.SnapshotState{
		Runs: []pipeline.SnapshotRun{{
			ID: 6, Kind: pipeline.SnapshotKindTable, ChunkRows: 1000, NextChunk: 12,
			Tables: []pipeline.TableProgress{
				{Table: "public.orders", Position: json.RawMessage(`[41,"x"]`), Rows: 11_000, Done: false},
				{Table: "public.users", Done: true, Rows: 5},
			},
		}},
		Queue:      []string{"public.audit"},
		TriggerLSN: lsn.LSN(0x16B374D900),
	}
	return s
}

func newStore(t *testing.T) (*statefile.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sub", "state.json")
	st, err := statefile.New(path)
	require.NoError(t, err)
	return st, path
}

func TestNew_EmptyPath(t *testing.T) {
	t.Parallel()
	_, err := statefile.New("")
	require.ErrorIs(t, err, statefile.ErrInvalidPath)
}

func TestStore_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, path := newStore(t)
	assert.Equal(t, path, st.Path())

	want := fullState()
	require.NoError(t, st.Save(ctx, want))
	got, err := st.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	want.RunCounter = 8
	want.Snapshot.Runs = nil
	require.NoError(t, st.Save(ctx, want))
	got, err = st.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(8), got.RunCounter)
	assert.Empty(t, got.Snapshot.Runs)
}

func TestStore_ParityWithFake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, _ := newStore(t)
	fake := fakestate.New()

	_, errReal := st.Load(ctx)
	_, errFake := fake.Load(ctx)
	require.ErrorIs(t, errReal, pipeline.ErrStateNotFound)
	require.ErrorIs(t, errFake, pipeline.ErrStateNotFound)

	s := fullState()
	require.NoError(t, st.Save(ctx, s))
	require.NoError(t, fake.Save(ctx, s))
	onDisk, err := st.Load(ctx)
	require.NoError(t, err)
	fk, err := fake.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, fk, onDisk)
}

func TestStore_Load_Missing(t *testing.T) {
	t.Parallel()
	st, _ := newStore(t)
	_, err := st.Load(context.Background())
	require.ErrorIs(t, err, pipeline.ErrStateNotFound)
}

func TestStore_Load_Invalid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    error
	}{
		{"truncated json", `{"format_version": 1, "slot_na`, statefile.ErrCorrupt},
		{"not json", "\x00\x01garbage", statefile.ErrCorrupt},
		{"empty file", "", statefile.ErrCorrupt},
		{"trailing data", `{"format_version":1}{}`, statefile.ErrCorrupt},
		{"wrong field type", `{"format_version":"one"}`, statefile.ErrCorrupt},
		{"missing version", `{"slot_name":"wake_app"}`, statefile.ErrCorrupt},
		{"future version", `{"format_version":2,"slot_name":"wake_app"}`, statefile.ErrUnsupportedVersion},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, path := newStore(t)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))

			_, err := st.Load(context.Background())
			require.ErrorIs(t, err, tc.want)
			require.NotErrorIs(t, err, pipeline.ErrStateNotFound)

			after, rerr := os.ReadFile(path)
			require.NoError(t, rerr)
			assert.Equal(t, tc.content, string(after), "invalid file must not be modified")
		})
	}
}

func TestStore_Load_ReadError(t *testing.T) {
	t.Parallel()
	st, path := newStore(t)
	require.NoError(t, os.MkdirAll(path, 0o700))
	_, err := st.Load(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, pipeline.ErrStateNotFound)
}

func TestStore_CanceledContext(t *testing.T) {
	t.Parallel()
	st, path := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, st.Save(ctx, fullState()), context.Canceled)
	_, err := st.Load(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestStore_Save_Permissions(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permissions")
	}
	ctx := context.Background()
	st, path := newStore(t)
	require.NoError(t, st.Save(ctx, fullState()))
	require.NoError(t, st.Save(ctx, fullState()))

	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
	di, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), di.Mode().Perm())

	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temporary files left")
}

func TestStore_Save_ParentIsFile(t *testing.T) {
	t.Parallel()
	blocker := filepath.Join(t.TempDir(), "blocker")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	st, err := statefile.New(filepath.Join(blocker, "state.json"))
	require.NoError(t, err)
	require.Error(t, st.Save(context.Background(), fullState()))
}

func TestStore_Save_ConcurrentLastWriterWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, path := newStore(t)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := fullState()
			s.RunCounter = uint64(i)
			assert.NoError(t, st.Save(ctx, s))
		}()
	}
	wg.Wait()
	_, err := st.Load(ctx)
	require.NoError(t, err)
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}
