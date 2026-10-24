// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/watermark"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/fakechunk"
)

const testNonce = "01JABCDEFGHJKMNPQRSTVWXY10"

var visibleSnapshot = pipeline.TxSnapshot{XMin: 1000, XMax: 1000}

type snapFixture struct {
	chunks  *fakechunk.Reader
	stream  *fakeStream
	keeper  *StateKeeper
	metrics *recMetrics
	snap    *Snapshotter
}

func newSnapFixture(t *testing.T, st pipeline.State, opts ...SnapshotterOption) *snapFixture {
	t.Helper()
	f := &snapFixture{chunks: fakechunk.New(), stream: newFakeStream(), metrics: &recMetrics{}}
	f.chunks.SetSnapshot(visibleSnapshot, nil)
	for id := int64(1); id <= 5; id++ {
		f.chunks.Put("public.a", id, `"a"`)
	}
	f.keeper, _ = keeperWith(t, st)
	opts = append([]SnapshotterOption{
		WithMetrics(f.metrics), WithLogger(testutil.DiscardLogger()),
		WithRetryBackoff(pipeline.Backoff{Initial: time.Microsecond, Max: time.Microsecond}),
	}, opts...)
	var err error
	f.snap, err = NewSnapshotter(SnapshotterDeps{
		Keeper: f.keeper, Chunks: f.chunks, Emitter: f.stream,
		Tables:   staticTables{tables: []Table{table("a"), table("b")}},
		Versions: fixedVersions(2), Clock: clock.System{},
	}, SnapshotterConfig{Nonce: testNonce, ChunkRows: 2, AddedTables: true}, opts...)
	require.NoError(t, err)
	f.stream.obs = f.snap
	return f
}

func (f *snapFixture) start(t *testing.T) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- f.snap.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	return done
}

func (f *snapFixture) waitIdle(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		st := f.keeper.State()
		return len(st.Snapshot.Runs) == 0 && len(st.Snapshot.Queue) == 0
	}, 10*time.Second, time.Millisecond)
}

func (f *snapFixture) readIDs() []string {
	reads := f.stream.reads()
	out := make([]string, 0, len(reads))
	for _, r := range reads {
		out = append(out, r.ID.String())
	}
	return out
}

func queued(tables ...string) pipeline.State {
	st := pipeline.NewState("wake_app", "1")
	st.ConsistentPoint = 0x100
	st.Snapshot.Queue = tables
	return st
}

func TestNewSnapshotter_Invalid(t *testing.T) {
	t.Parallel()
	k, _ := keeperWith(t, queued())
	deps := SnapshotterDeps{
		Keeper: k, Chunks: fakechunk.New(), Emitter: newFakeStream(), Tables: staticTables{},
		Versions: fixedVersions(1), Clock: clock.System{},
	}
	cases := []struct {
		name string
		deps SnapshotterDeps
		cfg  SnapshotterConfig
		opts []SnapshotterOption
	}{
		{name: "nil deps", cfg: SnapshotterConfig{Nonce: testNonce}},
		{name: "bad nonce", deps: deps, cfg: SnapshotterConfig{Nonce: "nonce"}},
		{name: "negative chunk rows", deps: deps, cfg: SnapshotterConfig{Nonce: testNonce, ChunkRows: -1}},
		{
			name: "no attempts", deps: deps, cfg: SnapshotterConfig{Nonce: testNonce},
			opts: []SnapshotterOption{WithMaxChunkAttempts(0)},
		},
		{
			name: "nil logger", deps: deps, cfg: SnapshotterConfig{Nonce: testNonce},
			opts: []SnapshotterOption{WithLogger(nil)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewSnapshotter(tc.deps, tc.cfg, tc.opts...)
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
	s, err := NewSnapshotter(deps, SnapshotterConfig{Nonce: testNonce})
	require.NoError(t, err)
	assert.Equal(t, DefaultChunkRows, s.cfg.ChunkRows)
}

func TestSnapshotter_Run_QueuedTable(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued("public.a"))
	f.start(t)
	f.waitIdle(t)
	held := f.stream.heldTxs()
	require.Len(t, held, 3)
	assert.Equal(t, []string{
		held[0].commit.String() + "-r1-0", held[0].commit.String() + "-r1-1",
		held[1].commit.String() + "-r1-2", held[1].commit.String() + "-r1-3",
		held[2].commit.String() + "-r1-4",
	}, f.readIDs())
	for _, h := range held {
		assert.True(t, h.isSealed())
		for _, r := range h.records() {
			assert.Equal(t, int64(h.commit), r.TsMs, "ts_ms of the HIGH transaction")
			assert.Equal(t, uint64(2), r.SchemaVersion)
		}
	}
	rows, requests, _ := f.metrics.get()
	assert.Equal(t, []string{"public.a"}, requests)
	assert.Equal(t, uint64(5), rows["public.a"])
	assert.Equal(t, uint64(1), f.keeper.State().RunCounter)
}

func TestSnapshotter_Run_DropsRowsChangedInWindow(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued("public.a"))
	ctx := context.Background()
	f.chunks.OnRead(func(n int) {
		if n == 1 {
			f.stream.change(ctx, "public.a", `{"id":2}`)
			f.stream.change(ctx, "public.b", `{"id":1}`)
		}
	})
	f.start(t)
	f.waitIdle(t)
	held := f.stream.heldTxs()
	require.Len(t, held, 3)
	ids := f.readIDs()
	assert.Equal(t, held[0].commit.String()+"-r1-0", ids[0])
	assert.Equal(t, held[1].commit.String()+"-r1-2", ids[1], "row 2 (n=1) dropped")
	assert.Len(t, ids, 4)
}

func TestSnapshotter_Run_RetriesChunk(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued("public.a"))
	f.chunks.SetSnapshot(pipeline.TxSnapshot{XMin: 50, XMax: 1000, XIP: []uint64{50}}, []uint64{50})
	f.chunks.OnRead(func(n int) {
		if n == 1 {
			f.chunks.SetSnapshot(visibleSnapshot, nil)
		}
	})
	f.start(t)
	f.waitIdle(t)
	_, _, retries := f.metrics.get()
	assert.Equal(t, []string{"public.a"}, retries)
	held := f.stream.heldTxs()
	require.Len(t, held, 3)
	assert.Equal(t, held[0].commit.String()+"-r1-2", f.readIDs()[0], "chunk 0 discarded, rows re-read as chunk 1")
	assert.Len(t, f.readIDs(), 5)
}

func TestSnapshotter_Run_ChunkErrorsRetriedThenFail(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection reset")
	f := newSnapFixture(t, queued("public.a"), WithMaxChunkAttempts(3))
	f.stream.failNext, f.stream.failErr = 2, boom
	f.start(t)
	f.waitIdle(t)
	assert.Len(t, f.readIDs(), 5, "two failed emits retried")

	g := newSnapFixture(t, queued("public.a"), WithMaxChunkAttempts(3))
	g.chunks.FailRead(boom)
	err := <-g.start(t)
	require.ErrorIs(t, err, ErrChunkAttempts)
	require.ErrorIs(t, err, boom)
}

func TestSnapshotter_Run_InvalidHighSnapshotFails(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued("public.a"), WithMaxChunkAttempts(1))
	f.chunks.SetSnapshot(pipeline.TxSnapshot{}, nil)
	require.ErrorIs(t, <-f.start(t), watermark.ErrInvalidPayload)
}

func TestSnapshotter_Run_ResumesRun(t *testing.T) {
	t.Parallel()
	st := queued()
	st.RunCounter = 7
	st.Snapshot.Runs = []pipeline.SnapshotRun{{
		ID: 7, Kind: pipeline.SnapshotKindInitial, ChunkRows: 2, NextChunk: 5,
		Tables: []pipeline.TableProgress{
			{Table: "public.a", Position: []byte(`[2]`), Rows: 2},
			{Table: "public.gone"},
			{Table: "public.b", Done: true},
		},
	}}
	f := newSnapFixture(t, st)
	f.start(t)
	f.waitIdle(t)
	held := f.stream.heldTxs()
	require.Len(t, held, 2)
	assert.Equal(t, []string{
		held[0].commit.String() + "-r7-10", held[0].commit.String() + "-r7-11",
		held[1].commit.String() + "-r7-12",
	}, f.readIDs())
	reads := f.chunks.Reads()
	assert.JSONEq(t, `[2]`, string(reads[0].After))
	assert.Equal(t, uint64(7), f.keeper.State().RunCounter, "no new run_id")
}

func TestSnapshotter_Observe_Triggers(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued())
	ctx := context.Background()
	f.start(t)
	for _, content := range []string{`{"table":"public.a"}`, `{"table":"public.missing"}`, `{"table":`} {
		_, err := f.stream.EmitMessage(ctx, pgoutput.PrefixSnapshot, []byte(content))
		require.NoError(t, err)
	}
	f.waitIdle(t)
	_, requests, _ := f.metrics.get()
	assert.ElementsMatch(t, []string{OtherTable, "public.a", OtherTable}, requests)
	assert.Len(t, f.readIDs(), 5)
	assert.NotZero(t, f.keeper.State().Snapshot.TriggerLSN)
}

func TestSnapshotter_Observe_TriggerSaveError(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued())
	_, store := keeperWith(t, queued())
	k, err := NewStateKeeper(store, &pipeline.State{})
	require.NoError(t, err)
	f.snap.deps.Keeper = k
	boom := errors.New("disk")
	store.FailSave(boom)
	f.snap.ObserveBegin(pipeline.ObservedTx{XID: 1, CommitLSN: 0x10})
	f.snap.ObserveMessage(pgoutput.PrefixSnapshot, []byte(`{"table":"public.a"}`))
	_, err = f.snap.ObserveCommit(context.Background())
	require.ErrorIs(t, err, boom)
}

func TestSnapshotter_Observe_ForeignWatermarksIgnored(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued())
	run := &pipeline.SnapshotRun{ID: 1, ChunkRows: 2}
	tbl := table("a")
	require.NoError(t, f.snap.install(context.Background(), run, &tbl))
	ctx := context.Background()
	for _, p := range []watermark.Payload{
		{Nonce: "01JZZZZZZZZZZZZZZZZZZZZZZZ", Run: 1, Table: "public.a", Mark: watermark.MarkLow},
		{Nonce: testNonce, Run: 2, Table: "public.a", Mark: watermark.MarkLow},
	} {
		content, err := p.MarshalJSON()
		require.NoError(t, err)
		_, err = f.stream.EmitMessage(ctx, pgoutput.PrefixWatermark, content)
		require.NoError(t, err)
	}
	_, err := f.stream.EmitMessage(ctx, pgoutput.PrefixWatermark, []byte(`not json`))
	require.NoError(t, err)
	assert.Empty(t, f.stream.heldTxs())
	f.snap.ObserveTruncate("public.a")
	f.snap.uninstall()
}

func TestSnapshotter_Observe_HighWithoutLowReportsError(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued())
	run := &pipeline.SnapshotRun{ID: 1, ChunkRows: 2}
	tbl := table("a")
	require.NoError(t, f.snap.install(context.Background(), run, &tbl))
	defer f.snap.uninstall()
	high := watermark.Payload{
		Nonce: testNonce, Run: 1, Table: "public.a", Chunk: 3, Mark: watermark.MarkHigh,
		Snapshot: watermark.Snapshot{Xmin: 1000, Xmax: 1000},
	}
	content, err := high.MarshalJSON()
	require.NoError(t, err)
	_, err = f.stream.EmitMessage(context.Background(), pgoutput.PrefixWatermark, content)
	require.NoError(t, err)
	assert.Empty(t, f.stream.heldTxs())
	res, err := f.snap.await(context.Background(), 3)
	require.NoError(t, err)
	require.ErrorIs(t, res.err, watermark.ErrNoLow)
}

func TestSnapshotter_Install_WaitsForTransactionBoundary(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued())
	ctx := context.Background()
	f.snap.ObserveBegin(pipeline.ObservedTx{XID: 7, CommitLSN: 0x10})
	installed := make(chan error, 1)
	run := &pipeline.SnapshotRun{ID: 1, ChunkRows: 2}
	tbl := table("a")
	go func() { installed <- f.snap.install(ctx, run, &tbl) }()
	require.Eventually(t, func() bool {
		f.snap.mu.Lock()
		defer f.snap.mu.Unlock()
		return f.snap.pendingInstall != nil
	}, 5*time.Second, time.Millisecond)
	f.snap.ObserveChange("public.a", []byte(`{"id":1}`), []byte(`{"id":0}`))
	hold, err := f.snap.ObserveCommit(ctx)
	require.NoError(t, err)
	assert.False(t, hold)
	require.NoError(t, <-installed)
	f.snap.mu.Lock()
	assert.NotNil(t, f.snap.active)
	assert.Equal(t, 0, f.snap.active.merger.Retained(), "the open transaction was not fed")
	f.snap.mu.Unlock()
	f.snap.uninstall()

	cctx, cancel := context.WithCancel(ctx)
	f.snap.ObserveBegin(pipeline.ObservedTx{XID: 8, CommitLSN: 0x20})
	cancel()
	require.ErrorIs(t, f.snap.install(cctx, run, &tbl), context.Canceled)
	f.snap.uninstall()
}

func TestSnapshotter_ObserveHeld_WithoutChunkSeals(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued())
	h := &fakeHeld{}
	require.NoError(t, f.snap.ObserveHeld(context.Background(), h))
	assert.True(t, h.isSealed())
	assert.Empty(t, h.allGroups())
}

func TestSnapshotter_Run_WaitsForAfterAndAddTables(t *testing.T) {
	t.Parallel()
	after := make(chan struct{})
	f := newSnapFixture(t, queued(), WithAfter(after))
	f.start(t)
	ctx := context.Background()
	require.NoError(t, f.snap.AddTables(ctx, []string{"public.a", "public.a"}))
	assert.Equal(t, []string{"public.a"}, f.keeper.State().Snapshot.Queue, "deduplicated")
	assert.Empty(t, f.stream.heldTxs(), "waits for the initial snapshot")
	close(after)
	f.waitIdle(t)
	assert.Len(t, f.readIDs(), 5)

	off := newSnapFixture(t, queued())
	off.snap.cfg.AddedTables = false
	require.NoError(t, off.snap.AddTables(ctx, []string{"public.a"}))
	assert.Empty(t, off.keeper.State().Snapshot.Queue)
}

func TestSnapshotter_Run_CanceledReturnsNil(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued(), WithAfter(make(chan struct{})))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, f.snap.Run(ctx))
}

func TestSnapshotter_Run_TableSourceError(t *testing.T) {
	t.Parallel()
	boom := errors.New("catalog")
	f := newSnapFixture(t, queued("public.a"))
	f.snap.deps.Tables = staticTables{err: boom}
	require.ErrorIs(t, <-f.start(t), boom)
}

func TestSnapshotter_Await_StaleResultsDiscarded(t *testing.T) {
	t.Parallel()
	f := newSnapFixture(t, queued())
	f.snap.mu.Lock()
	for c := range uint64(maxResults + 2) {
		f.snap.pushResultLocked(chunkResult{chunk: c})
	}
	f.snap.mu.Unlock()
	res, err := f.snap.await(context.Background(), maxResults+1)
	require.NoError(t, err)
	assert.Equal(t, uint64(maxResults+1), res.chunk)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = f.snap.await(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
}
