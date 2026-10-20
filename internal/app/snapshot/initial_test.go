// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/fakechunk"
)

const initialCP = lsn.LSN(0x5000)

type initialFixture struct {
	chunks  *fakechunk.Reader
	keeper  *StateKeeper
	opener  *fakeOpener
	metrics *recMetrics
	initial *Initial
}

// newInitialFixture prepares run 4 over public.a (ids 1..5) and public.b
// (ids 1..3) with chunk_rows 2 and the given number of sessions.
func newInitialFixture(t *testing.T, sessions int) *initialFixture {
	t.Helper()
	f := &initialFixture{chunks: fakechunk.New(), opener: &fakeOpener{}, metrics: &recMetrics{}}
	for id := int64(1); id <= 5; id++ {
		f.chunks.Put("public.a", id, `"a"`)
	}
	for id := int64(1); id <= 3; id++ {
		f.chunks.Put("public.b", id, `"b"`)
	}
	run := pipeline.SnapshotRun{
		ID: 4, Kind: pipeline.SnapshotKindInitial, ChunkRows: 2,
		Tables: []pipeline.TableProgress{{Table: "public.a"}, {Table: "public.b"}},
	}
	st := pipeline.NewState("wake_app", "1")
	st.ConsistentPoint, st.RunCounter = initialCP, 4
	st.Snapshot.Runs = []pipeline.SnapshotRun{run}
	f.keeper, _ = keeperWith(t, st)
	setup := &InitialSetup{
		Run: run, ConsistentPoint: initialCP, TsMs: 1_790_000_000_000,
		Tables: []Table{table("a"), table("b")},
	}
	for range sessions {
		sess, err := f.chunks.OpenExported(context.Background(), "snap-1")
		require.NoError(t, err)
		setup.Sessions = append(setup.Sessions, sess)
	}
	var err error
	f.initial, err = NewInitial(InitialDeps{
		Keeper: f.keeper, Opener: f.opener, Versions: fixedVersions(3), Clock: clock.NewFake(time.Unix(0, 0)),
		Logger: testutil.DiscardLogger(), Metrics: f.metrics,
	}, setup)
	require.NoError(t, err)
	return f
}

func (f *initialFixture) run(t *testing.T) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- f.initial.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
	})
	return done
}

func TestNewInitial_Invalid(t *testing.T) {
	t.Parallel()
	k, _ := keeperWith(t, pipeline.NewState("s", "1"))
	deps := InitialDeps{
		Keeper: k, Opener: &fakeOpener{}, Versions: fixedVersions(1), Clock: clock.NewFake(time.Unix(0, 0)),
		Logger: testutil.DiscardLogger(),
	}
	sess, err := fakechunk.New().OpenExported(context.Background(), "s")
	require.NoError(t, err)
	good := &InitialSetup{Run: pipeline.SnapshotRun{ChunkRows: 1}, Sessions: []pipeline.ChunkSession{sess}}
	cases := []struct {
		name  string
		deps  InitialDeps
		setup *InitialSetup
	}{
		{name: "nil keeper", deps: InitialDeps{}, setup: good},
		{name: "nil setup", deps: deps},
		{name: "no sessions", deps: deps, setup: &InitialSetup{Run: pipeline.SnapshotRun{ChunkRows: 1}}},
		{name: "no chunk rows", deps: deps, setup: &InitialSetup{Sessions: good.Sessions}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewInitial(tc.deps, tc.setup)
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

// TestInitial_Run_EmitsReleasesAndPersistsAckedProgress: r events carry the
// consistent point, the persisted clock_timestamp and n = c × chunk_rows + i;
// the stream is released once every row is routed; progress is persisted
// only for acknowledged chunks; Done follows the last acknowledgement.
func TestInitial_Run_EmitsReleasesAndPersistsAckedProgress(t *testing.T) {
	t.Parallel()
	f := newInitialFixture(t, 1)
	done := f.run(t)
	<-f.initial.Released()
	pseudo := f.opener.first(t)
	assert.True(t, pseudo.isSealed(), "pseudo-transaction sealed before release")
	assert.Equal(t, []lsn.LSN{initialCP}, f.opener.ends)
	recs := pseudo.records()
	require.Len(t, recs, 8)
	gotIDs := make([]string, 0, len(recs))
	for _, r := range recs {
		gotIDs = append(gotIDs, r.ID.String())
		assert.Equal(t, change.OpRead, r.Op)
		assert.Equal(t, int64(1_790_000_000_000), r.TsMs)
		assert.Equal(t, uint64(3), r.SchemaVersion)
		assert.NotContains(t, string(r.Payload), "tx_id")
	}
	// One session copies public.a in chunks 0..2 and public.b in 3..4.
	assert.Equal(t, []string{
		"0/5000-r4-0", "0/5000-r4-1", "0/5000-r4-2", "0/5000-r4-3", "0/5000-r4-4",
		"0/5000-r4-6", "0/5000-r4-7", "0/5000-r4-8",
	}, gotIDs)
	select {
	case <-f.initial.Done():
		t.Fatal("done before acknowledgements")
	default:
	}
	st := f.keeper.State()
	assert.Nil(t, st.Snapshot.Runs[0].Tables[0].Position, "nothing acknowledged yet")
	assert.Equal(t, uint64(5), st.Snapshot.Runs[0].NextChunk, "chunk counters persisted")

	groups := pseudo.allGroups()
	require.Len(t, groups, 5)
	groups[1].ack() // out of order: chunk 1 before chunk 0
	groups[0].ack()
	require.Eventually(t, func() bool {
		return string(f.keeper.State().Snapshot.Runs[0].Tables[0].Position) == "[4]"
	}, 5*time.Second, time.Millisecond)
	assert.Equal(t, uint64(4), f.keeper.State().Snapshot.Runs[0].Tables[0].Rows)
	for _, g := range groups {
		g.ack()
	}
	require.NoError(t, <-done)
	<-f.initial.Done()
	assert.Empty(t, f.keeper.State().Snapshot.Runs)
	rows, _, _ := f.metrics.get()
	assert.Equal(t, map[string]uint64{"public.a": 5, "public.b": 3}, rows)
	_, closed := f.chunks.Sessions()
	assert.Equal(t, 1, closed)
}

func TestInitial_Run_ParallelSessionsCopyEveryTableOnce(t *testing.T) {
	t.Parallel()
	f := newInitialFixture(t, 2)
	done := f.run(t)
	<-f.initial.Released()
	pseudo := f.opener.first(t)
	seen := map[string]bool{}
	ns := map[uint64]bool{}
	for _, r := range pseudo.records() {
		var p struct {
			Key json.RawMessage `json:"key"`
		}
		require.NoError(t, json.Unmarshal(r.Payload, &p))
		k := r.Destination + string(p.Key)
		assert.False(t, seen[k], "row %s copied twice", k)
		seen[k] = true
		assert.False(t, ns[r.ID.N], "n %d reused", r.ID.N)
		ns[r.ID.N] = true
	}
	assert.Len(t, seen, 8)
	for _, g := range pseudo.allGroups() {
		g.ack()
	}
	require.NoError(t, <-done)
	_, closed := f.chunks.Sessions()
	assert.Equal(t, 2, closed)
}

func TestInitial_Run_Errors(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	cases := []struct {
		name  string
		setup func(f *initialFixture)
	}{
		{name: "open transaction", setup: func(f *initialFixture) { f.opener.err = boom }},
		{name: "read chunk", setup: func(f *initialFixture) { f.chunks.FailRead(boom) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newInitialFixture(t, 2)
			tc.setup(f)
			require.ErrorIs(t, f.initial.Run(context.Background()), boom)
			_, closed := f.chunks.Sessions()
			assert.Equal(t, 2, closed, "sessions closed")
			select {
			case <-f.initial.Released():
				t.Fatal("released after a failure")
			default:
			}
		})
	}
}

func TestInitial_Run_CancelWhileWaitingForAcks(t *testing.T) {
	t.Parallel()
	f := newInitialFixture(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.initial.Run(ctx) }()
	<-f.initial.Released()
	cancel()
	require.NoError(t, <-done)
	assert.Len(t, f.keeper.State().Snapshot.Runs, 1, "run kept for the restart")
}
