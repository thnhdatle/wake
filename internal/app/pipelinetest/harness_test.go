// SPDX-License-Identifier: Apache-2.0

package pipelinetest

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/app/stream"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
	"github.com/thnhdatle/wake/internal/testutil/fakespill"
	"github.com/thnhdatle/wake/internal/testutil/fakestate"
)

var (
	items  = dbTable{oid: 16401, schema: "public", name: "items"}
	orders = dbTable{oid: 16402, schema: "public", name: "orders"}
)

// procOpts configures one Wake process.
type procOpts struct {
	mode      snapshot.Mode
	chunkRows int
	parallel  int
	nonce     string
	manualAck bool
}

// proc is one Wake process wired like cmd/wake will wire it (see the
// snapshot package documentation), over the fake database.
type proc struct {
	plan    *snapshot.Plan
	keeper  *snapshot.StateKeeper
	tracker *stream.AckTracker
	sink    *fakesink.Sink
	metrics *snapMetrics
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	errs    []error
}

func startProc(t *testing.T, db *fakeDB, store *fakestate.Store, o procOpts) *proc {
	t.Helper()
	ctx := context.Background()
	clk := clock.System{}
	logger := testutil.DiscardLogger()
	policy, err := pipeline.NewTablePolicy(pipeline.TablePolicyConfig{DB: "app"})
	require.NoError(t, err)
	tables, err := snapshot.NewCatalogTables(db, []string{"wake_app"}, policy)
	require.NoError(t, err)
	plan, err := snapshot.Prepare(ctx, &snapshot.PrepareDeps{
		Slots: db, Catalog: db, Store: store, Chunks: db, Tables: tables, Clock: clk, Logger: logger,
	}, snapshot.PrepareConfig{SlotName: "wake_app", Mode: o.mode, ChunkRows: o.chunkRows, ParallelTables: o.parallel})
	require.NoError(t, err)
	p := &proc{plan: plan, metrics: &snapMetrics{}}
	p.keeper, err = snapshot.NewStateKeeper(store, &plan.State)
	require.NoError(t, err)
	schemas, err := stream.NewSchemas(plan.Schemas)
	require.NoError(t, err)
	p.tracker, err = stream.NewAckTracker(plan.StartLSN)
	require.NoError(t, err)
	var sinkOpts []fakesink.Option
	if !o.manualAck {
		sinkOpts = append(sinkOpts, fakesink.WithAutoAck())
	}
	p.sink = fakesink.New("main", append(sinkOpts, fakesink.WithCapacity(1<<16))...)
	router, err := stream.NewRouter(p.tracker, []stream.SinkRoute{{Sink: p.sink, Tables: []string{"*"}}})
	require.NoError(t, err)
	status, err := stream.NewStatusUpdater(db, p.tracker, clk,
		stream.WithStatusInterval(5*time.Millisecond), stream.WithMinConfirmBytes(0))
	require.NoError(t, err)
	var (
		readerOpts = []stream.ReaderOption{stream.WithSchemaStore(p.keeper), stream.WithReaderLogger(logger)}
		runs       []func(context.Context) error
		after      <-chan struct{}
	)
	if plan.Initial != nil {
		initial, ierr := snapshot.NewInitial(snapshot.InitialDeps{
			Keeper: p.keeper, Opener: router, Versions: schemas, Clock: clk, Logger: logger, Metrics: p.metrics,
		}, plan.Initial)
		require.NoError(t, ierr)
		readerOpts = append(readerOpts, stream.WithGate(initial))
		after = initial.Done()
		runs = append(runs, initial.Run)
	}
	snap, err := snapshot.NewSnapshotter(snapshot.SnapshotterDeps{
		Keeper: p.keeper, Chunks: db, Emitter: db, Tables: tables, Versions: schemas, Clock: clk,
	}, snapshot.SnapshotterConfig{Nonce: o.nonce, ChunkRows: o.chunkRows, AddedTables: true},
		snapshot.WithAfter(after), snapshot.WithMetrics(p.metrics), snapshot.WithLogger(logger),
		snapshot.WithRetryBackoff(pipeline.Backoff{Initial: time.Millisecond, Max: 4 * time.Millisecond}))
	require.NoError(t, err)
	reader, err := stream.NewReader(stream.ReaderDeps{
		Source: db, Router: router, Status: status, Schemas: schemas, Tables: policy, Spill: fakespill.New(), Clock: clk,
	}, stream.ReaderConfig{Slot: "wake_app", Publications: []string{"wake_app"}, Start: plan.StartLSN},
		append(readerOpts, stream.WithObserver(snap))...)
	require.NoError(t, err)
	runs = append(runs, reader.Run, snap.Run, status.Run, p.sink.Run)
	rctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	for _, run := range runs {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			if err := run(rctx); err != nil {
				p.mu.Lock()
				p.errs = append(p.errs, err)
				p.mu.Unlock()
				cancel()
			}
		}()
	}
	t.Cleanup(func() { p.stop(t) })
	return p
}

// stop cancels the process ("kill") and checks that no component failed.
func (p *proc) stop(t *testing.T) {
	t.Helper()
	p.cancel()
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.Empty(t, p.errs)
}

// waitIdle waits until no snapshot is queued or running and every
// transaction in the WAL is confirmed.
func (p *proc) waitIdle(t *testing.T, db *fakeDB) {
	t.Helper()
	require.Eventually(t, func() bool {
		st := p.keeper.State()
		return len(st.Snapshot.Runs) == 0 && len(st.Snapshot.Queue) == 0 && p.tracker.Confirmed() >= db.walEnd()
	}, 30*time.Second, time.Millisecond, "pipeline did not settle")
}

// snapMetrics records snapshot metrics.
type snapMetrics struct {
	mu       sync.Mutex
	rows     map[string]uint64
	requests []string
	retries  int
}

func (m *snapMetrics) RowsAcked(table string, n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rows == nil {
		m.rows = map[string]uint64{}
	}
	m.rows[table] += n
}

func (m *snapMetrics) Request(table string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, table)
}

func (m *snapMetrics) ChunkRetry(string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries++
}

func (m *snapMetrics) snapshot() (map[string]uint64, []string, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := map[string]uint64{}
	for k, v := range m.rows {
		rows[k] = v
	}
	return rows, slices.Clone(m.requests), m.retries
}

// event is the part of a CDC event the final-state check needs.
type event struct {
	id     change.ID
	table  string
	op     change.Op
	key    int64
	before int64
	hasBef bool
	v      string
}

func parseEvents(t *testing.T, recs []pipeline.Record) []event {
	t.Helper()
	var out []event
	for i := range recs {
		if recs[i].Kind != pipeline.RecordKindEvent {
			continue
		}
		var raw struct {
			Op     string                     `json:"op"`
			Key    map[string]json.Number     `json:"key"`
			Before map[string]json.RawMessage `json:"before"`
			After  struct {
				V string `json:"v"`
			} `json:"after"`
		}
		require.NoError(t, json.Unmarshal(recs[i].Payload, &raw))
		e := event{id: recs[i].ID, table: recs[i].Destination, op: change.Op(raw.Op), v: raw.After.V}
		var err error
		e.key, err = raw.Key["id"].Int64()
		require.NoError(t, err)
		if b, ok := raw.Before["id"]; ok {
			e.before, err = strconv.ParseInt(string(b), 10, 64)
			require.NoError(t, err)
			e.hasBef = true
		}
		out = append(out, e)
	}
	return out
}

// finalState applies the events of a table per key in seq order as upserts
// (M2): `d` deletes, a `u` whose before key differs deletes it first.
func finalState(t *testing.T, recs []pipeline.Record, table string) map[int64]string {
	t.Helper()
	evs := parseEvents(t, recs)
	slices.SortStableFunc(evs, func(a, b event) int { return change.CompareSeq(a.id.Seq(), b.id.Seq()) })
	state := map[int64]string{}
	for _, e := range evs {
		if e.table != table {
			continue
		}
		switch e.op {
		case change.OpDelete:
			delete(state, e.key)
		case change.OpUpdate:
			if e.hasBef && e.before != e.key {
				delete(state, e.before)
			}
			state[e.key] = e.v
		case change.OpCreate, change.OpRead:
			state[e.key] = e.v
		case change.OpTruncate:
			clear(state)
		}
	}
	return state
}

// writer applies random single-table transactions and mirrors the table's
// ids so that every operation is valid.
type writer struct {
	db    *fakeDB
	table string
	rng   *rand.Rand
	ids   []int64
	next  int64
	mu    sync.Mutex
}

func newWriter(db *fakeDB, table string, seed uint64) *writer {
	w := &writer{db: db, table: table, rng: rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))}
	for id := range db.state(table) {
		w.ids = append(w.ids, id)
		w.next = max(w.next, id+1)
	}
	slices.Sort(w.ids)
	return w
}

// step commits one transaction of 1 to 3 random operations.
func (w *writer) step() {
	w.mu.Lock()
	defer w.mu.Unlock()
	ops := make([]op, 0, 3)
	for range 1 + w.rng.IntN(3) {
		ops = append(ops, w.op())
	}
	w.db.commit(ops...)
}

// touch commits an update, a key change and a delete of ids in [from, to].
func (w *writer) touch(from, to int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var in []int
	for i, id := range w.ids {
		if id >= from && id <= to {
			in = append(in, i)
		}
	}
	if len(in) < 3 {
		return
	}
	upd, key, del := w.ids[in[0]], w.ids[in[1]], w.ids[in[2]]
	newKey := w.next
	w.next++
	w.ids[in[1]] = newKey
	w.ids = slices.Delete(w.ids, in[2], in[2]+1)
	w.db.commit(
		op{kind: 'U', table: w.table, id: upd, v: "touched-" + strconv.FormatInt(upd, 10)},
		op{kind: 'K', table: w.table, oldID: key, id: newKey, v: "moved-" + strconv.FormatInt(key, 10)},
		op{kind: 'D', table: w.table, id: del},
	)
}

func (w *writer) op() op {
	n := w.rng.IntN(10)
	switch {
	case len(w.ids) == 0 || n < 3:
		id := w.next
		if len(w.ids) > 0 && w.rng.IntN(2) == 0 {
			id = w.rng.Int64N(w.next + 1) // may fill a gap below existing ids
			if slices.Contains(w.ids, id) {
				id = w.next
			}
		}
		if id == w.next {
			w.next++
		}
		w.ids = append(w.ids, id)
		return op{kind: 'I', table: w.table, id: id, v: "ins-" + strconv.Itoa(w.rng.IntN(1000))}
	case n < 7:
		id := w.ids[w.rng.IntN(len(w.ids))]
		return op{kind: 'U', table: w.table, id: id, v: "upd-" + strconv.Itoa(w.rng.IntN(1000))}
	case n < 8:
		i := w.rng.IntN(len(w.ids))
		old := w.ids[i]
		w.ids[i] = w.next
		w.next++
		return op{kind: 'K', table: w.table, oldID: old, id: w.ids[i], v: "key-" + strconv.Itoa(w.rng.IntN(1000))}
	default:
		i := w.rng.IntN(len(w.ids))
		id := w.ids[i]
		w.ids = slices.Delete(w.ids, i, i+1)
		return op{kind: 'D', table: w.table, id: id}
	}
}

// nonce returns a valid ULID per process number.
func nonce(n int) string { return "01JABCDEFGHJKMNPQRSTVWXY" + strconv.Itoa(10+n) }
