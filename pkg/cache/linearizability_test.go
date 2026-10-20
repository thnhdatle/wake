// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
)

// lineLog is the history checked by the linearizability-style test. Times
// are ticks of one logical clock shared by every goroutine.
type lineLog struct {
	clock atomic.Int64

	mu        sync.Mutex // protects the slices below
	writes    []lineWrite
	reads     []lineRead
	processed []lineProcessed
}

type lineWrite struct {
	key      string
	ver      int
	gen      uint64
	deleter  int
	returned int64 // Delete returned
}

type lineRead struct {
	inst  int
	key   string
	start int64
	ver   int
}

type lineProcessed struct {
	inst int
	key  string
	gen  uint64
	at   int64 // the subscriber finished handling the invalidation
}

func (l *lineLog) tick() int64 { return l.clock.Add(1) }

type genSinkKey struct{}

// recordingBackend records when the subscriber has handled each
// invalidation (it asks for the next event only afterwards) and reports the
// generation of Invalidate calls to the writer through the context.
type recordingBackend struct {
	*l2fake.Client
	inst int
	log  *lineLog
	prev *l2.Event // only touched by the subscriber goroutine
}

func (r *recordingBackend) Receive(ctx context.Context) (l2.Event, error) {
	if r.prev != nil {
		r.log.mu.Lock()
		r.log.processed = append(r.log.processed, lineProcessed{r.inst, r.prev.Key, r.prev.Gen, r.log.tick()})
		r.log.mu.Unlock()
		r.prev = nil
	}
	ev, err := r.Client.Receive(ctx)
	if err == nil && ev.Kind == l2.EventInvalidate {
		r.prev = &ev
	}
	return ev, err
}

func (r *recordingBackend) Invalidate(ctx context.Context, req l2.InvalidateRequest) (l2.WriteResult, error) {
	res, err := r.Client.Invalidate(ctx, req)
	if sink, ok := ctx.Value(genSinkKey{}).(*uint64); ok && err == nil {
		*sink = res.Gen
	}
	return res, err
}

// visibleAt returns when write w is guaranteed visible on instance inst:
// when Delete returned on the deleting instance, otherwise when inst's
// subscriber handled an invalidation of the key at generation >= w.gen.
func (l *lineLog) visibleAt(w lineWrite, inst int) int64 {
	vis := int64(1<<62 - 1)
	if inst == w.deleter {
		vis = w.returned
	}
	for _, p := range l.processed {
		if p.inst == inst && p.key == w.key && p.gen >= w.gen && p.at < vis {
			vis = p.at
		}
	}
	return vis
}

const (
	lineInstances = 3
	lineWriters   = 4
	lineReaders   = 8
	lineWritesPer = 150
	lineReadsPer  = 400
)

// lineHarness is the model database and the cache instances of the
// linearizability test.
type lineHarness struct {
	t     *testing.T
	nodes []*cache[int]
	log   *lineLog
	keys  []string
	dbMu  sync.Mutex // protects db
	db    map[string]int
}

func (h *lineHarness) loadRow(k string) LoadFunc[int] {
	return func(context.Context) (int, error) {
		h.dbMu.Lock()
		defer h.dbMu.Unlock()
		return h.db[k], nil
	}
}

// writer commits row updates and invalidates them through a random
// instance, as cdcsource does after the commit.
func (h *lineHarness) writer(rng *rand.Rand) {
	for range lineWritesPer {
		k := h.keys[rng.IntN(len(h.keys))]
		h.dbMu.Lock()
		h.db[k]++
		ver := h.db[k]
		h.dbMu.Unlock() // COMMIT
		d := rng.IntN(len(h.nodes))
		var gen uint64
		ctx := context.WithValue(context.Background(), genSinkKey{}, &gen)
		if err := h.nodes[d].Delete(ctx, k); err != nil {
			h.t.Errorf("delete: %v", err)
			return
		}
		returned := h.log.tick()
		h.log.mu.Lock()
		h.log.writes = append(h.log.writes, lineWrite{k, ver, gen, d, returned})
		h.log.mu.Unlock()
	}
}

func (h *lineHarness) reader(rng *rand.Rand) {
	for range lineReadsPer {
		k := h.keys[rng.IntN(len(h.keys))]
		i := rng.IntN(len(h.nodes))
		start := h.log.tick()
		v, err := h.nodes[i].GetOrLoad(context.Background(), k, h.loadRow(k))
		if err != nil {
			h.t.Errorf("get or load: %v", err)
			return
		}
		h.log.mu.Lock()
		h.log.reads = append(h.log.reads, lineRead{i, k, start, v})
		h.log.mu.Unlock()
	}
}

// violations counts reads that started after a write was visible on their
// instance and returned an older ver.
func (h *lineHarness) violations() int {
	n := 0
	for _, w := range h.log.writes {
		vis := make([]int64, len(h.nodes))
		for i := range vis {
			vis[i] = h.log.visibleAt(w, i)
		}
		for _, r := range h.log.reads {
			if r.key != w.key || r.start <= vis[r.inst] || r.ver >= w.ver {
				continue
			}
			n++
			if n <= 5 {
				h.t.Errorf("stale read: instance %d key %s started at %d read ver %d; ver %d visible there at %d",
					r.inst, r.key, r.start, r.ver, w.ver, vis[r.inst])
			}
		}
	}
	return n
}

// TestCache_Linearizability_NoStaleReadAfterInvalidation is the core race
// test: writers update rows (each update increments ver) and call
// Delete on a random instance, as cdcsource does after the commit; readers
// on three instances record (start, ver). No read that started after the
// write became visible on its instance may return a lower ver.
//
// Window: on the instance that ran Delete, from Delete's return; on the
// others, from the moment their bus subscriber handled the invalidation
// (bus delivery latency, asynchronous here). With CDC in front, add the
// commit-to-Delete pipeline latency (the stale-read acceptance test uses a 2 s window from COMMIT).
func TestCache_Linearizability_NoStaleReadAfterInvalidation(t *testing.T) {
	t.Parallel()
	seed := uint64(20261018)
	t.Logf("seed %d", seed)
	e := newEnv(t)
	h := &lineHarness{t: t, log: &lineLog{}, keys: []string{"r0", "r1", "r2", "r3"}, db: map[string]int{}}
	for i := range lineInstances {
		c, err := New[int](testNS, JSONCodec[int](), WithClock(e.clk), withBackend(func(cfg l2.Config) (l2.Backend, error) {
			return &recordingBackend{Client: e.srv.Client(cfg), inst: i, log: h.log}, nil
		}))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		n := asImpl(t, c)
		h.nodes = append(h.nodes, n)
		eventually(t, func() bool { return n.busUp.Load() && n.epochOK.Load() })
	}
	var wg sync.WaitGroup
	for w := range lineWriters {
		rng := rand.New(rand.NewPCG(seed, uint64(w)))
		wg.Go(func() { h.writer(rng) })
	}
	for r := range lineReaders {
		rng := rand.New(rand.NewPCG(seed, uint64(100+r)))
		wg.Go(func() { h.reader(rng) })
	}
	wg.Wait()
	e.idle()
	v := h.violations()
	require.Zero(t, v, "%d reads older than a visible write", v)
	require.Len(t, h.log.reads, lineReaders*lineReadsPer)
	require.Len(t, h.log.writes, lineWriters*lineWritesPer)
}
