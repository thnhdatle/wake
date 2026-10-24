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

type lineLog struct {
	clock atomic.Int64

	mu        sync.Mutex
	writes    []lineWrite
	reads     []lineRead
	processed []lineProcessed
}

type lineWrite struct {
	key      string
	ver      int
	gen      uint64
	deleter  int
	returned int64
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
	at   int64
}

func (l *lineLog) tick() int64 { return l.clock.Add(1) }

type genSinkKey struct{}

type recordingBackend struct {
	*l2fake.Client
	inst int
	log  *lineLog
	prev *l2.Event
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

type lineHarness struct {
	t     *testing.T
	nodes []*cache[int]
	log   *lineLog
	keys  []string
	dbMu  sync.Mutex
	db    map[string]int
}

func (h *lineHarness) loadRow(k string) LoadFunc[int] {
	return func(context.Context) (int, error) {
		h.dbMu.Lock()
		defer h.dbMu.Unlock()
		return h.db[k], nil
	}
}

func (h *lineHarness) writer(rng *rand.Rand) {
	for range lineWritesPer {
		k := h.keys[rng.IntN(len(h.keys))]
		h.dbMu.Lock()
		h.db[k]++
		ver := h.db[k]
		h.dbMu.Unlock()
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
