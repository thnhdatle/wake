// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/cache"
)

// WAK-11 scenario parameters. The criterion fixes the outage, the TTLs and
// the failing share of keys; the key count, the read load, the steady phases
// around the outage and the waits are chosen by the test.
const (
	wak11Instances  = 3
	wak11Keys       = 1000
	wak11ReadersPer = 8 // per instance
	wak11ReadPause  = time.Millisecond
	wak11Steady     = 15 * time.Second // read load before the kill
	wak11After      = 30 * time.Second // read load after recovery
	wak11RecoverMax = 2 * time.Minute  // bound of the recovery wait
	wak11Poll       = 50 * time.Millisecond
	wak11Seed       = uint64(11)
	wak11Timeout    = 15 * time.Minute
)

// wak11Value is the cached value: the key it belongs to and when the loader
// produced it, so a reader can compute the age of what it was served.
type wak11Value struct {
	Key      string `json:"key"`
	LoadedAt int64  `json:"loaded_at"` // unix nanoseconds
}

// wak11Loader is the primary database of the scenario: while failing is
// set it returns an error for the keys selected by fails.
type wak11Loader struct {
	failing  atomic.Bool
	fails    func(i int) bool
	loads    atomic.Int64
	injected atomic.Int64
}

func (l *wak11Loader) load(key string, i int) cache.LoadFunc[wak11Value] {
	return func(context.Context) (wak11Value, error) {
		l.loads.Add(1)
		if l.failing.Load() && l.fails(i) {
			l.injected.Add(1)
			return wak11Value{}, fmt.Errorf("primary unavailable for %s", key)
		}
		return wak11Value{Key: key, LoadedAt: time.Now().UnixNano()}, nil
	}
}

// wak11Stats aggregates what the readers observed.
type wak11Stats struct {
	reads, outageReads, errors, wrong atomic.Int64
	outage                            atomic.Bool

	mu        sync.Mutex // protects the fields below
	firstErrs []string
	maxAge    time.Duration
	maxAgeKey string
}

func (s *wak11Stats) record(key string, start time.Time, v wak11Value, err error) {
	s.reads.Add(1)
	if s.outage.Load() {
		s.outageReads.Add(1)
	}
	if err != nil {
		s.errors.Add(1)
		s.mu.Lock()
		if len(s.firstErrs) < 5 {
			s.firstErrs = append(s.firstErrs, start.UTC().Format(time.RFC3339Nano)+" "+err.Error())
		}
		s.mu.Unlock()
		return
	}
	if v.Key != key {
		s.wrong.Add(1)
		return
	}
	age := start.Sub(time.Unix(0, v.LoadedAt))
	s.mu.Lock()
	if age > s.maxAge {
		s.maxAge, s.maxAgeKey = age, key
	}
	s.mu.Unlock()
}

// wak11Readers is the read load: wak11Readers goroutines per instance, each
// calling GetOrLoad on uniformly random keys with a short pause.
type wak11Readers struct {
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func startWak11Readers(ctx context.Context, nodes []cacheNode[wak11Value], ld *wak11Loader, st *wak11Stats,
) *wak11Readers {
	r := &wak11Readers{stop: make(chan struct{})}
	for ni, n := range nodes {
		for j := range wak11ReadersPer {
			rng := rand.New(rand.NewPCG(wak11Seed, uint64(ni*wak11ReadersPer+j))) //nolint:gosec // workload choice
			r.wg.Go(func() {
				for {
					select {
					case <-r.stop:
						return
					case <-ctx.Done():
						return
					case <-time.After(wak11ReadPause):
					}
					i := rng.IntN(wak11Keys)
					key := wak11Key(i)
					start := time.Now()
					v, err := n.c.GetOrLoad(ctx, key, ld.load(key, i))
					st.record(key, start, v, err)
				}
			})
		}
	}
	return r
}

// Stop ends the read load and waits for every reader.
func (r *wak11Readers) Stop() {
	r.once.Do(func() { close(r.stop) })
	r.wg.Wait()
}

func wak11Key(i int) string { return "item:" + strconv.Itoa(i) }

// wak11Recovery is when each instance used L2 again after Redis was back.
type wak11Recovery struct {
	back      time.Time       // first successful PING after the restart
	firstL2   []time.Duration // per instance: first L2 hit after back, relative to back; -1 if none
	recovered bool
	worst     time.Duration
}

// TestAcceptance_WAK11_RedisOutage implements WAK-11: Redis killed for 2 min under read load; stale served,
// automatic recovery.
func TestAcceptance_WAK11_RedisOutage(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-11"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	durs := map[string]time.Duration{}
	for _, k := range []string{"outage_s", "l1_ttl_s", "stale_if_error_s", "stale_age_s_max", "recovery_s_max"} {
		durs[k], err = c.Duration(k)
		failIf(t, id, err, "threshold "+k)
	}
	failurePct, err := c.Int("loader_failure_pct")
	failIf(t, id, err, "threshold loader_failure_pct")
	callerErrorsMax, err := c.Int("caller_errors_max")
	failIf(t, id, err, "threshold caller_errors_max")

	stack.EnsureUp(t)
	ctx := withTimeout(t, wak11Timeout)
	failIf(t, id, stack.PingRedis(ctx), "compose redis")
	ns := uniqueNamespace("wak11")
	nodes := newCacheNodes[wak11Value](t, id, ns, wak11Instances,
		cache.WithTTL(durs["l1_ttl_s"], 0), cache.WithStaleIfError(durs["stale_if_error_s"]))
	ld := &wak11Loader{fails: func(i int) bool { return i%100 < int(failurePct) }}
	st := &wak11Stats{}

	warmAll(ctx, t, id, nodes, ld)
	readers := startWak11Readers(ctx, nodes, ld, st)
	t.Cleanup(readers.Stop) // runs before the instances close
	sleepCtx(ctx, wak11Steady)

	outage := runOutage(ctx, t, id, nodes, ld, st, durs["outage_s"])
	rec := measureRecovery(ctx, nodes, outage.l2HitsBefore, outage.back)
	sleepCtx(ctx, wak11After)
	readers.Stop()

	valueKeys, scanErr := countValueKeys(ctx, t, ns)
	staleServes := sumMetric(nodes, "wake_cache_stale_serves_total")
	if ld.injected.Load() == 0 || staleServes == 0 || !outage.breakersOpened {
		harness.Failf(t, id, "outage not exercised: loader failures %d, stale serves %.0f, breakers opened %v",
			ld.injected.Load(), staleServes, outage.breakersOpened)
	}
	st.mu.Lock()
	maxAge, maxAgeKey, firstErrs := st.maxAge, st.maxAgeKey, st.firstErrs
	st.mu.Unlock()
	errs := st.errors.Load()
	pass := errs <= callerErrorsMax && st.wrong.Load() == 0 && maxAge <= durs["stale_age_s_max"] &&
		rec.recovered && rec.worst <= durs["recovery_s_max"]
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"caller_errors": errs, "wrong_values": st.wrong.Load(), "stale_age_s": maxAge.Seconds(),
			"recovery_s": rec.worst.Seconds(), "recovered": rec.recovered,
			"outage_s": outage.duration.Seconds(), "l1_ttl_s": durs["l1_ttl_s"].Seconds(),
			"stale_if_error_s": durs["stale_if_error_s"].Seconds(), "loader_failure_pct": failurePct,
			"reads": st.reads.Load(), "outage_reads": st.outageReads.Load(), "stale_serves": staleServes,
			"loader_calls": ld.loads.Load(), "loader_failures": ld.injected.Load(),
			"l2_value_keys_after": valueKeys,
		},
		Pass: pass,
		Details: wak11Details(nodes, rec, outage, maxAge, maxAgeKey, firstErrs, valueKeys, scanErr) +
			fmt.Sprintf("; stale_age_s = max over all reads of (read start - time the loader produced the "+
				"value), an upper bound of the time since expiry; keys %d, %d instances x %d readers",
				wak11Keys, wak11Instances, wak11ReadersPer),
	})
}

// warmAll loads every key on every instance before the kill.
func warmAll(ctx context.Context, t *testing.T, id string, nodes []cacheNode[wak11Value], ld *wak11Loader) {
	t.Helper()
	for i := range wak11Keys {
		key := wak11Key(i)
		for _, n := range nodes {
			v, err := n.c.GetOrLoad(ctx, key, ld.load(key, i))
			failIf(t, id, err, "warm "+key)
			if v.Key != key {
				harness.Failf(t, id, "warm %s: got the value of %q", key, v.Key)
			}
		}
	}
}

// wak11Outage is what happened between the kill and Redis answering again.
type wak11Outage struct {
	killed, back   time.Time
	duration       time.Duration
	breakersOpened bool
	l2HitsBefore   []float64
}

// runOutage kills Redis, makes the loader fail for its share of keys, keeps
// Redis down for outage while checking that every instance opened its
// breaker and lost its bus, then clears the loader failures and starts Redis
// again. back is the first successful PING after the start.
func runOutage(ctx context.Context, t *testing.T, id string, nodes []cacheNode[wak11Value], ld *wak11Loader,
	st *wak11Stats, outage time.Duration,
) wak11Outage {
	t.Helper()
	t.Cleanup(func() { // never leave the shared stack without Redis
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if stack.PingRedis(cctx) != nil {
			_ = stack.StartRedis(cctx)
		}
	})
	failIf(t, id, stack.KillRedis(ctx), "kill redis")
	o := wak11Outage{killed: time.Now()}
	st.outage.Store(true)
	ld.failing.Store(true)
	opened := make([]bool, len(nodes))
	end := o.killed.Add(outage)
	for time.Now().Before(end) && ctx.Err() == nil {
		if stack.PingRedis(ctx) == nil {
			harness.Fail(t, id, "redis answered during the outage (restarted by another process?)")
		}
		for i, n := range nodes {
			if n.metric("wake_cache_breaker_state") != 0 && n.metric("wake_cache_bus_connected") == 0 {
				opened[i] = true
			}
		}
		sleepCtx(ctx, min(500*time.Millisecond, time.Until(end)))
	}
	o.breakersOpened = true
	for _, b := range opened {
		o.breakersOpened = o.breakersOpened && b
	}
	// Redis is still down: no L2 hit can happen until it is back.
	o.l2HitsBefore = make([]float64, len(nodes))
	for i, n := range nodes {
		o.l2HitsBefore[i] = n.metric("wake_cache_hits_total", "tier", "l2")
	}
	ld.failing.Store(false)
	backCh := make(chan time.Time, 1)
	wctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	go func() {
		at, _ := stack.WaitRedis(wctx, wak11Poll)
		backCh <- at
	}()
	failIf(t, id, stack.StartRedis(ctx), "start redis")
	o.back = <-backCh
	st.outage.Store(false)
	if o.back.IsZero() {
		harness.Fail(t, id, "redis did not answer PING after the restart")
	}
	o.duration = o.back.Sub(o.killed)
	return o
}

// measureRecovery polls each instance until its L2 hit counter rises above
// its value during the outage (a value read from Redis again) with its
// breaker closed, and reports each instance's delay from back.
func measureRecovery(ctx context.Context, nodes []cacheNode[wak11Value], before []float64, back time.Time,
) wak11Recovery {
	rec := wak11Recovery{back: back, firstL2: make([]time.Duration, len(nodes))}
	for i := range rec.firstL2 {
		rec.firstL2[i] = -1
	}
	deadline := back.Add(wak11RecoverMax)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		done := true
		for i, n := range nodes {
			if rec.firstL2[i] >= 0 {
				continue
			}
			if n.metric("wake_cache_breaker_state") == 0 && n.metric("wake_cache_hits_total", "tier", "l2") > before[i] {
				rec.firstL2[i] = time.Since(back)
				continue
			}
			done = false
		}
		if done {
			rec.recovered = true
			break
		}
		sleepCtx(ctx, wak11Poll)
	}
	for _, d := range rec.firstL2 {
		rec.worst = max(rec.worst, d)
	}
	if !rec.recovered {
		rec.worst = wak11RecoverMax
	}
	return rec
}

// countValueKeys counts the L2 value keys (<ns>:e<epoch>:{<key>}:v) of the
// namespace in Redis: direct evidence that L2 is written again.
func countValueKeys(ctx context.Context, t *testing.T, ns string) (int, error) {
	t.Helper()
	rdb := stack.NewRedisClient(t)
	n := 0
	iter := rdb.Scan(ctx, 0, ns+":e*:v", 1000).Iterator()
	for iter.Next(ctx) {
		n++
	}
	return n, iter.Err() //nolint:wrapcheck // reported in the details
}

func wak11Details(nodes []cacheNode[wak11Value], rec wak11Recovery, o wak11Outage, maxAge time.Duration,
	maxAgeKey string, firstErrs []string, valueKeys int, scanErr error,
) string {
	per := make([]string, len(nodes))
	for i, n := range nodes {
		per[i] = fmt.Sprintf("instance %d: first L2 hit %s after back, breaker %.0f, bus %.0f, l2 hits %.0f, "+
			"loads %.0f, stale serves %.0f, l2 errors %.0f", i, rec.firstL2[i].Round(time.Millisecond),
			n.metric("wake_cache_breaker_state"), n.metric("wake_cache_bus_connected"),
			n.metric("wake_cache_hits_total", "tier", "l2"), n.metric("wake_cache_loads_total"),
			n.metric("wake_cache_stale_serves_total"), n.metric("wake_cache_l2_errors_total"))
	}
	d := fmt.Sprintf("redis killed %s, PING answered again %s (outage %s); breakers opened and buses lost on "+
		"every instance: %v; oldest value served: %s for %s; recovery = max over instances of first L2 hit "+
		"after back; %s; L2 value keys after the run: %d",
		o.killed.UTC().Format(time.RFC3339Nano), rec.back.UTC().Format(time.RFC3339Nano),
		o.duration.Round(time.Millisecond), o.breakersOpened, maxAge.Round(time.Millisecond), maxAgeKey,
		strings.Join(per, "; "), valueKeys)
	if scanErr != nil {
		d += " (scan error: " + scanErr.Error() + ")"
	}
	if len(firstErrs) > 0 {
		d += "; first caller errors: " + strings.Join(firstErrs, " | ")
	}
	return d
}

// sleepCtx waits for d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-tm.C:
	case <-ctx.Done():
	}
}
