// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/cache"
)

// WAK-10 runs the CDC invalidation path with Wake running natively on the
// host and the compose Postgres, Kafka and Redis. The criterion fixes the
// threshold, the instances, the update rate, the duration and the cached
// keys; the writer connections, the cache TTL, the poll interval and the
// waits are chosen by the test.
const (
	wak10Table      = "wak10.items"
	wak10Conns      = 8
	wak10PGConns    = 4 // loader connections per instance
	wak10Partitions = 3
	// wak10TTL is far above the run, so a key leaves L1 only by
	// invalidation, never by expiry.
	wak10TTL = 10 * time.Minute
	// wak10Poll is the observer's Get interval per instance.
	wak10Poll = time.Millisecond
	// wak10ObserveTimeout bounds the wait for one update to leave every L1.
	wak10ObserveTimeout = 30 * time.Second
	wak10Seed           = uint64(10)
	wak10Timeout        = 15 * time.Minute
	// wak10MinRate is the share of the offered rate the writers must
	// achieve for the run to count.
	wak10MinRate  = 0.98
	wak10Examples = 5
)

// errWak10Timeout marks an update that did not leave every L1 within
// wak10ObserveTimeout.
var errWak10Timeout = errors.New("not gone from every L1 within the observe timeout")

// wak10Params are the criterion's values.
type wak10Params struct {
	p99Max    float64 // ms
	instances int
	rate      float64 // updates per second
	duration  time.Duration
	keys      int64
}

func wak10ReadParams(b *testing.B, id string, c *harness.Entry) wak10Params {
	b.Helper()
	var p wak10Params
	var err error
	p.p99Max, err = c.Float("latency_p99_ms_max")
	failIf(b, id, err, "threshold latency_p99_ms_max")
	instances, err := c.Int("instances")
	failIf(b, id, err, "threshold instances")
	p.rate, err = c.Float("updates_per_s")
	failIf(b, id, err, "threshold updates_per_s")
	p.duration, err = c.Duration("duration_s")
	failIf(b, id, err, "threshold duration_s")
	p.keys, err = c.Int("cached_keys")
	failIf(b, id, err, "threshold cached_keys")
	p.instances = int(instances)
	if p.instances < 1 || p.keys < 1 || p.rate <= 0 || p.duration <= 0 {
		harness.Failf(b, id, "invalid criterion values %+v", p)
	}
	return p
}

// wak10Commit is one committed update: the row's id, the ver it set and the
// M1 commit time (host clock when COMMIT returned).
type wak10Commit struct {
	id, ver int64
	at      time.Time
}

// wak10Observer measures, for every committed update, the time from its
// commit until the key is gone from the L1 of every instance, then re-warms
// the key on every instance so its next update is measurable.
type wak10Observer struct {
	keys    int64
	nodes   []cacheNode[cdcValue]
	loaders []*cdcLoader
	commits chan wak10Commit
	next    atomic.Int64 // round-robin key counter of the writers
	// ready[id-1] is set while the key is warm on every instance and no
	// observation of it is in flight.
	ready      []atomic.Bool
	overflow   atomic.Int64 // commits the channel could not take (lost samples)
	notReady   atomic.Int64 // updates of a key that was not ready
	dispatched chan struct{}
	wg         sync.WaitGroup

	mu       sync.Mutex // protects the fields below
	lat      []float64  // ms, one per observed update
	timeouts int
	getErrs  int
	rewarms  int // failed or stale re-warms
	examples []string
}

func newWak10Observer(keys int64, nodes []cacheNode[cdcValue], loaders []*cdcLoader, capacity int,
) *wak10Observer {
	o := &wak10Observer{
		keys: keys, nodes: nodes, loaders: loaders, commits: make(chan wak10Commit, capacity),
		ready: make([]atomic.Bool, keys), dispatched: make(chan struct{}), lat: make([]float64, 0, capacity),
	}
	for i := range o.ready {
		o.ready[i].Store(true) // warmed before the run
	}
	return o
}

// tx is the writers' transaction: it increments the ver of the next key in
// round-robin order (each key is updated every keys/rate seconds). It runs
// in autocommit mode: when RETURNING has been scanned the COMMIT has
// returned, which is M1.
func (o *wak10Observer) tx(table string) stack.TxFunc {
	update := "UPDATE " + table + " SET ver = ver + 1, payload = md5(random()::text) WHERE id = $1 RETURNING ver"
	return func(ctx context.Context, conn *pgx.Conn, _ int, _ *rand.Rand) ([]stack.Change, error) {
		id := (o.next.Add(1)-1)%o.keys + 1
		ready := o.ready[id-1].CompareAndSwap(true, false)
		var ver int64
		if err := conn.QueryRow(ctx, update, id).Scan(&ver); err != nil {
			if ready {
				o.ready[id-1].Store(true)
			}
			return nil, err //nolint:wrapcheck // reported in the load summary
		}
		commit := time.Now()
		if !ready {
			o.notReady.Add(1)
		}
		select {
		case o.commits <- wak10Commit{id: id, ver: ver, at: commit}:
		default:
			o.overflow.Add(1)
		}
		return []stack.Change{{Table: table, Key: stack.IDKey(id), Ver: ver, Op: stack.OpUpdate}}, nil
	}
}

// start observes every commit sent by the writers in its own goroutine.
func (o *wak10Observer) start(ctx context.Context) {
	go func() {
		defer close(o.dispatched)
		for c := range o.commits {
			o.wg.Go(func() { o.observe(ctx, c) })
		}
	}()
}

// finish waits for every observation; call it once the writers returned.
func (o *wak10Observer) finish() {
	close(o.commits)
	<-o.dispatched
	o.wg.Wait()
}

func (o *wak10Observer) observe(ctx context.Context, c wak10Commit) {
	lat, err := o.waitGone(ctx, c)
	if err != nil {
		o.mu.Lock()
		if errors.Is(err, errWak10Timeout) {
			o.timeouts++
		} else {
			o.getErrs++
		}
		o.example(fmt.Sprintf("%s ver %d: %v", cdcKey(c.id), c.ver, err))
		o.mu.Unlock()
		return // the key stays not ready: a later update of it is counted
	}
	o.mu.Lock()
	o.lat = append(o.lat, float64(lat)/float64(time.Millisecond))
	o.mu.Unlock()
	o.rewarm(ctx, c)
}

// waitGone polls Get on every instance every wak10Poll until each one
// misses and returns the latest first miss minus the commit time. Get reads
// L1, then L2, never the loader: the invalidator deletes the key from L2
// before it publishes on the bus, so an instance misses once the bus removed
// the key from its L1. A miss is stamped when Get returned (an upper bound).
func (o *wak10Observer) waitGone(ctx context.Context, c wak10Commit) (time.Duration, error) {
	key := cdcKey(c.id)
	gone := make([]bool, len(o.nodes))
	var last time.Time
	tick := time.NewTicker(wak10Poll)
	defer tick.Stop()
	for left := len(o.nodes); ; {
		for i, n := range o.nodes {
			if gone[i] {
				continue
			}
			_, ok, err := n.c.Get(ctx, key)
			if err != nil {
				return 0, fmt.Errorf("instance %d: get: %w", i, err)
			}
			if !ok {
				gone[i], last = true, time.Now()
				left--
			}
		}
		if left == 0 {
			return last.Sub(c.at), nil
		}
		if time.Since(c.at) > wak10ObserveTimeout {
			return 0, fmt.Errorf("%w (%d of %d instances still serve it)", errWak10Timeout, left, len(o.nodes))
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err() //nolint:wrapcheck // context error as is
		case <-tick.C:
		}
	}
}

// rewarm loads the key into every instance (the first one from Postgres,
// the others from L2), then reads it once more on every instance: that read
// also refills an L1 whose fill the shard guard skipped because another key
// of the shard was invalidated meanwhile. Every instance must serve at least
// the committed ver.
func (o *wak10Observer) rewarm(ctx context.Context, c wak10Commit) {
	key := cdcKey(c.id)
	for i, n := range o.nodes {
		v, err := n.c.GetOrLoad(ctx, key, o.loaders[i].load(c.id))
		if err == nil && (v.ID != c.id || v.Ver < c.ver) {
			err = fmt.Errorf("loaded %+v", v)
		}
		if err == nil {
			var ok bool
			if v, ok, err = n.c.Get(ctx, key); err == nil && (!ok || v.Ver < c.ver) {
				err = fmt.Errorf("read after load: %+v (hit %v)", v, ok)
			}
		}
		if err != nil {
			o.mu.Lock()
			o.rewarms++
			o.example(fmt.Sprintf("re-warm %s ver %d on instance %d: %v", key, c.ver, i, err))
			o.mu.Unlock()
			return
		}
	}
	o.ready[c.id-1].Store(true)
}

// example records a failure description; o.mu must be held.
func (o *wak10Observer) example(s string) {
	if len(o.examples) < wak10Examples {
		o.examples = append(o.examples, s)
	}
}

// wak10Run is what the measured phase recorded.
type wak10Run struct {
	res   stack.LoadResult
	obs   *wak10Observer
	lat   []float64 // sorted, ms
	ran   time.Duration
	probe time.Duration // probe commit to every instance having observed it (M7)
	// invalBefore is each instance's invalidations received at the start.
	invalBefore []float64
}

// runWak10 starts the measurement right after M7: the writers (open model)
// run for p.duration, every commit is observed, then the observations end.
func runWak10(ctx context.Context, b *testing.B, id string, p wak10Params, nodes []cacheNode[cdcValue],
	loaders []*cdcLoader,
) wak10Run {
	b.Helper()
	run := wak10Run{invalBefore: make([]float64, len(nodes))}
	for i, n := range nodes {
		run.invalBefore[i] = n.metric("wake_cache_invalidations_received_total")
	}
	// Room for twice the offered updates: a full channel loses a sample and
	// fails the run.
	run.obs = newWak10Observer(p.keys, nodes, loaders, 2*int(p.rate*p.duration.Seconds())+1000)
	run.obs.start(ctx)
	start := time.Now()
	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: p.rate, Duration: p.duration, Conns: wak10Conns, Seed: wak10Seed, Tx: run.obs.tx(wak10Table),
	})
	failIf(b, id, err, "start load")
	b.Cleanup(load.Abort)
	run.res = load.Wait()
	run.ran = time.Since(start)
	run.obs.finish()
	run.lat = slices.Sorted(slices.Values(run.obs.lat))
	return run
}

// BenchmarkAcceptance_WAK10_InvalidationLatency implements WAK-10: CDC invalidation latency from commit to
// key gone from L1 of all 3 instances.
func BenchmarkAcceptance_WAK10_InvalidationLatency(b *testing.B) {
	const id = "WAK-10"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(b, id, "load criterion: "+err.Error())
	}
	p := wak10ReadParams(b, id, c)
	sc := cdcScenario{id: id, table: wak10Table, keys: p.keys}

	stack.EnsureServices(b, "postgres", "kafka", "redis")
	ctx, cancel := context.WithTimeout(context.Background(), wak10Timeout)
	b.Cleanup(cancel)
	failIf(b, id, stack.PingRedis(ctx), "compose redis")
	pg := stack.ConnectPG(b, 2)
	// Seeded before Wake starts: the initial snapshot covers every row (its
	// r events are not invalidated).
	pg.Exec(b, fmt.Sprintf(`DROP SCHEMA IF EXISTS wak10 CASCADE;
CREATE SCHEMA wak10;
CREATE TABLE wak10.items (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text);
INSERT INTO wak10.items SELECT g, 1, md5(g::text) FROM generate_series(1, %d) g;`, sc.probeID()))
	b.Cleanup(func() { dropSchema(b, pg, "wak10") })

	prefix := fmt.Sprintf("w10_%d", time.Now().Unix())
	topic := stack.Topic(prefix, "wak10", "items")
	b.Cleanup(func() { dropTopics(b, topic, stack.SchemaTopic(prefix)) })
	cfg := stack.NativeConfig(wakeConfig("wak10", []string{"wak10.*"}, kafkaSinkPartitions(prefix, wak10Partitions),
		""), b.TempDir())
	wake := stack.StartNativeWake(b, stack.NativeOptions{ID: id, Config: cfg}) //nolint:contextcheck // own timeouts
	failIf(b, id, waitSnapshotDone(ctx, wake), "wait for the initial snapshot")

	ns := uniqueNamespace("wak10")
	inval := sc.startInvalidator(ctx, b, prefix, sc.newDeleter(b, ns, wak10TTL))
	nodes := newCacheNodes[cdcValue](b, id, ns, p.instances, cache.WithTTL(wak10TTL, 0))
	loaders := sc.newLoaders(b, len(nodes), wak10PGConns)
	sc.warm(ctx, b, nodes, loaders)
	probe := sc.waitProbe(ctx, b, pg, nodes)

	run := runWak10(ctx, b, id, p, nodes, loaders)
	run.probe = probe
	finishWak10(b, id, p, run, wake, inval, topic)
}

// wak10Stats are the latency quantiles of a run, in ms.
type wak10Stats struct{ p50, p90, p99, maxMs float64 }

func wak10Quantiles(sorted []float64) wak10Stats {
	s := wak10Stats{p50: quantile(sorted, 0.50), p90: quantile(sorted, 0.90), p99: quantile(sorted, 0.99)}
	if len(sorted) > 0 {
		s.maxMs = sorted[len(sorted)-1]
	}
	return s
}

// finishWak10 applies the thresholds and records the result.
func finishWak10(b *testing.B, id string, p wak10Params, run wak10Run, wake *stack.NativeWake,
	inval *cdcInvalidator, topic string,
) {
	b.Helper()
	st := wak10Quantiles(run.lat)
	problems := wak10Problems(p, run, st, wake, inval)
	o, res := run.obs, run.res
	b.ReportMetric(st.p99, "p99-ms")
	b.ReportMetric(st.p50, "p50-ms")
	harness.Finish(b, harness.Result{
		ID: id,
		Measured: map[string]any{
			"latency_p99_ms": st.p99, "latency_p50_ms": st.p50, "latency_p90_ms": st.p90,
			"latency_max_ms": st.maxMs, "updates": len(run.lat), "updates_committed": res.Committed,
			"instances": len(o.nodes), "updates_per_s": res.AchievedRate(), "updates_per_s_offered": p.rate,
			"duration_s": run.ran.Seconds(), "cached_keys": p.keys, "timeouts": o.timeouts,
			"get_errors": o.getErrs, "rewarm_failures": o.rewarms, "lost_samples": o.overflow.Load(),
			"updates_not_ready": o.notReady.Load(), "load_errors": res.Errors, "load_dropped": res.Dropped,
			"probe_observed_s": run.probe.Seconds(), "runs": 1,
		},
		Pass:    len(problems) == 0,
		Details: wak10Details(p, run, st, inval, topic, problems),
	})
}

// wak10Problems lists why the run does not pass; empty when it passes.
func wak10Problems(p wak10Params, run wak10Run, st wak10Stats, wake *stack.NativeWake, inval *cdcInvalidator,
) []string {
	var out []string
	add := func(failed bool, format string, args ...any) {
		if failed {
			out = append(out, fmt.Sprintf(format, args...))
		}
	}
	o, res := run.obs, run.res
	add(len(run.lat) == 0, "no update observed")
	add(!(st.p99 < p.p99Max), "p99 %.1f ms >= %.0f ms", st.p99, p.p99Max) // also NaN
	add(len(run.lat) != res.Committed, "observed updates %d != committed updates %d", len(run.lat), res.Committed)
	add(o.timeouts > 0, "updates not gone from every L1 within %s: %d", wak10ObserveTimeout, o.timeouts)
	add(o.getErrs > 0, "observer get errors %d", o.getErrs)
	add(o.rewarms > 0, "failed or stale re-warms %d", o.rewarms)
	add(o.overflow.Load() > 0, "lost samples (observer channel full) %d", o.overflow.Load())
	add(o.notReady.Load() > 0, "updates of a key not re-warmed since its previous update %d", o.notReady.Load())
	add(res.Errors > 0 || res.Dropped > 0, "load errors %d, dropped %d", res.Errors, res.Dropped)
	add(res.AchievedRate() < wak10MinRate*p.rate, "achieved %.1f updates/s < %.0f%% of %.0f", res.AchievedRate(),
		wak10MinRate*100, p.rate)
	add(run.ran < p.duration, "ran %s < %s", run.ran.Round(time.Millisecond), p.duration)
	if exited, err := wake.Exited(); exited {
		add(true, "native wake exited during the run: %v", err)
	}
	if exited, err := inval.exited(); exited {
		add(true, "invalidator stopped during the run: %v", err)
	}
	return out
}

func wak10Details(p wak10Params, run wak10Run, st wak10Stats, inval *cdcInvalidator, topic string,
	problems []string,
) string {
	o := run.obs
	per := make([]string, len(o.nodes))
	var loaderCalls int64
	for i, n := range o.nodes {
		per[i] = fmt.Sprintf("instance %d: invalidations received %.0f, l1 hits %.0f, l2 hits %.0f, loads %.0f, "+
			"l2 errors %.0f", i, n.metric("wake_cache_invalidations_received_total")-run.invalBefore[i],
			n.metric("wake_cache_hits_total", "tier", "l1"), n.metric("wake_cache_hits_total", "tier", "l2"),
			n.metric("wake_cache_loads_total"), n.metric("wake_cache_l2_errors_total"))
		loaderCalls += o.loaders[i].calls.Load()
	}
	inv := inval.node
	d := fmt.Sprintf("Wake native; Postgres -> Wake -> Kafka (%d partitions) -> cdcsource invalidator (own "+
		"cache instance) -> Redis + bus -> L1 of %d instances (TTL %s, own Redis client each); %d cached keys; "+
		"M7: the probe update was observed by every instance %s after its commit, measurement started there; "+
		"writers %s (UPDATE ver = ver + 1, keys round-robin so each key changes every %s, %d conns); latency = "+
		"M1 (host clock when COMMIT returned) to the last instance's first Get miss (L1 and L2 gone, polled "+
		"every %s per instance), then the key is re-warmed on every instance; one run of %s as the criterion "+
		"fixes (not the median of 3); %d updates observed of %d committed: p50 %.1f ms, p90 %.1f ms, p99 %.1f "+
		"ms, max %.1f ms; timeouts %d, get errors %d, re-warm failures %d, lost samples %d, not ready %d; "+
		"loader calls %d; invalidator: events %.0f, keys deleted %.0f, delete failures %.0f deferred / %.0f "+
		"error; %s; topic %s",
		wak10Partitions, len(o.nodes), wak10TTL, p.keys, run.probe.Round(time.Millisecond), run.res.Summary(),
		time.Duration(float64(p.keys)/p.rate*float64(time.Second)), wak10Conns, wak10Poll,
		run.ran.Round(time.Millisecond), len(run.lat), run.res.Committed, st.p50, st.p90, st.p99, st.maxMs,
		o.timeouts, o.getErrs, o.rewarms, o.overflow.Load(), o.notReady.Load(), loaderCalls,
		inv.metric("wake_cdcsource_events_total"), inv.metric("wake_cdcsource_keys_deleted_total"),
		inv.metric("wake_cdcsource_delete_failures_total", "kind", "deferred"),
		inv.metric("wake_cdcsource_delete_failures_total", "kind", "error"), strings.Join(per, "; "), topic)
	if len(o.examples) > 0 {
		d += "; first failures: " + strings.Join(o.examples, " | ")
	}
	if len(run.res.FirstErrs) > 0 {
		d += "; load errors: " + strings.Join(run.res.FirstErrs, "; ")
	}
	if len(problems) > 0 {
		d += "; FAIL: " + strings.Join(problems, "; ")
	}
	return d
}
