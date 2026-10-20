// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
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

// WAK-12 scenario parameters. The criterion fixes the duration, the window
// and the stale read bound; the key count, the write rate, the read load,
// the cache TTL and the waits are chosen by the test.
const (
	wak12Table     = "wak12.items"
	wak12Instances = 3
	wak12Keys      = 1000
	// wak12TxRate single-row update transactions per second (open model):
	// each key changes every 10 s on average.
	wak12TxRate     = 100.0
	wak12Conns      = 8
	wak12ReadersPer = 8 // per instance
	// wak12ReadEvery paces each reader (at most one read per tick).
	wak12ReadEvery = 2 * time.Millisecond
	wak12PGConns   = 4 // loader connections per instance
	// wak12TTL is far above the run, so freshness comes only from
	// invalidation, never from expiry.
	wak12TTL        = 10 * time.Minute
	wak12Partitions = 3
	wak12Seed       = uint64(12)
	wak12Timeout    = 15 * time.Minute
	wak12Examples   = 5
)

// wak12Tx increments the ver of a random row. It runs in autocommit mode:
// when RETURNING has been scanned the COMMIT has returned, and the
// generator stamps that time (M1).
func wak12Tx(ctx context.Context, conn *pgx.Conn, _ int, rng *rand.Rand) ([]stack.Change, error) {
	id := 1 + rng.Int64N(wak12Keys)
	var ver int64
	err := conn.QueryRow(ctx, `UPDATE wak12.items SET ver = ver + 1, payload = md5(random()::text)
		WHERE id = $1 RETURNING ver`, id).Scan(&ver)
	if err != nil {
		return nil, err //nolint:wrapcheck // reported in the load summary
	}
	return []stack.Change{{Table: wak12Table, Key: stack.IDKey(id), Ver: ver, Op: stack.OpUpdate}}, nil
}

// wak12Read is one successful read: when GetOrLoad was called (relative to
// the measurement start, monotonic clock), the ver it returned, the key's id
// and the instance.
type wak12Read struct {
	at   time.Duration
	ver  int64
	id   int32
	node int32
}

// wak12Reads is what the readers recorded.
type wak12Reads struct {
	per    [][]wak12Read // per reader goroutine
	errors atomic.Int64
	wrong  atomic.Int64

	mu        sync.Mutex // protects firstErrs
	firstErrs []string
}

func (r *wak12Reads) fail(at time.Duration, node int, id int64, err error) {
	r.errors.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.firstErrs) < wak12Examples {
		r.firstErrs = append(r.firstErrs, fmt.Sprintf("+%s instance %d %s: %v", at.Round(time.Millisecond),
			node, cdcKey(id), err))
	}
}

func (r *wak12Reads) total() int {
	n := int(r.errors.Load() + r.wrong.Load())
	for _, rs := range r.per {
		n += len(rs)
	}
	return n
}

// runWak12Readers runs wak12ReadersPer goroutines per instance from base
// until base + d, each calling GetOrLoad on uniformly random keys at most
// once per wak12ReadEvery, and returns what they recorded.
func runWak12Readers(ctx context.Context, nodes []cacheNode[cdcValue], loaders []*cdcLoader, base time.Time,
	d time.Duration,
) *wak12Reads {
	r := &wak12Reads{per: make([][]wak12Read, len(nodes)*wak12ReadersPer)}
	var wg sync.WaitGroup
	for ni := range nodes {
		for j := range wak12ReadersPer {
			slot := ni*wak12ReadersPer + j
			rng := rand.New(rand.NewPCG(wak12Seed, uint64(slot)+1)) //nolint:gosec // workload choice
			wg.Go(func() { r.per[slot] = r.read(ctx, nodes[ni], ni, loaders[ni], rng, base, d) })
		}
	}
	wg.Wait()
	return r
}

// read is one reader goroutine.
func (r *wak12Reads) read(ctx context.Context, n cacheNode[cdcValue], node int, ld *cdcLoader, rng *rand.Rand,
	base time.Time, d time.Duration,
) []wak12Read {
	out := make([]wak12Read, 0, int(d/wak12ReadEvery))
	tick := time.NewTicker(wak12ReadEvery)
	defer tick.Stop()
	for ctx.Err() == nil {
		at := time.Since(base) // read start
		if at >= d {
			break
		}
		id := 1 + rng.Int64N(wak12Keys)
		v, err := n.c.GetOrLoad(ctx, cdcKey(id), ld.load(id))
		switch {
		case err != nil:
			r.fail(at, node, id, err)
		case v.ID != id:
			r.wrong.Add(1)
		default:
			out = append(out, wak12Read{at: at, ver: v.Ver, id: int32(id), node: int32(node)}) //nolint:gosec // small
		}
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
	return out
}

// wak12Run is what the measured phase recorded.
type wak12Run struct {
	res   stack.LoadResult
	reads *wak12Reads
	base  time.Time // measurement start (M7)
	ran   time.Duration
	probe time.Duration // probe commit to every instance having observed it
	// invalBefore is each instance's invalidations received at base.
	invalBefore []float64
}

// runWak12 starts the measurement right after M7: the writers (open model)
// and the readers run for d.
func runWak12(ctx context.Context, t *testing.T, id string, nodes []cacheNode[cdcValue], loaders []*cdcLoader,
	d time.Duration,
) wak12Run {
	t.Helper()
	run := wak12Run{base: time.Now(), invalBefore: make([]float64, len(nodes))}
	for i, n := range nodes {
		run.invalBefore[i] = n.metric("wake_cache_invalidations_received_total")
	}
	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: wak12TxRate, Duration: d, Conns: wak12Conns, Seed: wak12Seed, Tx: wak12Tx,
	})
	failIf(t, id, err, "start load")
	t.Cleanup(load.Abort)
	run.reads = runWak12Readers(ctx, nodes, loaders, run.base, d)
	run.ran = time.Since(run.base)
	run.res = load.Wait()
	return run
}

// wak12History is one key's committed updates in commit order: commit
// times relative to the measurement start and the highest ver committed up
// to and including each one.
type wak12History struct {
	at     []time.Duration
	maxVer []int64
}

func wak12Histories(changes []stack.Change, base time.Time) map[int32]*wak12History {
	ids := make(map[string]int32, wak12Keys)
	for k := int64(1); k <= wak12Keys; k++ {
		ids[stack.IDKey(k)] = int32(k)
	}
	hist := make(map[int32]*wak12History, wak12Keys)
	for _, c := range changes { // sorted by commit time
		k, ok := ids[c.Key]
		if !ok || c.Table != wak12Table {
			continue
		}
		h := hist[k]
		if h == nil {
			h = &wak12History{}
			hist[k] = h
		}
		top := c.Ver
		if n := len(h.maxVer); n > 0 {
			top = max(top, h.maxVer[n-1])
		}
		h.at = append(h.at, c.Commit.Sub(base))
		h.maxVer = append(h.maxVer, top)
	}
	return hist
}

// wak12Check is the outcome of the stale-read check.
type wak12Check struct {
	counted, stale int
	// maxBehind is the largest (read start - commit time of the first ver
	// above the one returned) over the stale reads.
	maxBehind time.Duration
	worst     string
	examples  []string
}

// checkWak12 is the stale-read checker: a read of key k that started at s and
// returned ver v is stale if a commit of k with a ver above v returned at c
// with c + window <= s. Only reads that started after the M7 point count.
func checkWak12(changes []stack.Change, reads [][]wak12Read, base time.Time, window time.Duration) wak12Check {
	hist := wak12Histories(changes, base)
	var ch wak12Check
	for _, rs := range reads {
		for _, r := range rs {
			if r.at < 0 {
				continue
			}
			ch.counted++
			if h := hist[r.id]; h != nil {
				ch.check(r, h, window)
			}
		}
	}
	return ch
}

func (ch *wak12Check) check(r wak12Read, h *wak12History, window time.Duration) {
	// Commits that returned at or before the read start minus the window.
	n := sort.Search(len(h.at), func(i int) bool { return h.at[i] > r.at-window })
	if n == 0 || h.maxVer[n-1] <= r.ver {
		return
	}
	ch.stale++
	first := sort.Search(n, func(i int) bool { return h.maxVer[i] > r.ver })
	behind := r.at - h.at[first]
	desc := fmt.Sprintf("instance %d read %s at +%s and got ver %d; ver %d committed at +%s (%s before the read), "+
		"ver %d by read start - window", r.node, cdcKey(int64(r.id)), r.at.Round(time.Millisecond), r.ver,
		h.maxVer[first], h.at[first].Round(time.Millisecond), behind.Round(time.Millisecond), h.maxVer[n-1])
	if len(ch.examples) < wak12Examples {
		ch.examples = append(ch.examples, desc)
	}
	if behind > ch.maxBehind {
		ch.maxBehind, ch.worst = behind, desc
	}
}

// wak12Thresholds are the criterion's thresholds.
type wak12Thresholds struct {
	duration, window time.Duration
	staleMax         int64
}

// TestAcceptance_WAK12_NoStaleBeyondWindow implements WAK-12: 3-min read/write race through CDC invalidation;
// no read older than commit + 2 s.
func TestAcceptance_WAK12_NoStaleBeyondWindow(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-12"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	var th wak12Thresholds
	th.duration, err = c.Duration("duration_s")
	failIf(t, id, err, "threshold duration_s")
	th.window, err = c.Duration("window_s")
	failIf(t, id, err, "threshold window_s")
	th.staleMax, err = c.Int("stale_reads_max")
	failIf(t, id, err, "threshold stale_reads_max")

	stack.EnsureServices(t, "postgres", "kafka", "redis")
	ctx := withTimeout(t, wak12Timeout)
	failIf(t, id, stack.PingRedis(ctx), "compose redis")
	pg := stack.ConnectPG(t, 2)
	sc := cdcScenario{id: id, table: wak12Table, keys: wak12Keys}
	// Seeded before Wake starts: the initial snapshot covers every row (its
	// r events are not invalidated).
	pg.Exec(t, fmt.Sprintf(`DROP SCHEMA IF EXISTS wak12 CASCADE;
CREATE SCHEMA wak12;
CREATE TABLE wak12.items (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text);
INSERT INTO wak12.items SELECT g, 1, md5(g::text) FROM generate_series(1, %d) g;`, sc.probeID()))
	t.Cleanup(func() { dropSchema(t, pg, "wak12") })

	prefix := fmt.Sprintf("w12_%d", time.Now().Unix())
	topic := stack.Topic(prefix, "wak12", "items")
	t.Cleanup(func() { dropTopics(t, topic, stack.SchemaTopic(prefix)) })
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID: id, Config: wakeConfig("wak12", []string{"wak12.*"}, kafkaSinkPartitions(prefix, wak12Partitions), ""),
		FreshState: true,
	})
	failIf(t, id, waitSnapshotDone(ctx, wake), "wait for the initial snapshot")

	ns := uniqueNamespace("wak12")
	inval := sc.startInvalidator(ctx, t, prefix, sc.newDeleter(t, ns, wak12TTL))
	nodes := newCacheNodes[cdcValue](t, id, ns, wak12Instances, cache.WithTTL(wak12TTL, 0))
	loaders := sc.newLoaders(t, len(nodes), wak12PGConns)
	sc.warm(ctx, t, nodes, loaders)
	probe := sc.waitProbe(ctx, t, pg, nodes)

	run := runWak12(ctx, t, id, nodes, loaders, th.duration)
	run.probe = probe
	finishWak12(t, id, th, run, nodes, loaders, inval, topic)
}

// finishWak12 applies the checker and records the result.
func finishWak12(t *testing.T, id string, th wak12Thresholds, run wak12Run, nodes []cacheNode[cdcValue],
	loaders []*cdcLoader, inval *cdcInvalidator, topic string,
) {
	t.Helper()
	ch := checkWak12(run.res.Changes, run.reads.per, run.base, th.window)
	exited, runErr := inval.exited()
	keysDeleted := inval.node.metric("wake_cdcsource_keys_deleted_total")
	problems := wak12Problems(th, run, ch, keysDeleted, exited, runErr)
	var invalidations, loaderCalls float64
	for i, n := range nodes {
		invalidations += n.metric("wake_cache_invalidations_received_total") - run.invalBefore[i]
		loaderCalls += float64(loaders[i].calls.Load())
	}
	res := run.res
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"stale_reads": ch.stale, "duration_s": run.ran.Seconds(), "window_s": th.window.Seconds(),
			"reads_counted": ch.counted, "reads_total": run.reads.total(), "caller_errors": run.reads.errors.Load(),
			"wrong_values": run.reads.wrong.Load(), "writes_committed": res.Committed,
			"tx_per_s_achieved": res.AchievedRate(), "load_errors": res.Errors, "load_dropped": res.Dropped,
			"max_stale_behind_s": ch.maxBehind.Seconds(), "probe_observed_s": run.probe.Seconds(),
			"invalidations_received": invalidations, "keys_deleted": keysDeleted, "loader_calls": loaderCalls,
			"instances": len(nodes), "keys": wak12Keys,
		},
		Pass:    len(problems) == 0,
		Details: wak12Details(th, run, ch, nodes, inval, topic, problems),
	})
}

// wak12Problems lists why the run does not pass; empty when it passes.
func wak12Problems(th wak12Thresholds, run wak12Run, ch wak12Check, keysDeleted float64, exited bool, runErr error,
) []string {
	var p []string
	add := func(failed bool, format string, args ...any) {
		if failed {
			p = append(p, fmt.Sprintf(format, args...))
		}
	}
	res := run.res
	add(int64(ch.stale) > th.staleMax, "stale reads %d > %d", ch.stale, th.staleMax)
	add(ch.counted == 0, "no read counted")
	add(res.Committed == 0, "no write committed")
	add(run.reads.errors.Load() > 0, "caller errors %d", run.reads.errors.Load())
	add(run.reads.wrong.Load() > 0, "wrong values %d", run.reads.wrong.Load())
	add(res.Errors > 0 || res.Dropped > 0, "load errors %d, dropped %d", res.Errors, res.Dropped)
	add(run.ran < th.duration || float64(res.Offered) < th.duration.Seconds()*wak12TxRate,
		"ran %s with %d offered transactions, below %s at %.0f tx/s", run.ran.Round(time.Millisecond),
		res.Offered, th.duration, wak12TxRate)
	add(exited, "invalidator stopped during the run: %v", runErr)
	add(keysDeleted == 0, "the invalidator deleted no key (CDC path not exercised)")
	return p
}

func wak12Details(th wak12Thresholds, run wak12Run, ch wak12Check, nodes []cacheNode[cdcValue],
	inval *cdcInvalidator, topic string, problems []string,
) string {
	per := make([]string, len(nodes))
	for i, n := range nodes {
		per[i] = fmt.Sprintf("instance %d: invalidations received %.0f, l1 hits %.0f, l2 hits %.0f, loads %.0f, "+
			"l2 errors %.0f", i, n.metric("wake_cache_invalidations_received_total")-run.invalBefore[i],
			n.metric("wake_cache_hits_total", "tier", "l1"), n.metric("wake_cache_hits_total", "tier", "l2"),
			n.metric("wake_cache_loads_total"), n.metric("wake_cache_l2_errors_total"))
	}
	inv := inval.node
	d := fmt.Sprintf("Postgres -> Wake -> Kafka (%d partitions) -> cdcsource invalidator -> Redis + bus -> L1 of "+
		"%d instances (TTL %s, own Redis client each); %d keys; M7: the probe update was observed by every "+
		"instance %s after its commit, measurement started there; load %s (UPDATE ver = ver + 1, %d conns); "+
		"readers %d x %d GetOrLoad, at most one per %s each, for %s; reads %d, counted %d, caller errors %d, "+
		"wrong values %d; stale reads %d (window %s), max behind %s; invalidator: events %.0f, keys deleted "+
		"%.0f, delete failures %.0f deferred / %.0f error; %s; topic %s",
		wak12Partitions, len(nodes), wak12TTL, wak12Keys, run.probe.Round(time.Millisecond), run.res.Summary(),
		wak12Conns, len(nodes), wak12ReadersPer, wak12ReadEvery, run.ran.Round(time.Millisecond),
		run.reads.total(), ch.counted, run.reads.errors.Load(), run.reads.wrong.Load(), ch.stale, th.window,
		ch.maxBehind.Round(time.Millisecond), inv.metric("wake_cdcsource_events_total"),
		inv.metric("wake_cdcsource_keys_deleted_total"),
		inv.metric("wake_cdcsource_delete_failures_total", "kind", "deferred"),
		inv.metric("wake_cdcsource_delete_failures_total", "kind", "error"), strings.Join(per, "; "), topic)
	if ch.stale > 0 {
		d += "; worst stale read: " + ch.worst + "; first stale reads: " + strings.Join(ch.examples, " | ")
	}
	run.reads.mu.Lock()
	firstErrs := run.reads.firstErrs
	run.reads.mu.Unlock()
	if len(firstErrs) > 0 {
		d += "; first caller errors: " + strings.Join(firstErrs, " | ")
	}
	if len(run.res.FirstErrs) > 0 {
		d += "; load errors: " + strings.Join(run.res.FirstErrs, "; ")
	}
	if len(problems) > 0 {
		d += "; FAIL: " + strings.Join(problems, "; ")
	}
	return d
}
