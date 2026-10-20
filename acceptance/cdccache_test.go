// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/cache"
	"github.com/thnhdatle/wake/pkg/cache/cdcsource"
)

// Shared wiring of the CDC invalidation scenarios (WAK-10, WAK-12):
// Postgres -> Wake -> Kafka -> cdcsource invalidator -> Redis + bus -> L1 of
// several pkg/cache instances, over a table (id bigint PRIMARY KEY, ver
// bigint NOT NULL, payload text) cached as item:{id}.
const (
	cdcWait      = 2 * time.Minute // bound of each readiness wait
	cdcProbePoll = 10 * time.Millisecond
)

// cdcScenario names the cached table of a scenario.
type cdcScenario struct {
	id    string // criterion id
	table string // "<schema>.<table>"
	// keys are the ids 1..keys of the writers and readers; the M7 probe row
	// is keys+1, outside their key space.
	keys int64
}

// probeID is the id of the M7 probe row.
func (s cdcScenario) probeID() int64 { return s.keys + 1 }

// schema is the table's schema.
func (s cdcScenario) schema() string {
	schema, _, _ := strings.Cut(s.table, ".")
	return schema
}

// cdcValue is the cached value: the row's id and the ver the loader read.
type cdcValue struct {
	ID  int64 `json:"id"`
	Ver int64 `json:"ver"`
}

func cdcKey(id int64) string { return "item:" + strconv.FormatInt(id, 10) }

// cdcLoader is one instance's loader: it reads the row from the Postgres
// primary over the instance's own connections.
type cdcLoader struct {
	pg    *stack.PG
	query string
	calls atomic.Int64
}

// newLoaders returns n loaders of the scenario's table with conns
// connections each.
func (s cdcScenario) newLoaders(tb testing.TB, n, conns int) []*cdcLoader {
	tb.Helper()
	loaders := make([]*cdcLoader, n)
	for i := range loaders {
		loaders[i] = &cdcLoader{pg: stack.ConnectPG(tb, conns), query: "SELECT ver FROM " + s.table + " WHERE id = $1"}
	}
	return loaders
}

func (l *cdcLoader) load(id int64) cache.LoadFunc[cdcValue] {
	return func(ctx context.Context) (cdcValue, error) {
		l.calls.Add(1)
		v := cdcValue{ID: id}
		err := l.pg.QueryRow(ctx, l.query, []any{id}, &v.Ver)
		return v, err //nolint:wrapcheck // stack errors carry the query
	}
}

// cdcInvalidator is the cache-invalidator process of a scenario, wired as
// `wake cache-invalidator` runs it: a Kafka source on the Wake topics, the
// mapping item:{id} and its own cache instance (own Redis client) as the
// deleter.
type cdcInvalidator struct {
	inv *cdcsource.Invalidator
	// node is the deleter; its registry also holds the wake_cdcsource_*
	// metrics.
	node cacheNode[cdcValue]
	done chan struct{}
	err  error // the result of Run, set before done is closed
}

// startInvalidator starts the invalidator with node as the deleter on a
// consumer group unique to the run and waits until it reports ready.
func (s cdcScenario) startInvalidator(ctx context.Context, tb testing.TB, prefix string, node cacheNode[cdcValue],
) *cdcInvalidator {
	tb.Helper()
	m, err := cdcsource.NewMapping(map[string][]string{s.table: {"item:{id}"}})
	failIf(tb, s.id, err, "cdcsource.NewMapping")
	src, err := cdcsource.NewKafkaSource(cdcsource.KafkaConfig{
		Brokers: []string{stack.KafkaBroker}, TopicPrefix: prefix, Group: uniqueNamespace(s.schema() + "-inval"),
	}, m)
	failIf(tb, s.id, err, "cdcsource.NewKafkaSource")
	inv, err := cdcsource.New(src, m, node.c, cdcsource.WithMetrics(node.reg),
		cdcsource.WithRetry(50*time.Millisecond, 500*time.Millisecond))
	failIf(tb, s.id, err, "cdcsource.New")

	w := &cdcInvalidator{inv: inv, node: node, done: make(chan struct{})}
	rctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(w.done)
		w.err = inv.Run(rctx)
	}()
	tb.Cleanup(func() { // runs before the deleter closes
		cancel()
		<-w.done
	})
	wctx, wcancel := context.WithTimeout(ctx, cdcWait)
	defer wcancel()
	if err = waitUntil(wctx, 100*time.Millisecond, func() bool { return inv.Ready(wctx) == nil }); err != nil {
		_, runErr := w.exited()
		harness.Failf(tb, s.id, "wait for the invalidator to be ready: %v (ready: %v, run: %v)", err,
			inv.Ready(ctx), runErr)
	}
	return w
}

// newDeleter is the invalidator's own cache instance of namespace ns, with
// its own Redis client and registry and the given TTL, closed at cleanup.
func (s cdcScenario) newDeleter(tb testing.TB, ns string, ttl time.Duration) cacheNode[cdcValue] {
	tb.Helper()
	reg := prometheus.NewRegistry()
	del, err := cache.New[cdcValue](ns, cache.JSONCodec[cdcValue](), cache.WithL2(stack.NewRedisClient(tb)),
		cache.WithMetrics(reg), cache.WithTTL(ttl, 0))
	failIf(tb, s.id, err, "invalidator cache.New")
	tb.Cleanup(func() { _ = del.Close() })
	return cacheNode[cdcValue]{c: del, reg: reg}
}

// exited reports whether Run returned, and its error.
func (w *cdcInvalidator) exited() (bool, error) {
	select {
	case <-w.done:
		return true, w.err
	default:
		return false, nil
	}
}

// warm loads every key and the probe row into every instance (the first
// one loads from Postgres and fills L2, the others read L2).
func (s cdcScenario) warm(ctx context.Context, tb testing.TB, nodes []cacheNode[cdcValue], loaders []*cdcLoader) {
	tb.Helper()
	for k := int64(1); k <= s.probeID(); k++ {
		for i, n := range nodes {
			v, err := n.c.GetOrLoad(ctx, cdcKey(k), loaders[i].load(k))
			failIf(tb, s.id, err, "warm "+cdcKey(k))
			if v.ID != k || v.Ver < 1 {
				harness.Failf(tb, s.id, "warm %s on instance %d: got %+v", cdcKey(k), i, v)
			}
		}
	}
}

// waitProbe is the M7 warm-up: it commits an update of the probe row and
// waits until every instance received an invalidation from the bus and no
// longer serves the probe row (L1 and L2 dropped it). The whole path
// (Postgres, Wake, Kafka, invalidator, Redis, bus) is then live for every
// instance. It returns the time from the probe commit to that point.
func (s cdcScenario) waitProbe(ctx context.Context, tb testing.TB, pg *stack.PG, nodes []cacheNode[cdcValue],
) time.Duration {
	tb.Helper()
	before := make([]float64, len(nodes))
	for i, n := range nodes {
		before[i] = n.metric("wake_cache_invalidations_received_total")
	}
	_, err := pg.ExecContext(ctx, "UPDATE "+s.table+" SET ver = ver + 1 WHERE id = $1", s.probeID())
	failIf(tb, s.id, err, "update the probe row")
	committed := time.Now()
	probe := cdcKey(s.probeID())
	wctx, cancel := context.WithTimeout(ctx, cdcWait)
	defer cancel()
	err = waitUntil(wctx, cdcProbePoll, func() bool {
		for i, n := range nodes {
			if n.metric("wake_cache_invalidations_received_total") <= before[i] {
				return false
			}
			if _, ok, _ := n.c.Get(wctx, probe); ok {
				return false
			}
		}
		return true
	})
	failIf(tb, s.id, err, "M7: wait for every instance to observe the probe invalidation")
	return time.Since(committed)
}
