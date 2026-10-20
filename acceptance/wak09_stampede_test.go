// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/cache"
)

// WAK-09 scenario parameters. The criterion fixes the instances, the
// concurrent calls and the loader latency; the rounds (each one a fresh
// stampede on its own key, all of which must pass), the warm TTL and the
// waits are chosen by the test.
const (
	wak09Rounds  = 5
	wak09TTL     = 2 * time.Second
	wak09Wait    = 30 * time.Second
	wak09Timeout = 5 * time.Minute
)

// cacheNode is one pkg/cache instance of a test with its own Redis client
// and its own metrics registry, as a separate process would have.
type cacheNode[V any] struct {
	c   cache.Cache[V]
	reg *prometheus.Registry
}

// newCacheNodes starts n instances of namespace ns on the compose Redis and
// waits until each one reports its invalidation bus connected and its
// breaker closed.
func newCacheNodes[V any](tb testing.TB, id, ns string, n int, opts ...cache.Option) []cacheNode[V] {
	tb.Helper()
	nodes := make([]cacheNode[V], n)
	for i := range nodes {
		reg := prometheus.NewRegistry()
		all := append([]cache.Option{cache.WithL2(stack.NewRedisClient(tb)), cache.WithMetrics(reg)}, opts...)
		c, err := cache.New[V](ns, cache.JSONCodec[V](), all...)
		failIf(tb, id, err, "cache.New")
		tb.Cleanup(func() { _ = c.Close() })
		nodes[i] = cacheNode[V]{c: c, reg: reg}
	}
	ctx, cancel := context.WithTimeout(context.Background(), wak09Wait)
	defer cancel()
	err := waitUntil(ctx, 10*time.Millisecond, func() bool {
		for _, n := range nodes {
			if n.metric("wake_cache_bus_connected") != 1 || n.metric("wake_cache_breaker_state") != 0 {
				return false
			}
		}
		return true
	})
	failIf(tb, id, err, "wait for every instance's bus and breaker")
	return nodes
}

// metric returns the value of the cache metric name (a counter or gauge of
// this instance's namespace) whose labels include the given name=value
// pairs; 0 when absent.
func (n cacheNode[V]) metric(name string, labels ...string) float64 {
	mfs, err := n.reg.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if !hasLabels(m.GetLabel(), labels) {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

type labelPair interface {
	GetName() string
	GetValue() string
}

func hasLabels[L labelPair](have []L, want []string) bool {
	for i := 0; i+1 < len(want); i += 2 {
		found := false
		for _, l := range have {
			if l.GetName() == want[i] && l.GetValue() == want[i+1] {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// sumMetric adds metric over the instances.
func sumMetric[V any](nodes []cacheNode[V], name string, labels ...string) float64 {
	var s float64
	for _, n := range nodes {
		s += n.metric(name, labels...)
	}
	return s
}

// uniqueNamespace returns prefix-<random hex>, so runs never share keys.
func uniqueNamespace(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails (Go >= 1.24)
	return prefix + "-" + hex.EncodeToString(b[:])
}

// waitUntil polls cond every interval until it holds or ctx ends.
func waitUntil(ctx context.Context, interval time.Duration, cond func() bool) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for !cond() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

// wak09Round is the outcome of one stampede.
type wak09Round struct {
	key           string
	loaderCalls   int64
	errors        int
	wrongValues   int
	lockWaits     float64
	fallbackLoads float64
	elapsed       time.Duration
	firstErr      string
}

// TestAcceptance_WAK09_Stampede implements WAK-09: Stampede: 1,000 concurrent GetOrLoad on a just-expired key
// across 3 instances.
func TestAcceptance_WAK09_Stampede(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-09"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	instances, err := c.Int("instances")
	failIf(t, id, err, "threshold instances")
	calls, err := c.Int("concurrent_calls")
	failIf(t, id, err, "threshold concurrent_calls")
	loaderDelay, err := c.Duration("loader_ms")
	failIf(t, id, err, "threshold loader_ms")
	loaderCallsMax, err := c.Int("loader_calls_max")
	failIf(t, id, err, "threshold loader_calls_max")

	stack.EnsureUp(t)
	ctx := withTimeout(t, wak09Timeout)
	failIf(t, id, stack.PingRedis(ctx), "compose redis")
	ns := uniqueNamespace("wak09")
	// Default lock_timeout (5 s) is far above the 50 ms loader.
	nodes := newCacheNodes[string](t, id, ns, int(instances), cache.WithTTL(wak09TTL, 0))

	rounds := make([]wak09Round, 0, wak09Rounds)
	for r := range wak09Rounds {
		key := "hot:" + strconv.Itoa(r)
		warmJustExpired(ctx, t, id, nodes, key)
		rounds = append(rounds, stampede(ctx, nodes, key, int(calls), loaderDelay))
	}

	var maxCalls int64
	errs, wrong := 0, 0
	parts := make([]string, 0, len(rounds))
	for _, r := range rounds {
		maxCalls = max(maxCalls, r.loaderCalls)
		errs += r.errors
		wrong += r.wrongValues
		parts = append(parts, fmt.Sprintf("%s: loader calls %d, errors %d, wrong values %d, lock waits %.0f, "+
			"fallback loads %.0f, %s%s", r.key, r.loaderCalls, r.errors, r.wrongValues, r.lockWaits,
			r.fallbackLoads, r.elapsed.Round(time.Millisecond), r.firstErr))
	}
	minCalls := rounds[0].loaderCalls
	for _, r := range rounds {
		minCalls = min(minCalls, r.loaderCalls)
	}
	pass := maxCalls <= loaderCallsMax && minCalls >= 1 && errs == 0 && wrong == 0
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"loader_calls": maxCalls, "loader_calls_min": minCalls, "rounds": len(rounds),
			"instances": instances, "concurrent_calls": calls, "loader_ms": loaderDelay.Milliseconds(),
			"caller_errors": errs, "wrong_values": wrong,
		},
		Pass: pass,
		Details: fmt.Sprintf("%d rounds of %d concurrent GetOrLoad over %d instances (own Redis client each, "+
			"compose Redis), loader %s, lock_timeout 5 s; loader_calls is the maximum over the rounds; %s",
			len(rounds), calls, instances, loaderDelay, strings.Join(parts, "; ")),
	})
}

// warmJustExpired loads key through the first instance (value and L2
// TTL wak09TTL), reads it into every other instance's L1 from L2, then waits
// until it is expired everywhere: Get (L1, then L2, never the loader)
// reports a miss on every instance. The stampede starts right after.
func warmJustExpired(ctx context.Context, t *testing.T, id string, nodes []cacheNode[string], key string) {
	t.Helper()
	v, err := nodes[0].c.GetOrLoad(ctx, key, func(context.Context) (string, error) { return "warm", nil })
	failIf(t, id, err, "warm "+key)
	if v != "warm" {
		harness.Failf(t, id, "warm %s: got %q", key, v)
	}
	for i, n := range nodes {
		got, ok, gerr := n.c.Get(ctx, key)
		failIf(t, id, gerr, "read warm value")
		if !ok || got != "warm" {
			harness.Failf(t, id, "instance %d: warm value of %s not served from L2: %q %v", i, key, got, ok)
		}
	}
	err = waitUntil(ctx, 5*time.Millisecond, func() bool {
		for _, n := range nodes {
			if _, ok, _ := n.c.Get(ctx, key); ok {
				return false
			}
		}
		return true
	})
	failIf(t, id, err, "wait for "+key+" to expire in L1 and L2")
}

// stampede releases calls goroutines at once, spread round-robin over the
// instances, each calling GetOrLoad(key) with a loader that takes delay.
func stampede(ctx context.Context, nodes []cacheNode[string], key string, calls int, delay time.Duration,
) wak09Round {
	waitsBefore := sumMetric(nodes, "wake_cache_lock_waits_total")
	fallbackBefore := sumMetric(nodes, "wake_cache_fallback_loads_total")
	var loaderCalls atomic.Int64
	want := "loaded:" + key
	load := cache.LoadFunc[string](func(ctx context.Context) (string, error) {
		loaderCalls.Add(1)
		select { // the loader's own work, below lock_timeout
		case <-time.After(delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return want, nil
	})
	gate := make(chan struct{})
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		errs     int
		wrong    int
		firstErr string
	)
	for i := range calls {
		n := nodes[i%len(nodes)]
		wg.Go(func() {
			<-gate
			v, err := n.c.GetOrLoad(ctx, key, load)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				errs++
				if firstErr == "" {
					firstErr = "; first error: " + err.Error()
				}
			case v != want:
				wrong++
			}
		})
	}
	start := time.Now()
	close(gate)
	wg.Wait()
	return wak09Round{
		key: key, loaderCalls: loaderCalls.Load(), errors: errs, wrongValues: wrong, firstErr: firstErr,
		lockWaits:     sumMetric(nodes, "wake_cache_lock_waits_total") - waitsBefore,
		fallbackLoads: sumMetric(nodes, "wake_cache_fallback_loads_total") - fallbackBefore,
		elapsed:       time.Since(start),
	}
}
