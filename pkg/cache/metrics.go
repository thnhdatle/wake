// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Breaker states exported by wake_cache_breaker_state.
const (
	breakerClosed int32 = iota
	breakerOpen
	breakerHalfOpen
)

// metrics holds counters pre-bound to the namespace label, so recording a
// hit does not allocate.
type metrics struct {
	l1Hits, l1Misses, l2Hits, l2Misses prometheus.Counter
	loads, lockWaits, fallbackLoads    prometheus.Counter
	staleServes, l2Errors              prometheus.Counter
	invalidationsReceived, deferred    prometheus.Counter
	pendingOverflows, epochChanges     prometheus.Counter
	breakerState, pendingDepth, busUp  prometheus.Gauge
}

func counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
}

func gaugeVec(name, help string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, []string{"namespace"})
}

// register registers c on reg, reusing an identical collector registered by
// another cache. A nil reg leaves c unregistered (metrics disabled).
func register[C prometheus.Collector](reg prometheus.Registerer, c C) (C, error) {
	if reg == nil {
		return c, nil
	}
	err := reg.Register(c)
	if err == nil {
		return c, nil
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(C); ok {
			return existing, nil
		}
	}
	var zero C
	return zero, fmt.Errorf("register metrics: %w", err)
}

func newMetrics(reg prometheus.Registerer, ns string) (*metrics, error) {
	hits, err1 := register(reg, counterVec("wake_cache_hits_total", "Cache hits by tier.", "namespace", "tier"))
	misses, err2 := register(reg, counterVec("wake_cache_misses_total", "Cache misses by tier.", "namespace", "tier"))
	counters, err3 := registerCounters(reg)
	gauges, err4 := registerGauges(reg)
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		return nil, err
	}
	c := func(name string) prometheus.Counter { return counters[name].WithLabelValues(ns) }
	g := func(name string) prometheus.Gauge { return gauges[name].WithLabelValues(ns) }
	return &metrics{
		l1Hits:                hits.WithLabelValues(ns, "l1"),
		l1Misses:              misses.WithLabelValues(ns, "l1"),
		l2Hits:                hits.WithLabelValues(ns, "l2"),
		l2Misses:              misses.WithLabelValues(ns, "l2"),
		loads:                 c("loads"),
		lockWaits:             c("lock_waits"),
		fallbackLoads:         c("fallback_loads"),
		staleServes:           c("stale_serves"),
		l2Errors:              c("l2_errors"),
		invalidationsReceived: c("invalidations_received"),
		deferred:              c("deferred_invalidations"),
		pendingOverflows:      c("pending_overflows"),
		epochChanges:          c("epoch_changes"),
		breakerState:          g("breaker_state"),
		pendingDepth:          g("pending_invalidations"),
		busUp:                 g("bus_connected"),
	}, nil
}

var counterHelp = [...][2]string{
	{"loads", "Loader calls."},
	{"lock_waits", "GetOrLoad calls that waited for another instance's load lock."},
	{"fallback_loads", "Loads by waiters after the lock timeout."},
	{"stale_serves", "Expired L1 values served because the loader failed."},
	{"l2_errors", "Failed L2 operations."},
	{"invalidations_received", "Invalidation messages received from the bus."},
	{"deferred_invalidations", "Invalidations deferred while L2 was unreachable."},
	{"pending_overflows", "Deferred invalidations beyond the bound (epoch increment on recovery)."},
	{"epoch_changes", "Namespace epoch changes observed."},
}

func registerCounters(reg prometheus.Registerer) (map[string]*prometheus.CounterVec, error) {
	out := make(map[string]*prometheus.CounterVec, len(counterHelp))
	for _, h := range counterHelp {
		v, err := register(reg, counterVec("wake_cache_"+h[0]+"_total", h[1], "namespace"))
		if err != nil {
			return nil, err
		}
		out[h[0]] = v
	}
	return out, nil
}

func registerGauges(reg prometheus.Registerer) (map[string]*prometheus.GaugeVec, error) {
	help := [...][2]string{
		{"breaker_state", "L2 circuit breaker state: 0 closed, 1 open, 2 half-open."},
		{"pending_invalidations", "Keys whose L2 invalidation is deferred."},
		{"bus_connected", "1 while the invalidation bus subscription is up."},
	}
	out := make(map[string]*prometheus.GaugeVec, len(help))
	for _, h := range help {
		v, err := register(reg, gaugeVec("wake_cache_"+h[0], h[1]))
		if err != nil {
			return nil, err
		}
		out[h[0]] = v
	}
	return out, nil
}
