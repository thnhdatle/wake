// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	reasonTombstone     = "tombstone"
	reasonInvalid       = "invalid"
	reasonUnknownOp     = "unknown_op"
	reasonTruncate      = "truncate"
	reasonSnapshot      = "snapshot"
	reasonUnmappedTable = "unmapped_table"
	reasonNoKeys        = "no_keys"
)

var skipReasons = []string{
	reasonTombstone, reasonInvalid, reasonUnknownOp, reasonTruncate,
	reasonSnapshot, reasonUnmappedTable, reasonNoKeys,
}

const (
	failureDeferred = "deferred"
	failureError    = "error"
)

type metrics struct {
	events       prometheus.Counter
	skipped      map[string]prometheus.Counter
	keysDeleted  prometheus.Counter
	deferred     prometheus.Counter
	failed       prometheus.Counter
	fullIdentity *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	events, err1 := register(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wake_cdcsource_events_total",
		Help: "Messages received from the source, including skipped ones.",
	}))
	skipped, err2 := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wake_cdcsource_events_skipped_total",
		Help: "Messages that caused no invalidation, by reason.",
	}, []string{"reason"}))
	deleted, err3 := register(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wake_cdcsource_keys_deleted_total",
		Help: "Cache keys whose Delete returned nil.",
	}))
	failures, err4 := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wake_cdcsource_delete_failures_total",
		Help: "Failed Delete calls (retried), by kind: deferred (cache.ErrDeferred) or error.",
	}, []string{"kind"}))
	identity, err5 := register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wake_cdcsource_template_requires_full_identity_total",
		Help: "Update and delete events whose before image lacks a non-key column referenced by a template.",
	}, []string{"table"}))
	if err := errors.Join(err1, err2, err3, err4, err5); err != nil {
		return nil, err
	}
	m := &metrics{
		events:       events,
		skipped:      make(map[string]prometheus.Counter, len(skipReasons)),
		keysDeleted:  deleted,
		deferred:     failures.WithLabelValues(failureDeferred),
		failed:       failures.WithLabelValues(failureError),
		fullIdentity: identity,
	}
	for _, r := range skipReasons {
		m.skipped[r] = skipped.WithLabelValues(r)
	}
	return m, nil
}

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
	return zero, fmt.Errorf("%w: register metrics: %w", ErrInvalidConfig, err)
}
