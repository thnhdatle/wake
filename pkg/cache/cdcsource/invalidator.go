// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/wake/pkg/cache"
	"github.com/thnhdatle/wake/pkg/event"
)

// Deleter invalidates cache keys.
type Deleter interface {
	Delete(ctx context.Context, keys ...string) error
}

// Handler processes one batch of raw CDC event payloads in delivery order.
type Handler func(ctx context.Context, payloads [][]byte) error

// Source delivers CDC event payloads.
type Source interface {
	Consume(ctx context.Context, handle Handler) error
}

type healthChecker interface {
	Check(ctx context.Context) error
}

// Invalidator consumes CDC events from a Source and deletes the cache keys rendered from them.
type Invalidator struct {
	src     Source
	mapping *Mapping
	deleter Deleter
	opts    options
	m       *metrics
	running atomic.Bool

	rndMu sync.Mutex

	mu       sync.Mutex
	identity map[string][]string
}

// New validates its arguments and returns an invalidator.
func New(src Source, mapping *Mapping, deleter Deleter, opts ...Option) (*Invalidator, error) {
	switch {
	case src == nil:
		return nil, invalidConfig("nil source")
	case mapping == nil:
		return nil, invalidConfig("nil mapping")
	case deleter == nil:
		return nil, invalidConfig("nil deleter")
	}
	o := defaultOptions()
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	m, err := newMetrics(o.reg)
	if err != nil {
		return nil, err
	}
	o.log = o.log.With("component", "cdcsource")
	return &Invalidator{
		src: src, mapping: mapping, deleter: deleter, opts: o, m: m,
		identity: make(map[string][]string),
	}, nil
}

// Run consumes until ctx ends and returns nil then.
func (inv *Invalidator) Run(ctx context.Context) error {
	if !inv.running.CompareAndSwap(false, true) {
		return invalidConfig("Run called twice")
	}
	if err := inv.src.Consume(ctx, inv.handle); err != nil && ctx.Err() == nil {
		return fmt.Errorf("cdcsource: consume: %w", err)
	}
	return nil
}

// Ready reports whether the invalidator is healthy.
func (inv *Invalidator) Ready(ctx context.Context) error {
	var errs []error
	inv.mu.Lock()
	for _, table := range slices.Sorted(maps.Keys(inv.identity)) {
		errs = append(errs, &IdentityError{Table: table, Columns: slices.Clone(inv.identity[table])})
	}
	inv.mu.Unlock()
	if hc, ok := inv.src.(healthChecker); ok {
		if err := hc.Check(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (inv *Invalidator) handle(ctx context.Context, payloads [][]byte) error {
	var keys []string
	seen := make(map[string]struct{})
	for _, p := range payloads {
		inv.m.events.Inc()
		ks, reason := inv.keysOf(p)
		if reason != "" {
			inv.m.skipped[reason].Inc()
			continue
		}
		for _, k := range ks {
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	for chunk := range slices.Chunk(keys, maxKeysPerDelete) {
		if err := inv.deleteWithRetry(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (inv *Invalidator) keysOf(payload []byte) (keys []string, skip string) {
	if event.IsKafkaTombstone(payload) {
		return nil, reasonTombstone
	}
	e, err := event.Decode(payload)
	if err != nil {
		inv.opts.log.Error("skipping an invalid event", "error", err)
		return nil, reasonInvalid
	}
	switch e.Op {
	case event.OpTruncate:
		return nil, reasonTruncate
	case event.OpRead:
		if !inv.opts.onSnapshot {
			return nil, reasonSnapshot
		}
	case event.OpCreate, event.OpUpdate, event.OpDelete:
	default:
		return nil, reasonUnknownOp
	}
	if len(inv.mapping.tables[tableName(e)]) == 0 {
		return nil, reasonUnmappedTable
	}
	if e.Op == event.OpUpdate || e.Op == event.OpDelete {
		inv.checkIdentity(e)
	}
	keys = inv.mapping.Keys(e)
	if len(keys) == 0 {
		return nil, reasonNoKeys
	}
	return keys, ""
}

func (inv *Invalidator) checkIdentity(e *event.Event) {
	missing := inv.mapping.missingFromBefore(e)
	if len(missing) == 0 {
		return
	}
	table := tableName(e)
	inv.m.fullIdentity.WithLabelValues(table).Inc()
	inv.mu.Lock()
	cols, seen := inv.identity[table]
	for _, c := range missing {
		if !slices.Contains(cols, c) {
			cols = append(cols, c)
		}
	}
	slices.Sort(cols)
	inv.identity[table] = cols
	inv.mu.Unlock()
	if !seen {
		inv.opts.log.Error("old cache keys of updates and deletes cannot be invalidated",
			"table", table, "error", &IdentityError{Table: table, Columns: missing})
	}
}

func (inv *Invalidator) deleteWithRetry(ctx context.Context, keys []string) error {
	for attempt := 0; ; attempt++ {
		err := inv.deleteOnce(ctx, keys)
		if err == nil {
			inv.m.keysDeleted.Add(float64(len(keys)))
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("cdcsource: delete %d keys: %w", len(keys), ctx.Err())
		}
		if errors.Is(err, cache.ErrDeferred) {
			inv.m.deferred.Inc()
		} else {
			inv.m.failed.Inc()
		}
		delay := inv.backoff(attempt)
		inv.opts.log.Warn("cache delete failed; retrying", "error", err, "attempt", attempt+1,
			"count", len(keys), "duration_ms", delay.Milliseconds())
		select {
		case <-ctx.Done():
			return fmt.Errorf("cdcsource: delete %d keys: %w", len(keys), ctx.Err())
		case <-inv.opts.clock.After(delay):
		}
	}
}

func (inv *Invalidator) deleteOnce(ctx context.Context, keys []string) error {
	ctx, cancel := context.WithTimeout(ctx, inv.opts.deleteTimeout)
	defer cancel()
	return inv.deleter.Delete(ctx, keys...)
}

func (inv *Invalidator) backoff(attempt int) time.Duration {
	d := inv.opts.retryMax
	if attempt < 32 {
		if exp := inv.opts.retryMin << attempt; exp > 0 && exp < d {
			d = exp
		}
	}
	half := d / 2
	inv.rndMu.Lock()
	defer inv.rndMu.Unlock()
	return half + time.Duration(inv.opts.rnd.Int64N(int64(d-half)+1))
}
