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

// Deleter invalidates cache keys. cache.Cache[V] satisfies it. Delete must
// return nil only once every key is invalid for every cache instance; any
// error (cache.ErrDeferred included) makes the invalidator retry.
type Deleter interface {
	Delete(ctx context.Context, keys ...string) error
}

// Handler processes one batch of raw CDC event payloads in delivery order. A nil or
// empty payload is a Kafka tombstone. It returns nil once every payload has
// been handled; it retries failures itself and returns an error only when
// ctx ends, in which case the batch must not be acknowledged.
type Handler func(ctx context.Context, payloads [][]byte) error

// Source delivers CDC event payloads. Consume calls handle for each batch,
// one batch at a time, and acknowledges a batch (Kafka offset commit, NATS
// ack) only after handle returned nil. It returns when ctx ends (nil) or
// when handle or the transport fails for good. KafkaSource and NATSSource
// implement it. A Source that also has a method Check(ctx) error reports its
// health through Invalidator.Ready.
type Source interface {
	Consume(ctx context.Context, handle Handler) error
}

// healthChecker is the optional health method of a Source.
type healthChecker interface {
	Check(ctx context.Context) error
}

// Invalidator consumes CDC events from a Source and deletes the cache keys
// rendered from them.
type Invalidator struct {
	src     Source
	mapping *Mapping
	deleter Deleter
	opts    options
	m       *metrics
	running atomic.Bool

	// rndMu guards opts.rnd, which is not safe for concurrent use.
	rndMu sync.Mutex

	mu sync.Mutex
	// identity holds, per table, the non-key columns found missing from a
	// before image (template_requires_full_identity); guarded by mu.
	identity map[string][]string
}

// New validates its arguments and returns an invalidator. It starts nothing;
// call Run.
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

// Run consumes until ctx ends and returns nil then. It returns an error when
// the source fails for good. Run may be called only once.
func (inv *Invalidator) Run(ctx context.Context) error {
	if !inv.running.CompareAndSwap(false, true) {
		return invalidConfig("Run called twice")
	}
	// An error after ctx ended is the shutdown itself, not a failure.
	if err := inv.src.Consume(ctx, inv.handle); err != nil && ctx.Err() == nil {
		return fmt.Errorf("cdcsource: consume: %w", err)
	}
	return nil
}

// Ready reports nil when no template_requires_full_identity condition was
// raised and the source (when it has a Check method) is healthy. Otherwise it
// returns the joined reasons; each identity problem is an *IdentityError.
// The identity condition stays raised until the process restarts.
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

// handle is the Handler given to the source.
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

// keysOf decodes one payload and returns its keys, or the reason to skip it.
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

// checkIdentity raises template_requires_full_identity when the before image
// of an update or delete lacks a referenced non-key column: it counts every
// such event and logs once per table. Processing continues with the images
// available.
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

// deleteWithRetry calls Delete until it returns nil, waiting a capped,
// jittered, exponentially growing delay between attempts. It returns an
// error only when ctx ends.
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

// backoff returns the delay before retry attempt+1: uniform in [d/2, d]
// with d = retryMin * 2^attempt capped at retryMax.
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
