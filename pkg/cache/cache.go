// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	mrand "math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/l1"
)

// Cache is a two-tier read-through cache of values of type V. Values
// returned by Get and GetOrLoad are shared with other callers: treat V as
// immutable.
type Cache[V any] interface {
	// Get returns the cached value of key. It reports a miss (not an error)
	// when the key is absent, negatively cached or L2 is unreachable.
	Get(ctx context.Context, key string) (V, bool, error)
	// GetOrLoad returns the cached value of key or loads it with load,
	// coordinating concurrent loads in this process and across instances.
	GetOrLoad(ctx context.Context, key string, load LoadFunc[V]) (V, error)
	// Set stores v under key, replacing the value for every instance.
	Set(ctx context.Context, key string, v V, opts ...SetOption) error
	// Delete invalidates keys in L2 and in the L1 of every instance.
	Delete(ctx context.Context, keys ...string) error
	// Close stops background goroutines and releases connections.
	Close() error
}

// LoadFunc loads the current value from the primary database (never a replica).
type LoadFunc[V any] func(ctx context.Context) (V, error)

// item is what L1 stores: a value or a cached absence.
type item[V any] struct {
	v   V
	neg bool
}

// cache implements Cache. Without an L2 backend it is an L1-only cache and
// starts no goroutines.
type cache[V any] struct {
	ns      string
	cfg     config
	codec   Codec[V]
	l1      *l1.Cache[item[V]]
	env     *envelope
	backend l2.Backend // nil: L1 only
	m       *metrics
	group   singleflight.Group
	signals loadSignals
	pending *pendingSet
	breaker breaker

	epoch   atomic.Uint64
	epochOK atomic.Bool
	epochMu sync.Mutex // serializes epoch adoption with its L1 flush
	busUp   atomic.Bool
	closed  atomic.Bool

	instance string
	lockSeq  atomic.Uint64
	rngMu    sync.Mutex // protects rng
	rng      *mrand.Rand

	wake      chan struct{} // capacity 1: wakes the maintenance goroutine
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// New returns a cache for namespace ([A-Za-z0-9_-]+) whose L2 payloads are
// encoded with codec. With an L2 configured it starts the invalidation-bus
// subscriber and the L2 maintenance goroutine; Close stops both.
func New[V any](namespace string, codec Codec[V], opts ...Option) (Cache[V], error) {
	if !validNamespace(namespace) {
		return nil, invalid("namespace %q must match [A-Za-z0-9_-]+", namespace)
	}
	if codec == nil {
		return nil, invalid("nil codec")
	}
	cfg := defaultConfig(namespace)
	for _, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.jitter >= cfg.ttl {
		return nil, invalid("jitter %s must be below ttl %s", cfg.jitter, cfg.ttl)
	}
	m, err := newMetrics(cfg.registerer, namespace)
	if err != nil {
		return nil, err
	}
	l1c, err := l1.New[item[V]](cfg.l1Entries, cfg.l1Bytes, l1.WithClock(cfg.clock))
	if err != nil {
		return nil, fmt.Errorf("cache: %w", err)
	}
	env, err := newEnvelope(cfg.compressMin)
	if err != nil {
		return nil, fmt.Errorf("cache: %w", err)
	}
	var seed [16]byte
	_, _ = rand.Read(seed[:]) // crypto/rand.Read never fails (Go >= 1.24)
	c := &cache[V]{
		ns: namespace, cfg: cfg, codec: codec, l1: l1c, env: env, m: m,
		pending:  newPendingSet(cfg.maxPending),
		instance: hex.EncodeToString(seed[:8]),
		rng: mrand.New(mrand.NewPCG( //nolint:gosec // G404: non-crypto TTL and backoff jitter
			binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:]))),
		wake: make(chan struct{}, 1),
	}
	if cfg.backend != nil {
		if err := c.start(namespace); err != nil {
			env.close()
			return nil, err
		}
	}
	return c, nil
}

// start connects the L2 backend and starts the two owned goroutines.
func (c *cache[V]) start(namespace string) error {
	b, err := c.cfg.backend(l2.Config{
		Namespace: namespace, Channel: c.cfg.channel, BusHealthCheck: c.cfg.degradedTTL / 2,
	})
	if err != nil {
		return fmt.Errorf("cache: l2: %w", err)
	}
	c.backend = b
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is called by Close
	c.cancel = cancel
	c.wg.Add(2)
	go c.runSubscriber(ctx)
	go c.runMaintenance(ctx)
	c.wakeMaintenance()
	return nil
}

func validNamespace(ns string) bool {
	if ns == "" {
		return false
	}
	for _, r := range ns {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if !ok {
			return false
		}
	}
	return true
}

// Close stops the subscriber and maintenance goroutines, waits for them and
// closes the L2 backend. It is idempotent; later calls return errClosed
// from every other method.
func (c *cache[V]) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.cancel != nil {
			c.cancel()
			c.wg.Wait()
		}
		if c.backend != nil {
			if err := c.backend.Close(); err != nil {
				c.closeErr = fmt.Errorf("cache: close l2: %w", err)
			}
		}
		c.env.close()
	})
	return c.closeErr
}

// degraded reports whether invalidations may be missed (L2 breaker not
// closed or bus down); L1 ages are then capped to the degraded TTL.
func (c *cache[V]) degraded() bool {
	return c.backend != nil && (!c.busUp.Load() || !c.breaker.allow())
}

func (c *cache[V]) capTTL(ttl time.Duration) time.Duration {
	if c.degraded() {
		return min(ttl, c.cfg.degradedTTL)
	}
	return ttl
}

func (c *cache[V]) genTTL(ttl time.Duration) time.Duration {
	return max(ttl, c.cfg.ttl, c.cfg.negTTL) + c.cfg.lockTimeout + genGrace
}

// valueTTL returns the configured TTL minus a random jitter.
func (c *cache[V]) valueTTL() time.Duration {
	if c.cfg.jitter <= 0 {
		return c.cfg.ttl
	}
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.cfg.ttl - time.Duration(c.rng.Int64N(int64(c.cfg.jitter)))
}

// spread returns d plus up to 50% random jitter (poll backoff).
func (c *cache[V]) spread(d time.Duration) time.Duration {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return d + time.Duration(c.rng.Int64N(int64(d)/2+1))
}

func (c *cache[V]) newLockToken() string {
	return c.instance + "-" + strconv.FormatUint(c.lockSeq.Add(1), 10)
}

// adoptEpoch switches to epoch e. The new epoch is stored before L1 is
// flushed: a reader that loaded the old epoch captured its L1 token before
// the flush, so its insert is refused.
func (c *cache[V]) adoptEpoch(e uint64) {
	c.epochMu.Lock()
	defer c.epochMu.Unlock()
	known := c.epochOK.Load()
	if known && c.epoch.Load() == e {
		return
	}
	if known {
		c.m.epochChanges.Inc()
	}
	c.epoch.Store(e)
	c.epochOK.Store(true)
	c.l1.Flush()
}

// ensureEpoch makes sure the namespace epoch is known, reading it if not.
func (c *cache[V]) ensureEpoch(ctx context.Context) bool {
	if c.epochOK.Load() {
		return true
	}
	e, err := c.backend.Epoch(ctx)
	if err != nil {
		c.l2Failed(ctx, err)
		return false
	}
	c.l2OK()
	c.adoptEpoch(e)
	return true
}

// l2Readable reports whether this call may read L2 for key.
func (c *cache[V]) l2Readable(ctx context.Context, key string) bool {
	return c.l2Writable(ctx) && !c.pending.blocks(key)
}

// l2Writable reports whether this call may write L2.
func (c *cache[V]) l2Writable(ctx context.Context) bool {
	return c.backend != nil && c.breaker.allow() && c.ensureEpoch(ctx)
}

func (c *cache[V]) l2OK() { c.breaker.success() }

// l2Failed counts an L2 failure unless the caller's own context ended.
func (c *cache[V]) l2Failed(ctx context.Context, _ error) {
	if ctx.Err() != nil {
		return
	}
	c.m.l2Errors.Inc()
	if c.breaker.failure() {
		c.m.breakerState.Set(float64(breakerOpen))
		c.wakeMaintenance()
	}
}

func (c *cache[V]) wakeMaintenance() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *cache[V]) encodeItem(it item[V]) ([]byte, error) {
	if it.neg {
		return c.env.seal(nil, true), nil
	}
	raw, err := c.codec.Marshal(it.v)
	if err != nil {
		return nil, err
	}
	return c.env.seal(raw, false), nil
}

func (c *cache[V]) decodeItem(payload []byte) (item[V], error) {
	raw, neg, err := c.env.open(payload)
	if err != nil || neg {
		return item[V]{neg: neg}, err
	}
	v, err := c.codec.Unmarshal(raw)
	if err != nil {
		return item[V]{}, err
	}
	return item[V]{v: v}, nil
}

func (c *cache[V]) clock() Clock { return c.cfg.clock }
