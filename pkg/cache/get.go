// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/l1"
)

// maxSharedRetries bounds how often a GetOrLoad caller retries after the
// shared (singleflight) load failed only because its leader's context ended.
const maxSharedRetries = 2

// l1Get reads L1, capping entry ages while degraded.
func (c *cache[V]) l1Get(key string) (item[V], bool) {
	if c.degraded() {
		return c.l1.GetMaxAge(key, c.cfg.degradedTTL)
	}
	return c.l1.Get(key)
}

// Get implements Cache.
func (c *cache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	if c.closed.Load() {
		return zero, false, errClosed
	}
	if it, ok := c.l1Get(key); ok {
		c.m.l1Hits.Inc()
		if it.neg {
			return zero, false, nil
		}
		return it.v, true, nil
	}
	c.m.l1Misses.Inc()
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	it, ok, err := c.getL2(ctx, key)
	if err != nil || !ok || it.neg {
		return zero, false, err
	}
	return it.v, true, nil
}

// getL2 reads key from L2 and populates L1 through the guard. Any L2
// failure is a miss.
func (c *cache[V]) getL2(ctx context.Context, key string) (item[V], bool, error) {
	tok := c.l1.Token(key) // before the epoch and the L2 read (L1 guard)
	if !c.l2Readable(ctx, key) {
		return item[V]{}, false, nil
	}
	epoch := c.epoch.Load()
	r, err := c.backend.Read(ctx, l2.ReadRequest{Epoch: epoch, Key: key, GenTTL: c.genTTL(0)})
	if err != nil {
		c.l2Failed(ctx, err)
		return item[V]{}, false, nil
	}
	c.l2OK()
	if !r.Hit {
		c.m.l2Misses.Inc()
		return item[V]{}, false, nil
	}
	it, err := c.decodeItem(r.Payload)
	if err != nil {
		return item[V]{}, false, fmt.Errorf("cache: get %q: %w", key, err)
	}
	c.m.l2Hits.Inc()
	c.insertFromL2(tok, key, it, len(r.Payload), r.TTL, r.Gen)
	return it, true, nil
}

// insertFromL2 populates L1 with a value read from (or written to) L2 at
// generation gen, only if no invalidation touched the shard since tok.
func (c *cache[V]) insertFromL2(tok l1.Token, key string, it item[V], size int, ttl time.Duration, gen uint64) {
	c.l1.SetIfUnchanged(tok, key, l1.Entry[item[V]]{Value: it, Size: size, TTL: c.capTTL(ttl), Version: gen})
}

// GetOrLoad implements Cache.
func (c *cache[V]) GetOrLoad(ctx context.Context, key string, load LoadFunc[V]) (V, error) {
	var zero V
	if c.closed.Load() {
		return zero, errClosed
	}
	if it, ok := c.l1Get(key); ok {
		c.m.l1Hits.Inc()
		if it.neg {
			return zero, ErrNotFound
		}
		return it.v, nil
	}
	c.m.l1Misses.Inc()
	if load == nil {
		return zero, invalid("nil load function")
	}
	it, err := c.loadShared(ctx, key, load)
	if err != nil {
		return zero, err
	}
	return it.v, nil
}

// loadShared runs one fetch per key in this process (singleflight).
// Callers sharing a fetch share its leader's LoadFunc and context. The
// singleflight key includes the L1 shard invalidation counter: a caller that
// starts after an invalidation of the key never joins a fetch that started
// before it (whose loader may have read the old row).
func (c *cache[V]) loadShared(ctx context.Context, key string, load LoadFunc[V]) (item[V], error) {
	for attempt := 0; ; attempt++ {
		tok := c.l1.Token(key)
		flightKey := strconv.FormatUint(tok.Seq(), 36) + ":" + key
		ch := c.group.DoChan(flightKey, func() (any, error) {
			it, err := c.fetch(ctx, key, load, tok)
			return it, err
		})
		select {
		case <-ctx.Done():
			return item[V]{}, ctx.Err()
		case res := <-ch:
			if res.Err != nil && attempt < maxSharedRetries && ctx.Err() == nil && isContextErr(res.Err) {
				continue // the leader gave up, this caller has not
			}
			it, _ := res.Val.(item[V])
			return it, res.Err
		}
	}
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// fetch is the miss path: L2 protocol when L2 is usable, otherwise a local
// load cached in L1 only.
// tok was captured before the fetch was shared, so it predates every read.
func (c *cache[V]) fetch(ctx context.Context, key string, load LoadFunc[V], tok l1.Token) (item[V], error) {
	if c.backend == nil {
		return c.loadLocal(ctx, key, load, tok, false)
	}
	if !c.l2Readable(ctx, key) {
		return c.loadLocal(ctx, key, load, tok, true)
	}
	lc := &loadCall[V]{c: c, key: key, load: load, tok: tok, epoch: c.epoch.Load()}
	return lc.run(ctx)
}

// loadLocal calls the loader and caches the result in L1 only. Without a
// valid L2 generation (capped) the L1 TTL is capped to the degraded TTL.
func (c *cache[V]) loadLocal(ctx context.Context, key string, load LoadFunc[V], tok l1.Token, capped bool) (
	item[V], error,
) {
	c.m.loads.Inc()
	v, err := load(ctx)
	it, ttl := item[V]{v: v}, c.valueTTL()
	if err != nil {
		if !errors.Is(err, ErrNotFound) || c.cfg.negTTL <= 0 {
			return c.loadFailed(key, err)
		}
		it, ttl = item[V]{neg: true}, c.cfg.negTTL
	}
	payload, err := c.encodeItem(it)
	if err != nil {
		return item[V]{}, fmt.Errorf("cache: encode %q: %w", key, err)
	}
	if capped {
		ttl = min(ttl, c.cfg.degradedTTL)
	}
	c.l1.SetIfUnchanged(tok, key, l1.Entry[item[V]]{Value: it, Size: len(payload), TTL: ttl})
	return it, notFoundIf(it)
}

// loadFailed handles a loader error: ErrNotFound passes through; other
// errors are answered from an expired L1 value within stale_if_error.
func (c *cache[V]) loadFailed(key string, err error) (item[V], error) {
	if errors.Is(err, ErrNotFound) {
		return item[V]{neg: true}, ErrNotFound
	}
	if c.cfg.maxStale > 0 {
		if it, ok := c.l1.GetStale(key, c.cfg.maxStale); ok && !it.neg {
			c.m.staleServes.Inc()
			return it, nil
		}
	}
	return item[V]{}, fmt.Errorf("cache: load %q: %w", key, err)
}

func notFoundIf[V any](it item[V]) error {
	if it.neg {
		return ErrNotFound
	}
	return nil
}
