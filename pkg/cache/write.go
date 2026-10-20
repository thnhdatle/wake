// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/l1"
)

// maxEpochRetries bounds how often a write is repeated because the
// namespace epoch changed under it.
const maxEpochRetries = 3

// Set implements Cache: one L2 script increments the generation, writes v
// with it and publishes the invalidation; then L1 is set locally.
func (c *cache[V]) Set(ctx context.Context, key string, v V, opts ...SetOption) error {
	if c.closed.Load() {
		return errClosed
	}
	var sc setConfig
	for _, o := range opts {
		o(&sc)
	}
	if sc.ttl < 0 {
		return invalid("set %q: non-positive ttl", key)
	}
	ttl := sc.ttl
	if ttl == 0 {
		ttl = c.valueTTL()
	}
	it := item[V]{v: v}
	payload, err := c.encodeItem(it)
	if err != nil {
		return fmt.Errorf("cache: set %q: %w", key, err)
	}
	entry := l1.Entry[item[V]]{Value: it, Size: len(payload), TTL: ttl}
	if c.backend == nil {
		c.l1.Set(key, entry)
		return nil
	}
	if c.l2Writable(ctx) {
		seq := c.pending.seq()
		res, err := c.writeAtCurrentEpoch(ctx, func(epoch uint64) (l2.WriteResult, error) {
			return c.backend.Set(ctx, l2.SetRequest{
				Epoch: epoch, Key: key, Payload: payload, TTL: ttl, GenTTL: c.genTTL(ttl),
			})
		})
		if err == nil {
			c.pending.removeUpTo(key, seq)
			entry.TTL, entry.Version = c.capTTL(ttl), res.Gen
			c.l1.Set(key, entry)
			return nil
		}
		if ctx.Err() != nil {
			c.l1.Delete(key)
			return fmt.Errorf("cache: set %q: %w", key, err)
		}
	}
	c.l1.Delete(key)
	return c.deferInvalidation(key, "set")
}

// Delete implements Cache: per key one L2 script increments the
// generation, deletes the value and publishes the invalidation; then the
// key is removed from the local L1. While L2 is unreachable the key is
// removed locally and its invalidation deferred (ErrDeferred).
func (c *cache[V]) Delete(ctx context.Context, keys ...string) error {
	if c.closed.Load() {
		return errClosed
	}
	var errs []error
	for _, k := range keys {
		if err := c.deleteOne(ctx, k); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *cache[V]) deleteOne(ctx context.Context, key string) error {
	if c.backend == nil {
		c.l1.Delete(key)
		return nil
	}
	if c.l2Writable(ctx) {
		res, err := c.invalidate(ctx, key)
		if err == nil {
			c.l1.Invalidate(key, res.Gen)
			return nil
		}
		if ctx.Err() != nil {
			c.l1.Delete(key)
			return fmt.Errorf("cache: delete %q: %w", key, err)
		}
	}
	c.l1.Delete(key)
	return c.deferInvalidation(key, "delete")
}

// invalidate runs the L2 invalidation of key and clears it from the pending
// set (only additions that happened before the script ran).
func (c *cache[V]) invalidate(ctx context.Context, key string) (l2.WriteResult, error) {
	seq := c.pending.seq()
	res, err := c.writeAtCurrentEpoch(ctx, func(epoch uint64) (l2.WriteResult, error) {
		return c.backend.Invalidate(ctx, l2.InvalidateRequest{Epoch: epoch, Key: key, GenTTL: c.genTTL(0)})
	})
	if err != nil {
		return res, err
	}
	c.pending.removeUpTo(key, seq)
	c.m.pendingDepth.Set(float64(c.pending.size()))
	return res, nil
}

// writeAtCurrentEpoch runs write at the cached epoch and repeats it at the
// epoch the server reports when they differ, so an instance that has not
// yet seen an epoch change still invalidates the live keyspace. It records
// failures itself.
func (c *cache[V]) writeAtCurrentEpoch(ctx context.Context, write func(epoch uint64) (l2.WriteResult, error)) (
	l2.WriteResult, error,
) {
	epoch := c.epoch.Load()
	for range maxEpochRetries {
		res, err := write(epoch)
		if err != nil {
			c.l2Failed(ctx, err)
			return res, err
		}
		c.l2OK()
		if res.Epoch == epoch {
			return res, nil
		}
		c.adoptEpoch(res.Epoch)
		epoch = res.Epoch
	}
	return l2.WriteResult{}, errEpochUnstable
}

// deferInvalidation queues key for replay, or fails when deferral is
// disabled.
func (c *cache[V]) deferInvalidation(key, op string) error {
	if c.cfg.maxPending == 0 {
		return fmt.Errorf("cache: %s %q: %w", op, key, errL2Unavailable)
	}
	if c.pending.add(key) {
		c.m.pendingOverflows.Inc()
	}
	c.m.deferred.Inc()
	c.m.pendingDepth.Set(float64(c.pending.size()))
	c.wakeMaintenance()
	return fmt.Errorf("cache: %s %q: %w", op, key, ErrDeferred)
}
