// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/l1"
)

const (
	pollInitial = 20 * time.Millisecond
	pollMax     = 500 * time.Millisecond
)

// loadCall is one in-process leader's miss on key with L2 usable:
// read L2 → lock → (holder) re-check, load, generation-checked fill →
// (waiter) wait for "loaded" or poll until lock_timeout, then fall back.
type loadCall[V any] struct {
	c     *cache[V]
	key   string
	load  LoadFunc[V]
	tok   l1.Token // captured before the first L2 read (L1 guard)
	epoch uint64
}

// attemptResult tells run whether an attempt finished and, if not, the
// generation seen by its L2 read.
type attemptResult[V any] struct {
	it   item[V]
	err  error
	done bool
	gen  uint64
}

func (lc *loadCall[V]) run(ctx context.Context) (item[V], error) {
	c := lc.c
	var deadline <-chan time.Time
	backoff := pollInitial
	for {
		// Watch before reading so a "loaded" published after the read is
		// not missed.
		loaded, release := c.signals.watch(lc.key)
		res := lc.attempt(ctx, deadline == nil)
		if res.done {
			release()
			return res.it, res.err
		}
		if deadline == nil {
			c.m.lockWaits.Inc()
			deadline = c.clock().After(c.cfg.lockTimeout)
		}
		select {
		case <-loaded:
		case <-c.clock().After(c.spread(backoff)):
			backoff = min(2*backoff, pollMax)
		case <-deadline:
			release()
			c.m.fallbackLoads.Inc()
			return lc.loadAndFill(ctx, res.gen, "")
		case <-ctx.Done():
			release()
			return item[V]{}, ctx.Err()
		}
		release()
	}
}

// attempt reads L2 and tries to take the load lock once.
func (lc *loadCall[V]) attempt(ctx context.Context, first bool) attemptResult[V] {
	c := lc.c
	r, err := c.backend.Read(ctx, lc.readRequest())
	if err != nil {
		c.l2Failed(ctx, err)
		it, lerr := c.loadLocal(ctx, lc.key, lc.load, lc.tok, true)
		return attemptResult[V]{it: it, err: lerr, done: true}
	}
	c.l2OK()
	if it, ok := lc.hit(r); ok {
		return attemptResult[V]{it: it, err: notFoundIf(it), done: true}
	}
	if first {
		c.m.l2Misses.Inc()
	}
	token := c.newLockToken()
	acquired, err := c.backend.Lock(ctx, lc.epoch, lc.key, token, c.cfg.lockTimeout)
	if err != nil {
		c.l2Failed(ctx, err)
		it, lerr := c.loadLocal(ctx, lc.key, lc.load, lc.tok, true)
		return attemptResult[V]{it: it, err: lerr, done: true}
	}
	c.l2OK()
	if acquired {
		it, herr := lc.holder(ctx, token)
		return attemptResult[V]{it: it, err: herr, done: true}
	}
	return attemptResult[V]{gen: r.Gen}
}

func (lc *loadCall[V]) readRequest() l2.ReadRequest {
	return l2.ReadRequest{Epoch: lc.epoch, Key: lc.key, GenTTL: lc.c.genTTL(0)}
}

// hit decodes a valid L2 value and populates L1 through the guard. An
// undecodable value is treated as a miss and overwritten by the load.
func (lc *loadCall[V]) hit(r l2.ReadResult) (item[V], bool) {
	if !r.Hit {
		return item[V]{}, false
	}
	it, err := lc.c.decodeItem(r.Payload)
	if err != nil {
		return item[V]{}, false
	}
	lc.c.m.l2Hits.Inc()
	lc.c.insertFromL2(lc.tok, lc.key, it, len(r.Payload), r.TTL, r.Gen)
	return it, true
}

// holder runs with the load lock held: re-check L2, then load and fill.
func (lc *loadCall[V]) holder(ctx context.Context, token string) (item[V], error) {
	c := lc.c
	r, err := c.backend.Read(ctx, lc.readRequest())
	if err != nil {
		c.l2Failed(ctx, err)
		return c.loadLocal(ctx, lc.key, lc.load, lc.tok, true)
	}
	c.l2OK()
	if it, ok := lc.hit(r); ok {
		lc.unlock(ctx, token)
		return it, notFoundIf(it)
	}
	return lc.loadAndFill(ctx, r.Gen, token)
}

// loadAndFill calls the loader and writes the result only if the
// generation is still gen (captured before the loader ran). A rejected
// write still returns the loaded value but caches nothing. token releases
// the lock (empty for fallback loads).
func (lc *loadCall[V]) loadAndFill(ctx context.Context, gen uint64, token string) (item[V], error) {
	c := lc.c
	c.m.loads.Inc()
	v, err := lc.load(ctx)
	it, ttl := item[V]{v: v}, c.valueTTL()
	if err != nil {
		if !errors.Is(err, ErrNotFound) || c.cfg.negTTL <= 0 {
			lc.unlock(ctx, token)
			return c.loadFailed(lc.key, err)
		}
		it, ttl = item[V]{neg: true}, c.cfg.negTTL
	}
	payload, err := c.encodeItem(it)
	if err != nil {
		lc.unlock(ctx, token)
		return item[V]{}, fmt.Errorf("cache: encode %q: %w", lc.key, err)
	}
	stored, err := c.backend.Fill(ctx, l2.FillRequest{
		Epoch: lc.epoch, Key: lc.key, Gen: gen, Payload: payload,
		TTL: ttl, GenTTL: c.genTTL(ttl), LockToken: token,
	})
	if err != nil {
		c.l2Failed(ctx, err)
	} else {
		c.l2OK()
	}
	if stored {
		lc.c.insertFromL2(lc.tok, lc.key, it, len(payload), ttl, gen)
	}
	return it, notFoundIf(it)
}

// unlock releases the lock held with token, even if ctx was canceled.
func (lc *loadCall[V]) unlock(ctx context.Context, token string) {
	if token == "" {
		return
	}
	uctx := context.WithoutCancel(ctx)
	if err := lc.c.backend.Unlock(uctx, lc.epoch, lc.key, token); err != nil {
		lc.c.l2Failed(uctx, err)
		return
	}
	lc.c.l2OK()
}
