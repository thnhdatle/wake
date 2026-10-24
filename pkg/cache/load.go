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

type loadCall[V any] struct {
	c     *cache[V]
	key   string
	load  LoadFunc[V]
	tok   l1.Token
	epoch uint64
}

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
