// SPDX-License-Identifier: Apache-2.0

package cache

import "context"

// runMaintenance is the second goroutine started by New and stopped by
// Close. It sleeps until woken (breaker opened, invalidation
// deferred, bus lost) and then, every probeInterval until nothing is left
// to do: probes L2 while the breaker is open (half-open), refreshes the
// epoch while the bus is down, and replays deferred invalidations.
func (c *cache[V]) runMaintenance(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		}
		for c.needsMaintenance() {
			select {
			case <-ctx.Done():
				return
			case <-c.clock().After(probeInterval):
			}
			c.maintain(ctx)
		}
	}
}

func (c *cache[V]) needsMaintenance() bool {
	return !c.breaker.allow() || c.pending.active.Load() || !c.busUp.Load() || !c.epochOK.Load()
}

func (c *cache[V]) maintain(ctx context.Context) {
	switch {
	case c.breaker.halfOpen():
		c.m.breakerState.Set(float64(breakerHalfOpen))
		e, err := c.backend.Epoch(ctx)
		if err != nil {
			c.breaker.reopen()
			c.m.breakerState.Set(float64(breakerOpen))
			c.m.l2Errors.Inc()
			return
		}
		c.adoptEpoch(e)
		c.breaker.close()
		c.m.breakerState.Set(float64(breakerClosed))
	case !c.breaker.allow():
		return
	case !c.busUp.Load() || !c.epochOK.Load():
		// Epoch messages may be missed while the bus is down.
		e, err := c.backend.Epoch(ctx)
		if err != nil {
			c.l2Failed(ctx, err)
			return
		}
		c.l2OK()
		c.adoptEpoch(e)
	}
	c.replayPending(ctx)
}

// replayPending applies deferred invalidations: an epoch increment when the
// set overflowed, otherwise one invalidation per key. It stops at the first
// failure and retries on the next round.
func (c *cache[V]) replayPending(ctx context.Context) {
	if !c.pending.active.Load() {
		return
	}
	keys, overflow, seq := c.pending.snapshot()
	if overflow {
		e, err := c.backend.BumpEpoch(ctx)
		if err != nil {
			c.l2Failed(ctx, err)
			return
		}
		c.l2OK()
		c.adoptEpoch(e)
		c.pending.resolveUpTo(seq)
		c.m.pendingDepth.Set(float64(c.pending.size()))
		return
	}
	for _, k := range keys {
		res, err := c.invalidate(ctx, k)
		if err != nil {
			return
		}
		c.l1.Invalidate(k, res.Gen)
	}
}
