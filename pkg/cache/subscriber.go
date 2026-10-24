// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"time"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

const busRetryDelay = time.Second

func (c *cache[V]) runSubscriber(ctx context.Context) {
	defer c.wg.Done()
	resync := false
	for {
		ev, err := c.backend.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, l2.ErrClosed) {
				return
			}
			c.onBusDown()
			resync = true
			select {
			case <-ctx.Done():
				return
			case <-c.clock().After(busRetryDelay):
			}
			continue
		}
		if resync && ev.Kind != l2.EventConnected && ev.Kind != l2.EventDisconnected {
			c.onBusConnected(ctx)
		}
		resync = false
		c.handleEvent(ctx, ev)
	}
}

func (c *cache[V]) handleEvent(ctx context.Context, ev l2.Event) {
	switch ev.Kind {
	case l2.EventConnected:
		c.onBusConnected(ctx)
	case l2.EventDisconnected:
		c.onBusDown()
	case l2.EventInvalidate:
		c.m.invalidationsReceived.Inc()
		if c.epochOK.Load() && ev.Epoch == c.epoch.Load() {
			c.l1.Invalidate(ev.Key, ev.Gen)
		} else {
			c.l1.Delete(ev.Key)
		}
	case l2.EventLoaded:
		c.signals.notify(ev.Key)
	case l2.EventEpoch:
		c.adoptEpoch(ev.Epoch)
	}
}

func (c *cache[V]) onBusConnected(ctx context.Context) {
	c.l1.Flush()
	e, err := c.backend.Epoch(ctx)
	if err != nil {
		c.epochOK.Store(false)
		c.l2Failed(ctx, err)
	} else {
		c.l2OK()
		c.adoptEpoch(e)
	}
	c.busUp.Store(true)
	c.m.busUp.Set(1)
}

func (c *cache[V]) onBusDown() {
	c.busUp.Store(false)
	c.m.busUp.Set(0)
	c.wakeMaintenance()
}
