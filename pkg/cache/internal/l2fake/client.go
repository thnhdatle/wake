// SPDX-License-Identifier: Apache-2.0

package l2fake

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

// ErrUnavailable is returned by every command while the server or the
// client is down, like a connection error.
var ErrUnavailable = errors.New("l2fake: connection refused")

// Op names an l2 operation for hooks and call counts.
type Op string

// Operations of the l2 contract.
const (
	OpRead       Op = "read"
	OpFill       Op = "fill"
	OpSet        Op = "set"
	OpInvalidate Op = "invalidate"
	OpLock       Op = "lock"
	OpUnlock     Op = "unlock"
	OpEpoch      Op = "epoch"
	OpBumpEpoch  Op = "bump_epoch"
)

// Hooks run around every command, outside the server lock, on the caller's
// goroutine. Before may block (to force an interleaving) or return an error
// (injected failure or timeout); After runs once the command has executed.
type Hooks struct {
	Before func(ctx context.Context, op Op, key string) error
	After  func(op Op, key string)
}

// Client is one cache instance's connection; it implements l2.Backend.
type Client struct {
	srv *Server
	cfg l2.Config
	sub *subscription

	// Guarded by srv.mu.
	down    bool
	busDown bool
	closed  bool
	hooks   Hooks
	calls   map[Op]int
}

var _ l2.Backend = (*Client)(nil)

// SetDown makes this client unreachable (commands fail, bus disconnects).
func (c *Client) SetDown(down bool) {
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	c.down = down
	c.srv.refreshLocked(c.sub)
}

// SetBusDown disconnects (true) or reconnects only the bus subscription.
func (c *Client) SetBusDown(down bool) {
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	c.busDown = down
	c.srv.refreshLocked(c.sub)
}

// PauseBus holds queued events back from Receive until ResumeBus.
func (c *Client) PauseBus() { c.sub.setPaused(true) }

// ResumeBus releases events held by PauseBus.
func (c *Client) ResumeBus() { c.sub.setPaused(false) }

// WaitIdle blocks until this client's subscriber has handled every queued
// event.
func (c *Client) WaitIdle(ctx context.Context) error { return c.sub.waitIdle(ctx) }

// SetHooks installs hooks for later commands.
func (c *Client) SetHooks(h Hooks) {
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	c.hooks = h
}

// Calls returns how many times op was attempted.
func (c *Client) Calls(op Op) int {
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	return c.calls[op]
}

// do runs one command: hooks, reachability, then fn under the server lock.
func (c *Client) do(ctx context.Context, op Op, key string, fn func()) error {
	c.srv.mu.Lock()
	c.calls[op]++
	h := c.hooks
	c.srv.mu.Unlock()
	if h.Before != nil {
		if err := h.Before(ctx, op, key); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.srv.mu.Lock()
	if c.srv.down || c.down || c.closed {
		c.srv.mu.Unlock()
		return ErrUnavailable
	}
	// Redis runs one script at a time and each takes at least a
	// microsecond, so TIME advances with every command; generations, which
	// grow by one per script, therefore never overtake TIME.
	c.srv.scriptMicros = c.srv.timeMicrosLocked()
	fn()
	c.srv.mu.Unlock()
	if h.After != nil {
		h.After(op, key)
	}
	return nil
}

// Read implements l2.Store.
func (c *Client) Read(ctx context.Context, req l2.ReadRequest) (l2.ReadResult, error) {
	var res l2.ReadResult
	err := c.do(ctx, OpRead, req.Key, func() {
		s := c.srv
		v, g, _ := l2.KeyNames(c.cfg.Namespace, req.Epoch, req.Key)
		res.Gen = s.genLocked(g, req.GenTTL)
		raw, ok := s.getLocked(v)
		if !ok {
			return
		}
		sv, err := l2.DecodeValue([]byte(raw))
		nowMS := s.clock.Now().UnixMilli()
		if err != nil || sv.Gen != res.Gen || sv.ExpiresAtMS <= nowMS {
			return
		}
		res.Hit = true
		res.Payload = append([]byte(nil), sv.Payload...)
		res.TTL = time.Duration(sv.ExpiresAtMS-nowMS) * time.Millisecond
	})
	return res, err
}

// Fill implements l2.Store.
func (c *Client) Fill(ctx context.Context, req l2.FillRequest) (bool, error) {
	var stored bool
	err := c.do(ctx, OpFill, req.Key, func() {
		s := c.srv
		v, g, l := l2.KeyNames(c.cfg.Namespace, req.Epoch, req.Key)
		stored = s.genLocked(g, req.GenTTL) == req.Gen
		if stored {
			c.writeValueLocked(v, req.Gen, req.Payload, req.TTL)
		}
		released := false
		if req.LockToken != "" {
			if tok, ok := s.getLocked(l); ok && tok == req.LockToken {
				delete(s.data, l)
				released = true
			}
		}
		if stored || released {
			s.publishLocked(c.cfg.Channel, l2.Event{Kind: l2.EventLoaded, Epoch: req.Epoch, Key: req.Key})
		}
	})
	return stored, err
}

// Set implements l2.Store.
func (c *Client) Set(ctx context.Context, req l2.SetRequest) (l2.WriteResult, error) {
	var res l2.WriteResult
	err := c.do(ctx, OpSet, req.Key, func() {
		s := c.srv
		v, g, _ := l2.KeyNames(c.cfg.Namespace, req.Epoch, req.Key)
		res.Gen = s.incrGenLocked(g, req.GenTTL)
		c.writeValueLocked(v, res.Gen, req.Payload, req.TTL)
		s.publishLocked(c.cfg.Channel, l2.Event{Kind: l2.EventInvalidate, Epoch: req.Epoch, Gen: res.Gen, Key: req.Key})
		res.Epoch = s.epochLocked(c.cfg.Namespace)
	})
	return res, err
}

// Invalidate implements l2.Store.
func (c *Client) Invalidate(ctx context.Context, req l2.InvalidateRequest) (l2.WriteResult, error) {
	var res l2.WriteResult
	err := c.do(ctx, OpInvalidate, req.Key, func() {
		s := c.srv
		v, g, _ := l2.KeyNames(c.cfg.Namespace, req.Epoch, req.Key)
		res.Gen = s.incrGenLocked(g, req.GenTTL)
		delete(s.data, v)
		s.publishLocked(c.cfg.Channel, l2.Event{Kind: l2.EventInvalidate, Epoch: req.Epoch, Gen: res.Gen, Key: req.Key})
		res.Epoch = s.epochLocked(c.cfg.Namespace)
	})
	return res, err
}

func (c *Client) writeValueLocked(vkey string, gen uint64, payload []byte, ttl time.Duration) {
	s := c.srv
	exp := s.clock.Now().Add(ttl).UnixMilli()
	s.setLocked(vkey, string(l2.EncodeValue(l2.StoredValue{Gen: gen, ExpiresAtMS: exp, Payload: payload})), ttl)
}

// Lock implements l2.Locker.
func (c *Client) Lock(ctx context.Context, epoch uint64, key, token string, ttl time.Duration) (bool, error) {
	var ok bool
	err := c.do(ctx, OpLock, key, func() {
		_, _, l := l2.KeyNames(c.cfg.Namespace, epoch, key)
		if _, held := c.srv.getLocked(l); held {
			return
		}
		c.srv.setLocked(l, token, ttl)
		ok = true
	})
	return ok, err
}

// Unlock implements l2.Locker.
func (c *Client) Unlock(ctx context.Context, epoch uint64, key, token string) error {
	return c.do(ctx, OpUnlock, key, func() {
		s := c.srv
		_, _, l := l2.KeyNames(c.cfg.Namespace, epoch, key)
		if tok, ok := s.getLocked(l); ok && tok == token {
			delete(s.data, l)
			s.publishLocked(c.cfg.Channel, l2.Event{Kind: l2.EventLoaded, Epoch: epoch, Key: key})
		}
	})
}

// Epoch implements l2.EpochStore.
func (c *Client) Epoch(ctx context.Context) (uint64, error) {
	var e uint64
	err := c.do(ctx, OpEpoch, "", func() { e = c.srv.epochLocked(c.cfg.Namespace) })
	return e, err
}

// BumpEpoch implements l2.EpochStore.
func (c *Client) BumpEpoch(ctx context.Context) (uint64, error) {
	var e uint64
	err := c.do(ctx, OpBumpEpoch, "", func() {
		s := c.srv
		e = s.epochLocked(c.cfg.Namespace) + 1
		s.data[l2.EpochKey(c.cfg.Namespace)] = record{val: strconv.FormatUint(e, 10)}
		s.publishLocked(c.cfg.Channel, l2.Event{Kind: l2.EventEpoch, Epoch: e})
	})
	return e, err
}

// Receive implements l2.Bus.
func (c *Client) Receive(ctx context.Context) (l2.Event, error) { return c.sub.receive(ctx) }

// Close implements l2.Bus. It is idempotent.
func (c *Client) Close() error {
	c.srv.mu.Lock()
	defer c.srv.mu.Unlock()
	c.closed = true
	delete(c.srv.subs, c.sub)
	c.sub.close()
	return nil
}
