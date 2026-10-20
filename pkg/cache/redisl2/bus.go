// SPDX-License-Identifier: Apache-2.0

package redisl2

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

const (
	// defaultHealthCheck is the bus health-check interval when the config
	// leaves it unset; minHealthCheck bounds PING traffic.
	defaultHealthCheck = time.Second
	minHealthCheck     = 10 * time.Millisecond
	// Reconnect backoff: exponential from backoffMin to backoffMax, with
	// jitter in [d/2, d).
	backoffMin = 50 * time.Millisecond
	backoffMax = 2 * time.Second
)

// bus owns the subscription connection. One goroutine (run) reads it and
// hands each event to receive over an unbuffered channel, so nothing is
// queued in the process: a consumer that falls behind stops the reads and
// Redis' client-output-buffer-limit eventually drops the connection, which
// is reported as a disconnect.
type bus struct {
	client  redis.UniversalClient
	channel string
	health  time.Duration

	events chan l2.Event  // unbuffered
	done   chan struct{}  // closed by close
	cancel func()         // ends run's context
	wg     sync.WaitGroup // waits for run

	mu     sync.Mutex // protects ps and closed
	ps     *redis.PubSub
	closed bool

	// connected is owned by run: an EventConnected was emitted and no
	// EventDisconnected since.
	connected bool
	closeOnce sync.Once
}

func startBus(client redis.UniversalClient, channel string, health time.Duration) *bus {
	if health <= 0 {
		health = defaultHealthCheck
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &bus{
		client:  client,
		channel: channel,
		health:  max(health, minHealthCheck),
		events:  make(chan l2.Event),
		done:    make(chan struct{}),
		cancel:  cancel,
	}
	b.wg.Add(1)
	go b.run(ctx)
	return b
}

func (b *bus) receive(ctx context.Context) (l2.Event, error) {
	select {
	case <-b.done:
		return l2.Event{}, l2.ErrClosed
	default:
	}
	select {
	case ev := <-b.events:
		return ev, nil
	case <-b.done:
		return l2.Event{}, l2.ErrClosed
	case <-ctx.Done():
		return l2.Event{}, ctx.Err()
	}
}

// close stops run and closes the subscription; it is idempotent.
func (b *bus) close() {
	b.closeOnce.Do(func() {
		close(b.done)
		b.cancel()
		b.mu.Lock()
		b.closed = true
		ps := b.ps
		b.mu.Unlock()
		if ps != nil {
			_ = ps.Close() // unblocks a pending read; run closes it too
		}
		b.wg.Wait()
	})
}

// run keeps one subscription alive until close: open, serve until the link
// fails, report the disconnect, back off, repeat.
func (b *bus) run(ctx context.Context) {
	defer b.wg.Done()
	failures := 0
	for {
		if failures > 0 && !sleep(ctx, backoff(failures)) {
			return
		}
		ps := b.open(ctx)
		if ps == nil {
			return
		}
		healthy := b.serve(ctx, ps)
		b.release(ps)
		if b.connected {
			b.connected = false
			if !b.emit(ctx, l2.Event{Kind: l2.EventDisconnected}) {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		if healthy {
			failures = 1 // reconnect promptly after a working link broke
		} else {
			failures++
		}
	}
}

// open creates a subscription to the channel; nil once closed. Creating the
// PubSub does no I/O; Subscribe registers the channel even when it cannot
// connect, and ReceiveTimeout then reports the connection error.
func (b *bus) open(ctx context.Context) *redis.PubSub {
	ps := b.client.Subscribe(ctx)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = ps.Close()
		return nil
	}
	b.ps = ps
	b.mu.Unlock()
	_ = ps.Subscribe(ctx, b.channel) // errors resurface from ReceiveTimeout
	return ps
}

func (b *bus) release(ps *redis.PubSub) {
	b.mu.Lock()
	if b.ps == ps {
		b.ps = nil
	}
	b.mu.Unlock()
	_ = ps.Close() // ErrClosed when close got there first
}

// serve reads ps until the link fails or ctx ends, and reports whether the
// subscription was confirmed. After health of silence it sends PING; a
// second silent interval means the link is dead.
func (b *bus) serve(ctx context.Context, ps *redis.PubSub) bool {
	confirmed, pinged := false, false
	for {
		msg, err := ps.ReceiveTimeout(ctx, b.health)
		switch {
		case err == nil:
			pinged = false
			if !b.handle(ctx, msg, &confirmed) {
				return confirmed
			}
		case isTimeout(err) && !pinged && ctx.Err() == nil:
			if ps.Ping(ctx) != nil {
				return confirmed
			}
			pinged = true
		default:
			return confirmed
		}
	}
}

// handle turns one pub/sub reply into events; false when ctx ended. Every
// "subscribe" confirmation means a new connection: one that follows an
// already reported connection (go-redis reconnected by itself) is reported
// as a disconnect first, since messages may have been lost in between.
func (b *bus) handle(ctx context.Context, msg any, confirmed *bool) bool {
	switch m := msg.(type) {
	case *redis.Subscription:
		if m.Kind != "subscribe" || m.Channel != b.channel {
			return true
		}
		*confirmed = true
		if b.connected && !b.emit(ctx, l2.Event{Kind: l2.EventDisconnected}) {
			return false
		}
		b.connected = true
		return b.emit(ctx, l2.Event{Kind: l2.EventConnected})
	case *redis.Message:
		if m.Channel != b.channel {
			return true
		}
		ev, err := l2.DecodeMessage(m.Payload)
		if err != nil {
			return true // foreign or garbage message on the channel: skipped
		}
		return b.emit(ctx, ev)
	}
	return true // *redis.Pong
}

func (b *bus) emit(ctx context.Context, ev l2.Event) bool {
	select {
	case b.events <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// backoff returns the delay before reconnect attempt n (n >= 1).
func backoff(n int) time.Duration {
	d := backoffMax
	if n <= 10 {
		d = min(backoffMin<<(n-1), backoffMax)
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2))) //nolint:gosec // G404: non-crypto reconnect jitter
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
