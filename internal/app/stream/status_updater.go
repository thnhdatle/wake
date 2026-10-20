// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// Defaults of stream.status_interval and stream.min_confirm_bytes.
const (
	DefaultStatusInterval  = 10 * time.Second
	DefaultMinConfirmBytes = 1 << 20
)

// ConfirmedSource provides the confirmed LSN and a coalesced signal of its
// advances; AckTracker implements it.
type ConfirmedSource interface {
	Confirmed() lsn.LSN
	ConfirmedChanged() <-chan struct{}
}

// StatusUpdaterOption configures a StatusUpdater.
type StatusUpdaterOption func(*StatusUpdater)

// WithStatusInterval sets stream.status_interval (default
// DefaultStatusInterval).
func WithStatusInterval(d time.Duration) StatusUpdaterOption {
	return func(u *StatusUpdater) { u.interval = d }
}

// WithMinConfirmBytes sets stream.min_confirm_bytes (default
// DefaultMinConfirmBytes); zero sends on every advance.
func WithMinConfirmBytes(n uint64) StatusUpdaterOption {
	return func(u *StatusUpdater) { u.minBytes = n }
}

// CheckStatusInterval is the startup check of stream.status_interval: with wal_sender_timeout
// > 0, status_interval must be below half of it, so that two consecutive
// updates fit in the timeout even while reading is paused. It returns
// ErrStatusInterval otherwise; a zero timeout skips the check.
func CheckStatusInterval(interval, walSenderTimeout time.Duration) error {
	if walSenderTimeout > 0 && 2*interval >= walSenderTimeout {
		return fmt.Errorf("%w: status_interval %s, wal_sender_timeout %s", ErrStatusInterval, interval, walSenderTimeout)
	}
	return nil
}

// StatusUpdater sends standby status updates: every status
// interval, whenever the confirmed LSN advanced by at least min confirm
// bytes since the last update, and at once when the reader saw a keepalive
// requesting a reply. It runs in its own goroutine, independent of the
// reader, so updates continue while reading is paused for backpressure; the
// sender must therefore allow calls concurrent with a blocked Receive.
type StatusUpdater struct {
	sender    pipeline.StandbyStatusSender
	confirmed ConfirmedSource
	clock     clock.Clock
	interval  time.Duration
	minBytes  uint64

	received atomic.Uint64 // highest WAL position received (lsn.LSN)
	reply    chan struct{} // capacity 1: coalesced reply requests

	mu       sync.Mutex // serializes sends and protects lastSent
	lastSent lsn.LSN
}

// NewStatusUpdater validates the dependencies and options.
func NewStatusUpdater(
	sender pipeline.StandbyStatusSender, confirmed ConfirmedSource, clk clock.Clock, opts ...StatusUpdaterOption,
) (*StatusUpdater, error) {
	if sender == nil || confirmed == nil || clk == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidOption)
	}
	u := &StatusUpdater{
		sender:    sender,
		confirmed: confirmed,
		clock:     clk,
		interval:  DefaultStatusInterval,
		minBytes:  DefaultMinConfirmBytes,
		reply:     make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(u)
	}
	if u.interval <= 0 {
		return nil, fmt.Errorf("%w: status interval %s", ErrInvalidOption, u.interval)
	}
	u.lastSent = confirmed.Confirmed()
	return u, nil
}

// ObserveReceived records a WAL position the reader received (reported as
// the written position). Positions only move forward.
func (u *StatusUpdater) ObserveReceived(l lsn.LSN) {
	for {
		cur := u.received.Load()
		if uint64(l) <= cur || u.received.CompareAndSwap(cur, uint64(l)) {
			return
		}
	}
}

// RequestReply asks for an update at once, for a keepalive that requests a
// reply. It never blocks; requests made before the update is sent coalesce.
func (u *StatusUpdater) RequestReply() {
	select {
	case u.reply <- struct{}{}:
	default:
	}
}

// Run sends updates until ctx ends; it returns nil on cancellation and the
// sender's error otherwise.
func (u *StatusUpdater) Run(ctx context.Context) error {
	ticker := u.clock.NewTicker(u.interval)
	defer ticker.Stop()
	for {
		var err error
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			err = u.Flush(ctx)
		case <-u.reply:
			err = u.Flush(ctx)
		case <-u.confirmed.ConfirmedChanged():
			err = u.sendIfAdvanced(ctx)
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

// Flush sends an update with the current confirmed LSN now, e.g. as the
// last step of a graceful shutdown.
func (u *StatusUpdater) Flush(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.sendLocked(ctx, u.confirmed.Confirmed())
}

// LastSent returns the flushed LSN of the last update sent (initially the
// confirmed LSN at construction).
func (u *StatusUpdater) LastSent() lsn.LSN {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastSent
}

func (u *StatusUpdater) sendIfAdvanced(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	c := u.confirmed.Confirmed()
	if c <= u.lastSent || uint64(c-u.lastSent) < u.minBytes {
		return nil
	}
	return u.sendLocked(ctx, c)
}

func (u *StatusUpdater) sendLocked(ctx context.Context, flushed lsn.LSN) error {
	written := max(lsn.LSN(u.received.Load()), flushed)
	status := pipeline.StandbyStatus{Written: written, Flushed: flushed, ClientTime: u.clock.Now()}
	if err := u.sender.SendStandbyStatus(ctx, status); err != nil {
		return fmt.Errorf("stream: send standby status (flushed %s): %w", flushed, err)
	}
	u.lastSent = flushed
	return nil
}
