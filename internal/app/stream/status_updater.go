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

const (
	DefaultStatusInterval  = 10 * time.Second
	DefaultMinConfirmBytes = 1 << 20
)

type ConfirmedSource interface {
	Confirmed() lsn.LSN
	ConfirmedChanged() <-chan struct{}
}

type StatusUpdaterOption func(*StatusUpdater)

func WithStatusInterval(d time.Duration) StatusUpdaterOption {
	return func(u *StatusUpdater) { u.interval = d }
}

func WithMinConfirmBytes(n uint64) StatusUpdaterOption {
	return func(u *StatusUpdater) { u.minBytes = n }
}

func CheckStatusInterval(interval, walSenderTimeout time.Duration) error {
	if walSenderTimeout > 0 && 2*interval >= walSenderTimeout {
		return fmt.Errorf("%w: status_interval %s, wal_sender_timeout %s", ErrStatusInterval, interval, walSenderTimeout)
	}
	return nil
}

// StatusUpdater sends standby status updates with the confirmed LSN.
type StatusUpdater struct {
	sender    pipeline.StandbyStatusSender
	confirmed ConfirmedSource
	clock     clock.Clock
	interval  time.Duration
	minBytes  uint64

	received atomic.Uint64
	reply    chan struct{}

	mu       sync.Mutex
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

func (u *StatusUpdater) ObserveReceived(l lsn.LSN) {
	for {
		cur := u.received.Load()
		if uint64(l) <= cur || u.received.CompareAndSwap(cur, uint64(l)) {
			return
		}
	}
}

func (u *StatusUpdater) RequestReply() {
	select {
	case u.reply <- struct{}{}:
	default:
	}
}

// Run sends updates until ctx ends.
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

func (u *StatusUpdater) Flush(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.sendLocked(ctx, u.confirmed.Confirmed())
}

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
