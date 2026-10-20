// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// HeartbeatPrefix is the logical message prefix of heartbeats; the
// decoder consumes these messages internally.
const HeartbeatPrefix = "wake.heartbeat"

// DefaultHeartbeatInterval is the default heartbeat.interval.
const DefaultHeartbeatInterval = time.Minute

// HeartbeatOption configures a Heartbeat.
type HeartbeatOption func(*Heartbeat)

// WithHeartbeatInterval sets heartbeat.interval (default
// DefaultHeartbeatInterval): the idle time before a heartbeat, the time
// between heartbeats while idle, and the timeout of each emit.
func WithHeartbeatInterval(d time.Duration) HeartbeatOption {
	return func(h *Heartbeat) { h.interval = d }
}

// WithHeartbeatLogger sets the logger (default: discard).
func WithHeartbeatLogger(l *slog.Logger) HeartbeatOption {
	return func(h *Heartbeat) { h.logger = l }
}

// Heartbeat emits a transactional `wake.heartbeat` logical message whenever
// the captured tables were idle for heartbeat.interval, so the server
// sends a transaction the ack tracker confirms at once, the confirmed LSN
// advances and WAL written for other databases can be recycled. The reader
// calls Touch for every transaction with captured changes; while it does so
// at least once per interval, no heartbeat is emitted. A failed emit is
// logged and retried one interval later.
type Heartbeat struct {
	emitter  pipeline.MessageEmitter
	clock    clock.Clock
	interval time.Duration
	logger   *slog.Logger

	last atomic.Int64 // Unix ns of the latest activity or heartbeat attempt
}

// NewHeartbeat validates the dependencies and options. The idle time counts
// from the construction.
func NewHeartbeat(emitter pipeline.MessageEmitter, clk clock.Clock, opts ...HeartbeatOption) (*Heartbeat, error) {
	if emitter == nil || clk == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidOption)
	}
	h := &Heartbeat{
		emitter: emitter, clock: clk, interval: DefaultHeartbeatInterval,
		logger: slog.New(slog.DiscardHandler),
	}
	for _, o := range opts {
		o(h)
	}
	if h.interval <= 0 {
		return nil, fmt.Errorf("%w: heartbeat interval %s", ErrInvalidOption, h.interval)
	}
	if h.logger == nil {
		return nil, fmt.Errorf("%w: nil logger", ErrInvalidOption)
	}
	h.last.Store(clk.Now().UnixNano())
	return h, nil
}

// Touch records that a transaction with captured changes was read at at.
// It never blocks and is safe for concurrent use; earlier times than the
// latest recorded one are ignored.
func (h *Heartbeat) Touch(at time.Time) {
	h.advance(at.UnixNano())
}

// Run emits heartbeats while the source is idle until ctx ends; it returns
// nil.
func (h *Heartbeat) Run(ctx context.Context) error {
	for {
		now := h.clock.Now()
		wait := h.interval - now.Sub(time.Unix(0, h.last.Load()))
		if wait <= 0 {
			h.emit(ctx, now)
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-h.clock.After(wait):
		}
	}
}

func (h *Heartbeat) emit(ctx context.Context, now time.Time) {
	h.advance(now.UnixNano())
	content := []byte(`{"ts":` + strconv.FormatInt(now.UnixMilli(), 10) + `}`)
	ectx, cancel := context.WithTimeout(ctx, h.interval)
	at, err := h.emitter.EmitMessage(ectx, HeartbeatPrefix, content)
	cancel()
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		h.logger.LogAttrs(ctx, slog.LevelWarn, "heartbeat emit failed, retrying after the heartbeat interval",
			slog.String("component", "heartbeat"), slog.String("operation", "emit"), slog.Any("error", err))
		return
	}
	h.logger.LogAttrs(ctx, slog.LevelDebug, "heartbeat emitted",
		slog.String("component", "heartbeat"), slog.String("operation", "emit"), slog.String("lsn", at.String()))
}

func (h *Heartbeat) advance(ns int64) {
	for {
		cur := h.last.Load()
		if ns <= cur || h.last.CompareAndSwap(cur, ns) {
			return
		}
	}
}
