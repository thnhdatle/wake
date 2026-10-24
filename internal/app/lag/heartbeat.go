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

const HeartbeatPrefix = "wake.heartbeat"

const DefaultHeartbeatInterval = time.Minute

type HeartbeatOption func(*Heartbeat)

func WithHeartbeatInterval(d time.Duration) HeartbeatOption {
	return func(h *Heartbeat) { h.interval = d }
}

func WithHeartbeatLogger(l *slog.Logger) HeartbeatOption {
	return func(h *Heartbeat) { h.logger = l }
}

// Heartbeat emits a logical message when the captured tables are idle.
type Heartbeat struct {
	emitter  pipeline.MessageEmitter
	clock    clock.Clock
	interval time.Duration
	logger   *slog.Logger

	last atomic.Int64
}

// NewHeartbeat validates the dependencies and options.
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

func (h *Heartbeat) Touch(at time.Time) {
	h.advance(at.UnixNano())
}

// Run emits heartbeats while the source is idle until ctx ends.
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
