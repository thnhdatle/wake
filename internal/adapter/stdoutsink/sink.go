// SPDX-License-Identifier: Apache-2.0

package stdoutsink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

const queueFactor = 2

const cappedAttempt = 64

// Sink writes each record as a JSON line to a writer and acknowledges it once written.
type Sink struct {
	name          string
	out           io.Writer
	limits        pipeline.BatchLimits
	linger        time.Duration
	maxEventBytes int
	clock         clock.Clock
	log           *slog.Logger
	obs           Observer
	ackObs        AckObserver
	rnd           *rand.Rand
	queue         chan pipeline.Record
	started       atomic.Bool
	buf           []byte

	mu       sync.Mutex
	health   pipeline.SinkHealth
	writeErr string
	tooLarge int
	tooBig   string
}

type Option func(*options)

type options struct {
	out      io.Writer
	observer Observer
	ackObs   AckObserver
	rnd      *rand.Rand
	capacity int
}

func WithWriter(w io.Writer) Option { return func(o *options) { o.out = w } }

func WithObserver(obs Observer) Option { return func(o *options) { o.observer = obs } }

func WithAckObserver(obs AckObserver) Option { return func(o *options) { o.ackObs = obs } }

func WithRand(r *rand.Rand) Option { return func(o *options) { o.rnd = r } }

func WithQueueCapacity(n int) Option { return func(o *options) { o.capacity = n } }

// New returns a stdout Sink for the given config.
func New(cfg config.Sink, clk clock.Clock, log *slog.Logger, opts ...Option) (*Sink, error) {
	if clk == nil || log == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidConfig)
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	o := options{out: os.Stdout, observer: nopObserver{}, ackObs: nopObserver{}}
	for _, opt := range opts {
		opt(&o)
	}
	if o.out == nil {
		return nil, fmt.Errorf("%w: nil writer", ErrInvalidConfig)
	}
	if o.rnd == nil {
		o.rnd = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // G404: non-crypto jitter
	}
	capacity := o.capacity
	if capacity <= 0 {
		capacity = cfg.MaxEvents * queueFactor
	}
	return &Sink{
		name:          cfg.Name,
		out:           o.out,
		limits:        pipeline.BatchLimits{MaxEvents: cfg.MaxEvents, MaxBytes: int(cfg.MaxBytes)},
		linger:        cfg.Linger.Std(),
		maxEventBytes: int(cfg.MaxEventBytes),
		clock:         clk,
		log:           log.With(slog.String("component", "stdoutsink"), slog.String("queue", cfg.Name)),
		obs:           o.observer,
		ackObs:        o.ackObs,
		rnd:           o.rnd,
		queue:         make(chan pipeline.Record, capacity),
		health:        pipeline.SinkHealth{State: pipeline.SinkStateHealthy, Since: clk.Now()},
	}, nil
}

func validate(cfg *config.Sink) error {
	var errs []error
	if cfg.Name == "" {
		errs = append(errs, fmt.Errorf("%w: name is required", ErrInvalidConfig))
	}
	if cfg.MaxEvents < 1 {
		errs = append(errs, fmt.Errorf("%w: sink %q: max_events %d must be at least 1",
			ErrInvalidConfig, cfg.Name, cfg.MaxEvents))
	}
	if cfg.MaxBytes < 0 || cfg.Linger < 0 || cfg.MaxEventBytes < 0 {
		errs = append(errs, fmt.Errorf("%w: sink %q: max_bytes, linger and max_event_bytes must not be negative",
			ErrInvalidConfig, cfg.Name))
	}
	return errors.Join(errs...)
}

func (s *Sink) Name() string { return s.name }

func (s *Sink) Health() pipeline.SinkHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health
}

// Send queues one record for output.
func (s *Sink) Send(ctx context.Context, rec pipeline.Record) error {
	if s.maxEventBytes > 0 && len(rec.Payload) > s.maxEventBytes {
		return s.blockTooLarge(ctx, &rec)
	}
	select {
	case s.queue <- rec:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stdoutsink %s: send: %w", s.name, ctx.Err())
	}
}

func (s *Sink) blockTooLarge(ctx context.Context, rec *pipeline.Record) error {
	s.mu.Lock()
	s.tooLarge++
	s.tooBig = fmt.Sprintf("event %s of table %q (lsn %s) is %d bytes, above max_event_bytes %d",
		rec.ID, rec.Table(), rec.ID.LSN, len(rec.Payload), s.maxEventBytes)
	s.updateLocked()
	s.mu.Unlock()
	s.log.LogAttrs(ctx, slog.LevelError, "event too large, sink blocked until restart with a fixed configuration",
		slog.String("operation", "send"), slog.String("status", pipeline.CodeEventTooLarge),
		slog.String("table", rec.Table()), slog.String("lsn", rec.ID.LSN.String()),
		slog.Int("bytes", len(rec.Payload)))
	<-ctx.Done()
	s.mu.Lock()
	s.tooLarge--
	s.updateLocked()
	s.mu.Unlock()
	return fmt.Errorf("stdoutsink %s: %w: %w", s.name, ErrEventTooLarge, ctx.Err())
}

func (s *Sink) updateLocked() {
	h := pipeline.SinkHealth{State: pipeline.SinkStateHealthy}
	switch {
	case s.tooLarge > 0:
		h = pipeline.SinkHealth{State: pipeline.SinkStateFatal, Code: pipeline.CodeEventTooLarge, Reason: s.tooBig}
	case s.writeErr != "":
		h = pipeline.SinkHealth{State: pipeline.SinkStateRetrying, Reason: s.writeErr}
	}
	h.Since = s.health.Since
	if h.State != s.health.State {
		h.Since = s.clock.Now()
	}
	s.health = h
}

func (s *Sink) setWriteErr(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeErr = reason
	s.updateLocked()
}
