// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// Defaults of the webhook.* options, applied when a field is zero.
const (
	DefaultTimeout       = 10 * time.Second
	DefaultMaxAttempts   = 10
	DefaultMaxEventBytes = 10 << 20
)

// queueFactor sizes the default queue: lanes × max_events × queueFactor
// records, so every lane can have a full batch queued behind the one in
// flight.
const queueFactor = 2

// Sink is the webhook sink. Send, Health and Name are safe for concurrent
// use; Run is called once.
type Sink struct {
	name          string
	url           string
	secret        []byte
	client        *http.Client
	limits        pipeline.BatchLimits
	linger        time.Duration
	maxEventBytes int
	maxAttempts   int
	deadLetterDir string
	backoff       pipeline.Backoff
	clock         clock.Clock
	log           *slog.Logger
	obs           Observer
	ackObs        AckObserver
	notify        chan struct{} // capacity 1: wakes the dispatcher in Run
	started       atomic.Bool

	randMu sync.Mutex // protects rnd
	rnd    *rand.Rand

	mu          sync.Mutex // protects every field below
	sched       *pipeline.LaneScheduler
	queuedBytes int
	firstQueued time.Time     // when the queue last became non-empty
	space       chan struct{} // closed and replaced when queued records leave
	inflight    int
	health      healthTracker
}

// Option configures a Sink.
type Option func(*options)

type options struct {
	observer Observer
	ackObs   AckObserver
	rnd      *rand.Rand
	backoff  pipeline.Backoff
	capacity int
}

// WithObserver receives the sink's metrics.
func WithObserver(obs Observer) Option { return func(o *options) { o.observer = obs } }

// WithAckObserver receives the ts_ms of every delivered event.
func WithAckObserver(obs AckObserver) Option { return func(o *options) { o.ackObs = obs } }

// WithRand sets the seeded source of backoff jitter (default: randomly
// seeded).
func WithRand(r *rand.Rand) Option { return func(o *options) { o.rnd = r } }

// WithBackoff overrides the retry delays (default pipeline.Backoff{}:
// 100 ms doubling up to 30 s, equal jitter).
func WithBackoff(b pipeline.Backoff) Option { return func(o *options) { o.backoff = b } }

// WithQueueCapacity bounds the queued records (default lanes × max_events ×
// 2). Send blocks while the queue is full.
func WithQueueCapacity(n int) Option { return func(o *options) { o.capacity = n } }

// New validates cfg (a sink of type webhook), reads the signing secret from
// the environment variable named by webhook.secret_env through getenv, and
// builds the sink. It starts nothing.
func New(
	cfg config.Sink, getenv func(string) string, clk clock.Clock, log *slog.Logger, opts ...Option,
) (*Sink, error) {
	if getenv == nil || clk == nil || log == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidConfig)
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	wh := cfg.Webhook
	secret := getenv(wh.SecretEnv)
	if secret == "" {
		return nil, fmt.Errorf("%w: environment variable %s is empty", ErrMissingSecret, wh.SecretEnv)
	}
	o := options{observer: nopObserver{}, ackObs: nopObserver{}}
	for _, opt := range opts {
		opt(&o)
	}
	s := &Sink{
		name:          cfg.Name,
		url:           wh.URL,
		secret:        []byte(secret),
		limits:        pipeline.BatchLimits{MaxEvents: cfg.MaxEvents, MaxBytes: int(cfg.MaxBytes)},
		linger:        cfg.Linger.Std(),
		maxEventBytes: orDefault(int(cfg.MaxEventBytes), DefaultMaxEventBytes),
		maxAttempts:   orDefault(wh.MaxAttempts, DefaultMaxAttempts),
		deadLetterDir: wh.DeadLetterDir,
		backoff:       o.backoff,
		clock:         clk,
		log:           log.With(slog.String("component", "webhooksink"), slog.String("queue", cfg.Name)),
		obs:           o.observer,
		ackObs:        o.ackObs,
		notify:        make(chan struct{}, 1),
		rnd:           o.rnd,
		space:         make(chan struct{}),
		health:        newHealthTracker(wh.Lanes),
	}
	if s.rnd == nil {
		s.rnd = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // G404: non-crypto jitter
	}
	capacity := o.capacity
	if capacity <= 0 {
		capacity = wh.Lanes * cfg.MaxEvents * queueFactor
	}
	sched, err := pipeline.NewLaneScheduler(wh.Lanes, capacity)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	s.sched = sched
	s.health.current.Since = clk.Now()
	s.client = newClient(orDefault(wh.Timeout.Std(), DefaultTimeout), wh.Lanes)
	return s, nil
}

func validate(cfg *config.Sink) error {
	var errs []error
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("%w: sink %q: "+format, append([]any{ErrInvalidConfig, cfg.Name}, args...)...))
	}
	if cfg.Name == "" {
		add("name is required")
	}
	if cfg.Webhook == nil {
		add("webhook section is required")
		return errors.Join(errs...)
	}
	if u, err := url.Parse(cfg.Webhook.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		add("webhook.url must be an http or https URL")
	}
	if cfg.Webhook.SecretEnv == "" {
		add("webhook.secret_env is required")
	}
	if cfg.Webhook.Lanes < 1 {
		add("webhook.lanes %d must be at least 1", cfg.Webhook.Lanes)
	}
	if cfg.Webhook.MaxAttempts < 0 || cfg.Webhook.Timeout < 0 {
		add("webhook.max_attempts and webhook.timeout must not be negative")
	}
	if cfg.MaxEvents < 1 {
		add("max_events %d must be at least 1", cfg.MaxEvents)
	}
	if cfg.MaxBytes < 0 || cfg.Linger < 0 || cfg.MaxEventBytes < 0 {
		add("max_bytes, linger and max_event_bytes must not be negative")
	}
	return errors.Join(errs...)
}

func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

func newClient(timeout time.Duration, lanes int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          lanes + 1,
			MaxIdleConnsPerHost:   lanes + 1,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   timeout,
			ExpectContinueTimeout: time.Second,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		},
		Timeout: timeout,
		// A redirect is a misconfigured URL: it is reported as a poison
		// status instead of being followed (a 301/302 would turn the POST
		// into a GET).
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Name implements pipeline.Sink.
func (s *Sink) Name() string { return s.name }

// Health implements pipeline.Sink.
func (s *Sink) Health() pipeline.SinkHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health.current
}

// Send implements pipeline.Sink. It queues rec behind every record sent
// before it and blocks while the queue holds its capacity (backpressure). A
// record above max_event_bytes is never queued: Send blocks with Health
// event_too_large until ctx ends, since only a configuration change on
// restart can fix it.
func (s *Sink) Send(ctx context.Context, rec pipeline.Record) error {
	if len(rec.Payload) > s.maxEventBytes {
		return s.blockTooLarge(ctx, &rec)
	}
	for {
		s.mu.Lock()
		empty := s.sched.Len() == 0
		err := s.sched.Push(rec)
		if err == nil {
			if empty {
				s.firstQueued = s.clock.Now()
			}
			s.queuedBytes += len(rec.Payload)
			s.mu.Unlock()
			s.wake()
			return nil
		}
		space := s.space
		s.mu.Unlock()
		if !errors.Is(err, pipeline.ErrLanesFull) {
			return fmt.Errorf("webhooksink %s: queue: %w", s.name, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("webhooksink %s: send: %w", s.name, ctx.Err())
		case <-space:
		}
	}
}

func (s *Sink) blockTooLarge(ctx context.Context, rec *pipeline.Record) error {
	reason := fmt.Sprintf("event %s of table %q (key %s, lsn %s) is %d bytes, above max_event_bytes %d",
		rec.ID, rec.Table(), truncate(rec.Key), rec.ID.LSN, len(rec.Payload), s.maxEventBytes)
	s.mu.Lock()
	s.health.addTooLarge(reason, s.clock.Now())
	s.mu.Unlock()
	s.log.LogAttrs(ctx, slog.LevelError, "event too large, sink blocked until restart with a fixed configuration",
		slog.String("operation", "send"), slog.String("status", pipeline.CodeEventTooLarge),
		slog.String("table", rec.Table()), slog.String("key", truncate(rec.Key)),
		slog.String("lsn", rec.ID.LSN.String()), slog.Int("bytes", len(rec.Payload)))
	<-ctx.Done()
	s.mu.Lock()
	s.health.removeTooLarge(s.clock.Now())
	s.mu.Unlock()
	return fmt.Errorf("webhooksink %s: %w: %w", s.name, ErrEventTooLarge, ctx.Err())
}

// wake nudges the dispatcher without blocking.
func (s *Sink) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Sink) jitter() float64 {
	s.randMu.Lock()
	defer s.randMu.Unlock()
	return s.rnd.Float64()
}

func (s *Sink) newDeliveryID() (string, error) {
	ms := ulid.Timestamp(s.clock.Now())
	id, err := ulid.New(ms, ulid.DefaultEntropy())
	if err != nil {
		return "", fmt.Errorf("webhooksink %s: delivery id: %w", s.name, err)
	}
	return id.String(), nil
}

// truncate renders a key for logs and health reasons, at most 256 bytes.
func truncate(key []byte) string {
	const limit = 256
	if key == nil {
		return "null"
	}
	if len(key) > limit {
		return string(key[:limit]) + "…"
	}
	return string(key)
}
