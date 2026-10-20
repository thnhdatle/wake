// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/semaphore"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// Defaults of the sink (see the bounds documented in doc.go).
const (
	// DefaultDuplicateWindow is nats.duplicate_window when the config
	// leaves it 0.
	DefaultDuplicateWindow = 2 * time.Minute
	// DefaultMaxBufferedRecords bounds the events accepted by Send and not
	// yet acknowledged; it is also the JetStream async publish limit.
	DefaultMaxBufferedRecords = 10_000
	// DefaultMaxBufferedBytes bounds their message bytes.
	DefaultMaxBufferedBytes = 64 << 20
	// DefaultAckTimeout is how long a published message waits for its
	// PubAck before it is re-published.
	DefaultAckTimeout = 10 * time.Second
	// DefaultStallTimeout is how long published messages may wait for a
	// PubAck, or the connection may be down, before Health reports it.
	DefaultStallTimeout = 10 * time.Second
	// DefaultRequestTimeout bounds one connection attempt and one stream
	// request.
	DefaultRequestTimeout = 10 * time.Second
	// DefaultShutdownTimeout bounds the wait for outstanding PubAcks when
	// Run's context ends.
	DefaultShutdownTimeout = 10 * time.Second
)

const (
	reconnectWait       = time.Second
	pingInterval        = 10 * time.Second
	maxPingsOutstanding = 3
)

// Sink is the NATS JetStream sink. Send, Health and Name are safe for
// concurrent use; Run is called once.
type Sink struct {
	name          string
	url           string
	prefix        string
	stream        string
	createStream  bool
	window        time.Duration
	credsFile     string
	tls           *tls.Config
	maxEventBytes int64 // 0: the server's max_payload

	maxRecords      int
	maxBytes        int64
	ackTimeout      time.Duration
	stallTimeout    time.Duration
	requestTimeout  time.Duration
	shutdownTimeout time.Duration
	backoff         pipeline.Backoff
	clock           clock.Clock
	log             *slog.Logger
	obs             Observer
	ackObs          AckObserver
	dial            dialer
	running         atomic.Bool

	slots   chan struct{}       // one token per accepted event; capacity maxRecords
	bytes   *semaphore.Weighted // message bytes of accepted events
	queue   chan *unit          // accepted events not yet taken by the dispatcher; capacity maxRecords
	pending chan pendingAck     // published events in publish order; capacity maxRecords
	notify  chan struct{}       // capacity 1: wakes the dispatcher after a failure or when idle

	randMu sync.Mutex // protects rnd
	rnd    *rand.Rand

	mu          sync.Mutex // protects every field below
	accepted    int        // events accepted by Send and not yet acknowledged
	inflight    int        // events published and not yet resolved
	recovering  int        // re-published events not yet resolved
	failed      []*unit    // failed events waiting for re-publication
	closing     bool       // Run is shutting down; failures are dropped
	streamReady bool       // the stream was checked or created on this run
	pubEpoch    uint64     // connection epoch of every event in flight
	progress    time.Time  // last PubAck, or when inflight became non-zero
	lastConnErr error
	health      healthTracker
}

// unit is one pipeline.Record from Send to acknowledgement.
type unit struct {
	rec      pipeline.Record
	seq      uint64 // dispatch order, assigned by the dispatcher
	id       string // Nats-Msg-Id
	subject  string
	size     int64 // payload and headers, as the server's max_payload counts them
	reserved int64 // bytes held in the buffer semaphore
	msg      *nats.Msg

	// Guarded by Sink.mu.
	sentAt time.Time
	retry  bool
}

// pendingAck pairs a published event with its PubAck future.
type pendingAck struct {
	u   *unit
	fut ackFuture
}

// Option configures a Sink.
type Option func(*options)

type options struct {
	observer        Observer
	ackObs          AckObserver
	rnd             *rand.Rand
	backoff         pipeline.Backoff
	maxRecords      int
	maxBytes        int64
	ackTimeout      time.Duration
	stallTimeout    time.Duration
	requestTimeout  time.Duration
	shutdownTimeout time.Duration
	dial            dialer
}

// WithObserver receives the sink's metrics.
func WithObserver(obs Observer) Option { return func(o *options) { o.observer = obs } }

// WithAckObserver receives the ts_ms of every acknowledged event.
func WithAckObserver(obs AckObserver) Option { return func(o *options) { o.ackObs = obs } }

// WithRand sets the seeded source of backoff jitter (default: randomly
// seeded).
func WithRand(r *rand.Rand) Option { return func(o *options) { o.rnd = r } }

// WithBackoff overrides the retry delays (default pipeline.Backoff{}:
// 100 ms doubling up to 30 s, equal jitter).
func WithBackoff(b pipeline.Backoff) Option { return func(o *options) { o.backoff = b } }

// WithMaxBuffered bounds the events accepted by Send and not yet
// acknowledged, by count and by message bytes (defaults
// DefaultMaxBufferedRecords and DefaultMaxBufferedBytes). Send blocks at
// either bound. Non-positive values keep the default.
func WithMaxBuffered(records int, bytes int64) Option {
	return func(o *options) { o.maxRecords, o.maxBytes = records, bytes }
}

// WithAckTimeout overrides DefaultAckTimeout.
func WithAckTimeout(d time.Duration) Option { return func(o *options) { o.ackTimeout = d } }

// WithStallTimeout overrides DefaultStallTimeout.
func WithStallTimeout(d time.Duration) Option { return func(o *options) { o.stallTimeout = d } }

// WithRequestTimeout overrides DefaultRequestTimeout.
func WithRequestTimeout(d time.Duration) Option { return func(o *options) { o.requestTimeout = d } }

// WithShutdownTimeout overrides DefaultShutdownTimeout.
func WithShutdownTimeout(d time.Duration) Option { return func(o *options) { o.shutdownTimeout = d } }

// New validates cfg (a sink of type nats) and loads its TLS files. It starts
// nothing; Run connects. nats.stream must be set (config defaults it to
// wake_<source.name>).
func New(cfg config.Sink, clk clock.Clock, log *slog.Logger, opts ...Option) (*Sink, error) {
	if clk == nil || log == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidConfig)
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	n := cfg.NATS
	o := options{observer: nopObserver{}, ackObs: nopObserver{}, dial: dialNATS}
	for _, opt := range opts {
		opt(&o)
	}
	s := &Sink{
		name:            cfg.Name,
		url:             n.URL,
		prefix:          n.SubjectPrefix,
		stream:          n.Stream,
		createStream:    n.CreateStream,
		window:          orDefault(n.DuplicateWindow.Std(), DefaultDuplicateWindow),
		credsFile:       n.CredentialsFile,
		maxEventBytes:   cfg.MaxEventBytes.Int64(),
		maxRecords:      orDefault(o.maxRecords, DefaultMaxBufferedRecords),
		maxBytes:        orDefault(o.maxBytes, DefaultMaxBufferedBytes),
		ackTimeout:      orDefault(o.ackTimeout, DefaultAckTimeout),
		stallTimeout:    orDefault(o.stallTimeout, DefaultStallTimeout),
		requestTimeout:  orDefault(o.requestTimeout, DefaultRequestTimeout),
		shutdownTimeout: orDefault(o.shutdownTimeout, DefaultShutdownTimeout),
		backoff:         o.backoff,
		clock:           clk,
		log:             log.With(slog.String("component", "natssink"), slog.String("queue", cfg.Name)),
		obs:             o.observer,
		ackObs:          o.ackObs,
		dial:            o.dial,
		notify:          make(chan struct{}, 1),
		rnd:             o.rnd,
	}
	if n.TLS.Enabled {
		t, err := loadTLS(n.TLS)
		if err != nil {
			return nil, err
		}
		s.tls = t
	}
	if s.rnd == nil {
		s.rnd = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // G404: non-crypto jitter
	}
	s.slots = make(chan struct{}, s.maxRecords)
	s.queue = make(chan *unit, s.maxRecords)
	s.pending = make(chan pendingAck, s.maxRecords)
	s.bytes = semaphore.NewWeighted(s.maxBytes)
	s.health.current.Since = clk.Now()
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
	if cfg.Type != "" && cfg.Type != config.SinkNATS {
		add("type %q is not nats", cfg.Type)
	}
	n := cfg.NATS
	if n == nil {
		add("nats section is required")
		return errors.Join(errs...)
	}
	if strings.TrimSpace(n.URL) == "" {
		add("nats.url is required")
	}
	if !config.ValidIdentifier(n.SubjectPrefix) {
		add("nats.subject_prefix %q must match [A-Za-z0-9_-]+", n.SubjectPrefix)
	}
	if n.Stream == "" || strings.ContainsAny(n.Stream, ".*> \t") {
		add("nats.stream %q must be a non-empty name without '.', '*', '>' or spaces", n.Stream)
	}
	if n.DuplicateWindow < 0 || cfg.MaxBytes < 0 || cfg.MaxEventBytes < 0 {
		add("nats.duplicate_window, max_bytes and max_event_bytes must not be negative")
	}
	return errors.Join(errs...)
}

func orDefault[T int | int64 | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
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
// before it and blocks while the sink holds its bound of unacknowledged
// events or message bytes (backpressure, see WithMaxBuffered). It returns an
// error only when ctx ends first.
func (s *Sink) Send(ctx context.Context, rec pipeline.Record) error {
	u := s.newUnit(rec)
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("natssink: send: %w", ctx.Err())
	}
	u.reserved = min(u.size, s.maxBytes)
	if err := s.bytes.Acquire(ctx, u.reserved); err != nil {
		<-s.slots
		return fmt.Errorf("natssink: send: %w", err)
	}
	s.mu.Lock()
	s.accepted++
	n := s.accepted
	s.mu.Unlock()
	s.obs.SetInflight(s.name, n)
	s.queue <- u // never blocks: the queue holds at most one event per slot
	return nil
}

// Header sizes of the NATS/1.0 header block the sink publishes: the status
// line, the blank line and ": " plus CRLF per header.
const (
	headerFraming  = len("NATS/1.0\r\n") + len("\r\n")
	perHeaderBytes = len(": \r\n")
)

func (s *Sink) newUnit(rec pipeline.Record) *unit {
	id := rec.ID.String()
	hdr := headerFraming + len(jetstream.MsgIDHeader) + perHeaderBytes + len(id) +
		len(jetstream.ExpectedStreamHeader) + perHeaderBytes + len(s.stream)
	return &unit{
		rec:     rec,
		id:      id,
		subject: s.prefix + "." + rec.Destination,
		size:    int64(len(rec.Payload) + hdr),
	}
}

// signal wakes the dispatcher without blocking.
func (s *Sink) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// delay returns the jittered backoff of retry attempt.
func (s *Sink) delay(attempt int) time.Duration {
	s.randMu.Lock()
	r := s.rnd.Float64()
	s.randMu.Unlock()
	return s.backoff.Delay(attempt, r)
}

// sleep waits d or until ctx ends; it reports whether ctx is still live.
func (s *Sink) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-s.clock.After(d):
		return true
	}
}

// noteConnErr remembers the connection's last error (nats.go callbacks).
func (s *Sink) noteConnErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.lastConnErr = err
	s.mu.Unlock()
}

func (s *Sink) logTransition(tr transition) {
	if !tr.changed {
		return
	}
	h := tr.health
	if h.IsHealthy() {
		s.log.Info("sink recovered", slog.String("status", h.State.String()))
		return
	}
	msg := h.Reason
	if h.Code != "" {
		msg = h.Code + ": " + msg
	}
	s.log.Warn("sink degraded", slog.String("status", h.State.String()), slog.String("error", msg))
}
