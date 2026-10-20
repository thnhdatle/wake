// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// Defaults of the sink (see the bounds documented in doc.go).
const (
	// DefaultMaxEventBytes is max_event_bytes when the config leaves it 0
	// (the broker's message.max.bytes default).
	DefaultMaxEventBytes = 1 << 20
	// DefaultMaxBufferedRecords bounds the events accepted by Send and not
	// yet acknowledged.
	DefaultMaxBufferedRecords = 10_000
	// DefaultMaxBufferedBytes bounds their event bytes.
	DefaultMaxBufferedBytes = 64 << 20
	// DefaultStallTimeout is how long records may wait for an
	// acknowledgement before Health reports retrying.
	DefaultStallTimeout = 10 * time.Second
	// DefaultRequestTimeout bounds one metadata or topic-creation request
	// and one connection attempt.
	DefaultRequestTimeout = 10 * time.Second
	// DefaultShutdownTimeout bounds the final flush when Run's context ends.
	DefaultShutdownTimeout = 10 * time.Second
)

const (
	unknownTopicRetries = 4
	// fanoutAllowance leaves franz-go room for `t` fan-out and tombstone
	// records beyond two records per buffered event.
	fanoutAllowance = 1 << 16
	minBatchBytes   = 1 << 10
	maxBatchBytes   = 100 << 20 // franz-go's default broker write limit
)

// Sink is the Kafka sink. Send, Health and Name are safe for concurrent
// use; Run is called once.
type Sink struct {
	name          string
	prefix        string
	tombstones    bool
	autoCreate    bool
	partitions    int32
	replication   int16
	maxEventBytes int64
	maxBatchBytes int64
	linger        time.Duration
	conn          connSettings

	maxRecords     int
	maxBytes       int64
	stallTimeout   time.Duration
	requestTimeout time.Duration
	backoff        pipeline.Backoff
	clock          clock.Clock
	log            *slog.Logger
	obs            Observer
	ackObs         AckObserver
	newClient      clientFactory
	running        atomic.Bool

	slots  chan struct{}       // one token per accepted event; capacity maxRecords
	bytes  *semaphore.Weighted // event bytes of accepted events
	queue  chan *unit          // accepted events not yet taken by the dispatcher; capacity maxRecords
	notify chan struct{}       // capacity 1: wakes the dispatcher after a failure or when idle

	randMu sync.Mutex // protects rnd
	rnd    *rand.Rand

	mu          sync.Mutex // protects every field below
	accepted    int        // events accepted by Send and not yet acknowledged
	inflight    int        // writes handed to franz-go and not yet completed
	recovering  int        // re-produced writes not yet completed
	failed      []*write   // failed writes waiting for re-production
	closing     bool       // Run is shutting down; failures are dropped
	topics      map[string]topicInfo
	progress    time.Time // last acknowledgement, or when inflight became non-zero
	lastConnErr error
	health      healthTracker
}

// Option configures a Sink.
type Option func(*options)

type options struct {
	observer       Observer
	ackObs         AckObserver
	rnd            *rand.Rand
	backoff        pipeline.Backoff
	maxRecords     int
	maxBytes       int64
	stallTimeout   time.Duration
	requestTimeout time.Duration
	newClient      clientFactory
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
// acknowledged, by count and by event bytes (defaults
// DefaultMaxBufferedRecords and DefaultMaxBufferedBytes). Send blocks at
// either bound. Non-positive values keep the default.
func WithMaxBuffered(records int, bytes int64) Option {
	return func(o *options) { o.maxRecords, o.maxBytes = records, bytes }
}

// WithStallTimeout overrides DefaultStallTimeout.
func WithStallTimeout(d time.Duration) Option { return func(o *options) { o.stallTimeout = d } }

// WithRequestTimeout overrides DefaultRequestTimeout.
func WithRequestTimeout(d time.Duration) Option { return func(o *options) { o.requestTimeout = d } }

// New validates cfg (a sink of type kafka), loads its TLS files and reads
// the SASL credentials from the environment variables it names through
// getenv. It starts nothing; Run connects.
func New(
	cfg config.Sink, getenv func(string) string, clk clock.Clock, log *slog.Logger, opts ...Option,
) (*Sink, error) {
	if getenv == nil || clk == nil || log == nil {
		return nil, fmt.Errorf("%w: nil dependency", ErrInvalidConfig)
	}
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	conn, err := loadConnSettings(cfg.Kafka, getenv)
	if err != nil {
		return nil, err
	}
	o := options{observer: nopObserver{}, ackObs: nopObserver{}, newClient: newKgoClient}
	for _, opt := range opts {
		opt(&o)
	}
	k := cfg.Kafka
	s := &Sink{
		name:           cfg.Name,
		prefix:         k.TopicPrefix,
		tombstones:     k.Tombstones,
		autoCreate:     k.AutoCreateTopics,
		partitions:     int32(k.Partitions),        //nolint:gosec // G115: validated small positive or -1
		replication:    int16(k.ReplicationFactor), //nolint:gosec // G115: validated small positive or -1
		maxEventBytes:  orDefault(cfg.MaxEventBytes.Int64(), DefaultMaxEventBytes),
		maxBatchBytes:  cfg.MaxBytes.Int64(),
		linger:         cfg.Linger.Std(),
		conn:           conn,
		maxRecords:     orDefault(o.maxRecords, DefaultMaxBufferedRecords),
		maxBytes:       orDefault(o.maxBytes, DefaultMaxBufferedBytes),
		stallTimeout:   orDefault(o.stallTimeout, DefaultStallTimeout),
		requestTimeout: orDefault(o.requestTimeout, DefaultRequestTimeout),
		backoff:        o.backoff,
		clock:          clk,
		log:            log.With(slog.String("component", "kafkasink"), slog.String("queue", cfg.Name)),
		obs:            o.observer,
		ackObs:         o.ackObs,
		newClient:      o.newClient,
		notify:         make(chan struct{}, 1),
		rnd:            o.rnd,
		topics:         make(map[string]topicInfo),
	}
	if s.rnd == nil {
		s.rnd = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())) //nolint:gosec // G404: non-crypto jitter
	}
	s.slots = make(chan struct{}, s.maxRecords)
	s.queue = make(chan *unit, s.maxRecords)
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
	if cfg.Type != "" && cfg.Type != config.SinkKafka {
		add("type %q is not kafka", cfg.Type)
	}
	k := cfg.Kafka
	if k == nil {
		add("kafka section is required")
		return errors.Join(errs...)
	}
	if len(k.Brokers) == 0 {
		add("kafka.brokers is required")
	}
	if !config.ValidIdentifier(k.TopicPrefix) {
		add("kafka.topic_prefix %q must match [A-Za-z0-9_-]+", k.TopicPrefix)
	}
	if !validLayout(k.Partitions, 1<<20) || !validLayout(k.ReplicationFactor, 1<<14) {
		add("kafka.partitions and kafka.replication_factor must be -1 or positive")
	}
	if cfg.MaxBytes < 0 || cfg.Linger < 0 || cfg.MaxEventBytes < 0 {
		add("max_bytes, linger and max_event_bytes must not be negative")
	}
	return errors.Join(errs...)
}

// validLayout accepts -1 (broker default) or 1..limit.
func validLayout(n, limit int) bool { return n == -1 || (n >= 1 && n <= limit) }

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
// events or event bytes (backpressure, see WithMaxBuffered). It returns an
// error only when ctx ends first.
func (s *Sink) Send(ctx context.Context, rec pipeline.Record) error {
	u := newUnit(rec, s.prefix)
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("kafkasink: send: %w", ctx.Err())
	}
	u.reserved = min(u.size, s.maxBytes)
	if err := s.bytes.Acquire(ctx, u.reserved); err != nil {
		<-s.slots
		return fmt.Errorf("kafkasink: send: %w", err)
	}
	s.mu.Lock()
	s.accepted++
	n := s.accepted
	s.mu.Unlock()
	s.obs.SetInflight(s.name, n)
	s.queue <- u // never blocks: the queue holds at most one event per slot
	return nil
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
