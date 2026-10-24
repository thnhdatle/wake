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

const (
	DefaultMaxEventBytes      = 1 << 20
	DefaultMaxBufferedRecords = 10_000
	DefaultMaxBufferedBytes   = 64 << 20
	DefaultStallTimeout       = 10 * time.Second
	DefaultRequestTimeout     = 10 * time.Second
	DefaultShutdownTimeout    = 10 * time.Second
)

const (
	unknownTopicRetries = 4
	fanoutAllowance     = 1 << 16
	minBatchBytes       = 1 << 10
	maxBatchBytes       = 100 << 20
)

// Sink writes events to Kafka and acknowledges them once Kafka does.
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

	slots  chan struct{}
	bytes  *semaphore.Weighted
	queue  chan *unit
	notify chan struct{}

	randMu sync.Mutex
	rnd    *rand.Rand

	mu          sync.Mutex
	accepted    int
	inflight    int
	recovering  int
	failed      []*write
	closing     bool
	topics      map[string]topicInfo
	progress    time.Time
	lastConnErr error
	health      healthTracker
}

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

func WithObserver(obs Observer) Option { return func(o *options) { o.observer = obs } }

func WithAckObserver(obs AckObserver) Option { return func(o *options) { o.ackObs = obs } }

func WithRand(r *rand.Rand) Option { return func(o *options) { o.rnd = r } }

func WithBackoff(b pipeline.Backoff) Option { return func(o *options) { o.backoff = b } }

func WithMaxBuffered(records int, bytes int64) Option {
	return func(o *options) { o.maxRecords, o.maxBytes = records, bytes }
}

func WithStallTimeout(d time.Duration) Option { return func(o *options) { o.stallTimeout = d } }

func WithRequestTimeout(d time.Duration) Option { return func(o *options) { o.requestTimeout = d } }

// New returns a Kafka Sink for the given config.
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

func validLayout(n, limit int) bool { return n == -1 || (n >= 1 && n <= limit) }

func orDefault[T int | int64 | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

func (s *Sink) Name() string { return s.name }

func (s *Sink) Health() pipeline.SinkHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health.current
}

// Send queues one record for delivery and blocks while the buffer is full.
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
	s.queue <- u
	return nil
}

func (s *Sink) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Sink) delay(attempt int) time.Duration {
	s.randMu.Lock()
	r := s.rnd.Float64()
	s.randMu.Unlock()
	return s.backoff.Delay(attempt, r)
}

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
