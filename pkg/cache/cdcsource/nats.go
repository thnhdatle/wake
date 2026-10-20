// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATS defaults.
const (
	// DefaultNATSMaxBatch bounds the messages handled per batch.
	DefaultNATSMaxBatch = 256
	// DefaultNATSAckWait is the consumer's ack wait. Messages of a batch whose
	// handling takes longer are kept in progress, not redelivered.
	DefaultNATSAckWait = 30 * time.Second
	// natsFetchWait bounds one wait for the first message of a batch.
	natsFetchWait = 5 * time.Second
	// natsRequestTimeout bounds JetStream API requests.
	natsRequestTimeout = 10 * time.Second
)

// NATSConfig configures a NATSSource; it mirrors the cache_invalidator.nats
// section of the Wake configuration.
type NATSConfig struct {
	// URL is the server URL (nats://host:port, several separated by commas).
	URL string
	// SubjectPrefix is the producer's subject prefix: the consumer filters
	// "<SubjectPrefix>.<schema>.<table>" for every mapped table.
	SubjectPrefix string
	// Stream is the JetStream stream holding the events.
	Stream string
	// Durable is the durable consumer name, created or updated by Consume.
	Durable string
	// CredentialsFile is a user credentials file (optional).
	CredentialsFile string
	// TLS enables TLS when non-nil.
	TLS *tls.Config
	// AckWait is the consumer's ack wait (default DefaultNATSAckWait).
	AckWait time.Duration
	// MaxBatch bounds the messages per batch (default DefaultNATSMaxBatch).
	MaxBatch int
}

// natsConn is the part of a NATS connection the source uses.
type natsConn interface {
	consumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (natsConsumer, error)
	connected() bool
	close()
}

// natsConsumer is the part of jetstream.Consumer the source uses.
type natsConsumer interface {
	Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error)
	FetchNoWait(batch int) (jetstream.MessageBatch, error)
}

// NATSSource consumes CDC events from a JetStream durable pull consumer with
// explicit acks. Consume creates or updates the consumer (filter subjects
// derived from the mapping, deliver policy all, so keys cached before the
// first run are invalidated too). A message is acknowledged only after the
// handler returned nil; while a batch is retried its messages are kept in
// progress, and when Consume stops before handling them they are negatively
// acknowledged so they are redelivered at once.
type NATSSource struct {
	cfg      NATSConfig
	subjects []string
	maxBatch int
	ackWait  time.Duration
	log      *slog.Logger
	dial     func(cfg *NATSConfig) (natsConn, error)
	// retryMin and retryMax bound the delays after transport errors.
	retryMin, retryMax time.Duration

	mu      sync.Mutex
	conn    natsConn // non-nil while consuming; guarded by mu
	lastErr error    // last transport error, nil after a clean fetch; guarded by mu
}

// NewNATSSource validates cfg and returns a source for the subjects of the
// mapped tables. It starts nothing; the connection is made by Consume.
func NewNATSSource(cfg NATSConfig, mapping *Mapping, opts ...SourceOption) (*NATSSource, error) {
	if mapping == nil {
		return nil, invalidConfig("nats: nil mapping")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &NATSSource{
		cfg:      cfg,
		subjects: subjectNames(cfg.SubjectPrefix, mapping),
		maxBatch: cfg.MaxBatch,
		ackWait:  cfg.AckWait,
		log:      applySourceOptions("nats", opts).log,
		dial:     dialNATS,
		retryMin: transportRetryMin,
		retryMax: transportRetryMax,
	}
	if s.maxBatch == 0 {
		s.maxBatch = DefaultNATSMaxBatch
	}
	if s.ackWait == 0 {
		s.ackWait = DefaultNATSAckWait
	}
	return s, nil
}

func (c *NATSConfig) validate() error {
	var errs []error
	if c.URL == "" {
		errs = append(errs, invalidConfig("nats: empty URL"))
	}
	if !identifierRE.MatchString(c.SubjectPrefix) {
		errs = append(errs, invalidConfig("nats: subject prefix %q must match [A-Za-z0-9_-]+", c.SubjectPrefix))
	}
	for _, n := range [...]struct{ what, name string }{{"stream", c.Stream}, {"durable", c.Durable}} {
		if n.name == "" || strings.ContainsAny(n.name, ".*> \t\r\n/\\") {
			errs = append(errs, invalidConfig("nats: %s name %q must be non-empty without . * > / \\ or white space",
				n.what, n.name))
		}
	}
	if c.AckWait < 0 || c.MaxBatch < 0 {
		errs = append(errs, invalidConfig("nats: negative ack wait %s or max batch %d", c.AckWait, c.MaxBatch))
	}
	return errors.Join(errs...)
}

// Subjects returns the filter subjects, sorted.
func (s *NATSSource) Subjects() []string { return append([]string(nil), s.subjects...) }

// Consume implements Source. It returns nil when ctx ends and an error when
// the handler fails or the connection cannot be set up.
func (s *NATSSource) Consume(ctx context.Context, handle Handler) error {
	conn, err := s.dial(&s.cfg)
	if err != nil {
		return err
	}
	s.setConn(conn)
	defer func() {
		s.setConn(nil)
		conn.close()
	}()
	s.log.Info("consuming nats subjects", "count", len(s.subjects))
	for attempt := 0; ctx.Err() == nil; {
		cons, err := s.openConsumer(ctx, conn)
		if err != nil {
			return nil //nolint:nilerr // openConsumer fails only when ctx ended
		}
		for ctx.Err() == nil {
			msgs, err := s.fetch(ctx, cons)
			if err != nil {
				s.noteErr(err)
				s.log.Warn("nats fetch failed", "error", err, "attempt", attempt+1)
				sleepCtx(ctx, retryDelay(attempt, s.retryMin, s.retryMax))
				attempt++
				break // re-create the consumer: it may have been deleted
			}
			attempt = 0
			s.noteErr(nil)
			if err := s.process(ctx, msgs, handle); err != nil {
				return err
			}
		}
	}
	return nil
}

// openConsumer creates or updates the durable consumer, retrying until it
// succeeds or ctx ends.
func (s *NATSSource) openConsumer(ctx context.Context, conn natsConn) (natsConsumer, error) {
	cfg := jetstream.ConsumerConfig{
		Durable:        s.cfg.Durable,
		FilterSubjects: s.subjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        s.ackWait,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     -1,
		MaxAckPending:  2 * s.maxBatch,
	}
	for attempt := 0; ; attempt++ {
		rctx, cancel := context.WithTimeout(ctx, natsRequestTimeout)
		cons, err := conn.consumer(rctx, s.cfg.Stream, cfg)
		cancel()
		if err == nil {
			s.noteErr(nil)
			return cons, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		err = fmt.Errorf("cdcsource: nats consumer %s on stream %s: %w", s.cfg.Durable, s.cfg.Stream, err)
		s.noteErr(err)
		s.log.Warn("nats consumer setup failed", "error", err, "attempt", attempt+1)
		if !sleepCtx(ctx, retryDelay(attempt, s.retryMin, s.retryMax)) {
			return nil, ctx.Err()
		}
	}
}

// fetch waits up to natsFetchWait for one message, then takes whatever else
// is available at once, up to maxBatch messages in total. It returns no
// messages and no error when nothing arrived.
func (s *NATSSource) fetch(ctx context.Context, cons natsConsumer) ([]jetstream.Msg, error) {
	fctx, cancel := context.WithTimeout(ctx, natsFetchWait)
	defer cancel()
	b, err := cons.Fetch(1, jetstream.FetchContext(fctx))
	if err != nil {
		return nil, fmt.Errorf("cdcsource: nats fetch: %w", err)
	}
	msgs, err := drain(b)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return nil, fmt.Errorf("cdcsource: nats fetch: %w", err)
	}
	if len(msgs) == 0 || s.maxBatch == 1 {
		return msgs, nil
	}
	b, err = cons.FetchNoWait(s.maxBatch - 1)
	if err != nil {
		return msgs, nil //nolint:nilerr // the first message is handled; the next fetch reports the error
	}
	more, _ := drain(b) // an error ends the batch early; the next fetch reports it
	return append(msgs, more...), nil
}

func drain(b jetstream.MessageBatch) ([]jetstream.Msg, error) {
	var msgs []jetstream.Msg
	for m := range b.Messages() {
		msgs = append(msgs, m)
	}
	return msgs, b.Error()
}

// process hands one batch to handle, keeping its messages in progress
// meanwhile, then acknowledges them; on failure it naks them.
func (s *NATSSource) process(ctx context.Context, msgs []jetstream.Msg, handle Handler) error {
	if len(msgs) == 0 {
		return nil
	}
	payloads := make([][]byte, len(msgs))
	for i, m := range msgs {
		payloads[i] = m.Data()
	}
	stop := s.keepInProgress(msgs)
	err := handle(ctx, payloads)
	stop()
	if err != nil {
		for _, m := range msgs {
			_ = m.Nak() // best effort: without it the server redelivers after AckWait
		}
		return err
	}
	for _, m := range msgs {
		if err := m.Ack(); err != nil {
			// Redelivered after AckWait and invalidated again (idempotent).
			s.log.Warn("nats ack failed", "error", err)
		}
	}
	return nil
}

// keepInProgress resets the ack timer of msgs every third of AckWait until
// the returned function is called, so a batch whose Delete is retried for a
// long time is not redelivered meanwhile.
func (s *NATSSource) keepInProgress(msgs []jetstream.Msg) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(s.ackWait / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				for _, m := range msgs {
					_ = m.InProgress() // best effort; a lost one only risks a duplicate
				}
			}
		}
	})
	return func() {
		close(done)
		wg.Wait()
	}
}

func (s *NATSSource) setConn(c natsConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = c
	s.lastErr = nil
}

func (s *NATSSource) noteErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErr = err
}

// Check reports whether the source is consuming, connected and its last
// JetStream request succeeded.
func (s *NATSSource) Check(context.Context) error {
	s.mu.Lock()
	conn, lastErr := s.conn, s.lastErr
	s.mu.Unlock()
	switch {
	case conn == nil:
		return fmt.Errorf("%w: nats", ErrSourceNotRunning)
	case !conn.connected():
		return errors.New("cdcsource: nats: not connected")
	}
	return lastErr
}

// liveNATS is the production natsConn.
type liveNATS struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// dialNATS connects with unlimited reconnects and retries on a failed first
// connect, so it returns before the server is reachable.
func dialNATS(cfg *NATSConfig) (natsConn, error) {
	opts := []nats.Option{
		nats.Name("wake-cache-invalidator"),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
		nats.Timeout(natsRequestTimeout),
	}
	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}
	if cfg.TLS != nil {
		opts = append(opts, nats.Secure(cfg.TLS.Clone()))
	}
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("cdcsource: nats connect: %w", err)
	}
	js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(natsRequestTimeout))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("cdcsource: jetstream: %w", err)
	}
	return &liveNATS{nc: nc, js: js}, nil
}

func (c *liveNATS) consumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (natsConsumer, error) {
	cons, err := c.js.CreateOrUpdateConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("create or update consumer: %w", err)
	}
	return cons, nil
}

func (c *liveNATS) connected() bool { return c.nc.Status() == nats.CONNECTED }

func (c *liveNATS) close() { c.nc.Close() }
