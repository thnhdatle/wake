// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// SASL mechanisms of KafkaConfig.SASL.
const (
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLScramSHA512 = "SCRAM-SHA-512"
)

// Kafka defaults.
const (
	// DefaultKafkaMaxBatch bounds the records handled per batch.
	DefaultKafkaMaxBatch = 500
	// maxTopicLength is Kafka's limit on topic names.
	maxTopicLength = 249
	// kafkaCommitTimeout bounds one offset commit.
	kafkaCommitTimeout = 10 * time.Second
	// kafkaMetadataMaxAge bounds how long a topic created after startup
	// stays unnoticed.
	kafkaMetadataMaxAge = 30 * time.Second
)

// KafkaConfig configures a KafkaSource; it mirrors the
// cache_invalidator.kafka section of the Wake configuration.
type KafkaConfig struct {
	// Brokers is the seed broker list (host:port).
	Brokers []string
	// TopicPrefix is the producer's topic prefix: the source consumes
	// "<TopicPrefix>.<schema>.<table>" for every mapped table.
	TopicPrefix string
	// Group is the consumer group.
	Group string
	// TLS enables TLS when non-nil.
	TLS *tls.Config
	// SASL enables SASL/SCRAM when its Mechanism is set.
	SASL SASL
	// MaxBatch bounds the records per batch (default DefaultKafkaMaxBatch).
	MaxBatch int
}

// SASL holds SASL/SCRAM credentials.
type SASL struct {
	// Mechanism is "" (disabled), SASLScramSHA256 or SASLScramSHA512.
	Mechanism string
	User      string
	Password  string
}

// kafkaClient is the part of *kgo.Client the source uses.
type kafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
	Ping(ctx context.Context) error
	Close()
}

// KafkaSource consumes CDC events from Kafka as a member of a consumer group.
// Records are handled in poll order, which keeps the order of each
// partition; offsets are committed (synchronously, after the handler
// returned nil) only for records that were handled. Rebalances are held back
// while a batch is in flight (BlockRebalanceOnPoll), so a commit never
// covers a partition the member no longer owns. Without a committed offset
// a partition is read from its start, so keys cached before the first run
// are invalidated too. Topics that do not exist yet are picked up within
// 30 s of their creation.
type KafkaSource struct {
	topics    []string
	kopts     []kgo.Opt
	maxBatch  int
	log       *slog.Logger
	newClient func(opts ...kgo.Opt) (kafkaClient, error)

	mu       sync.Mutex
	client   kafkaClient // non-nil while consuming; guarded by mu
	fetchErr error       // last fetch error, nil after a clean poll; guarded by mu
}

// NewKafkaSource validates cfg and returns a source for the topics of the
// mapped tables. It starts nothing; the client is created by Consume.
func NewKafkaSource(cfg KafkaConfig, mapping *Mapping, opts ...SourceOption) (*KafkaSource, error) {
	if mapping == nil {
		return nil, invalidConfig("kafka: nil mapping")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	topics := subjectNames(cfg.TopicPrefix, mapping)
	for _, t := range topics {
		if len(t) > maxTopicLength {
			return nil, invalidConfig("kafka: topic %s is longer than %d characters", t, maxTopicLength)
		}
	}
	kopts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("wake-cache-invalidator"),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMaxAge(kafkaMetadataMaxAge),
	}
	if cfg.TLS != nil {
		kopts = append(kopts, kgo.DialTLSConfig(cfg.TLS.Clone()))
	}
	if cfg.SASL.Mechanism != "" {
		auth := scram.Auth{User: cfg.SASL.User, Pass: cfg.SASL.Password}
		mech := auth.AsSha512Mechanism()
		if cfg.SASL.Mechanism == SASLScramSHA256 {
			mech = auth.AsSha256Mechanism()
		}
		kopts = append(kopts, kgo.SASL(mech))
	}
	batch := cfg.MaxBatch
	if batch == 0 {
		batch = DefaultKafkaMaxBatch
	}
	return &KafkaSource{
		topics:    topics,
		kopts:     kopts,
		maxBatch:  batch,
		log:       applySourceOptions("kafka", opts).log,
		newClient: newKgoClient,
	}, nil
}

func newKgoClient(opts ...kgo.Opt) (kafkaClient, error) {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cdcsource: kafka client: %w", err)
	}
	return cl, nil
}

func (c *KafkaConfig) validate() error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, invalidConfig("kafka: no brokers"))
	}
	if !identifierRE.MatchString(c.TopicPrefix) {
		errs = append(errs, invalidConfig("kafka: topic prefix %q must match [A-Za-z0-9_-]+", c.TopicPrefix))
	}
	if c.Group == "" {
		errs = append(errs, invalidConfig("kafka: empty consumer group"))
	}
	if c.MaxBatch < 0 {
		errs = append(errs, invalidConfig("kafka: negative max batch %d", c.MaxBatch))
	}
	switch c.SASL.Mechanism {
	case "":
	case SASLScramSHA256, SASLScramSHA512:
		if c.SASL.User == "" || c.SASL.Password == "" {
			errs = append(errs, invalidConfig("kafka: SASL %s needs a user and a password", c.SASL.Mechanism))
		}
	default:
		errs = append(errs, invalidConfig("kafka: unsupported SASL mechanism %q", c.SASL.Mechanism))
	}
	return errors.Join(errs...)
}

// Topics returns the consumed topics, sorted.
func (s *KafkaSource) Topics() []string { return append([]string(nil), s.topics...) }

// Consume implements Source. It returns nil when ctx ends and an error when
// the handler fails or the client cannot be created.
func (s *KafkaSource) Consume(ctx context.Context, handle Handler) error {
	cl, err := s.newClient(s.kopts...)
	if err != nil {
		return err
	}
	s.setClient(cl)
	defer func() {
		s.setClient(nil)
		cl.Close()
	}()
	s.log.Info("consuming kafka topics", "count", len(s.topics))
	for {
		fetches := cl.PollRecords(ctx, s.maxBatch)
		if ctx.Err() != nil {
			cl.AllowRebalance()
			return nil
		}
		if fetches.IsClientClosed() {
			return fmt.Errorf("cdcsource: kafka: %w", kgo.ErrClientClosed)
		}
		s.noteFetchErrors(fetches)
		if err := s.process(ctx, cl, fetches.Records(), handle); err != nil {
			return err
		}
	}
}

// process hands one polled batch to handle and commits it on success. The
// rebalance held back by the poll is allowed once the batch is done.
func (s *KafkaSource) process(ctx context.Context, cl kafkaClient, recs []*kgo.Record, handle Handler) error {
	defer cl.AllowRebalance()
	if len(recs) == 0 {
		return nil
	}
	payloads := make([][]byte, len(recs))
	for i, r := range recs {
		payloads[i] = r.Value
	}
	if err := handle(ctx, payloads); err != nil {
		return err
	}
	// The records are handled: commit even if shutdown started meanwhile.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), kafkaCommitTimeout)
	defer cancel()
	if err := cl.CommitRecords(cctx, recs...); err != nil {
		// The next commit covers these offsets; after a rebalance the new
		// owner handles them again (invalidation is idempotent).
		s.log.Warn("kafka offset commit failed", "error", err, "count", len(recs))
	}
	return nil
}

func (s *KafkaSource) noteFetchErrors(fetches kgo.Fetches) {
	var first error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		err = fmt.Errorf("cdcsource: kafka fetch %s[%d]: %w", topic, partition, err)
		s.log.Warn("kafka fetch failed", "error", err)
		if first == nil {
			first = err
		}
	})
	s.mu.Lock()
	s.fetchErr = first
	s.mu.Unlock()
}

func (s *KafkaSource) setClient(cl kafkaClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = cl
	s.fetchErr = nil
}

// Check reports whether the source is consuming, a broker answers and the
// last poll had no fetch error.
func (s *KafkaSource) Check(ctx context.Context) error {
	s.mu.Lock()
	cl, fetchErr := s.client, s.fetchErr
	s.mu.Unlock()
	if cl == nil {
		return fmt.Errorf("%w: kafka", ErrSourceNotRunning)
	}
	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("cdcsource: kafka: no broker reachable: %w", err)
	}
	return fetchErr
}
