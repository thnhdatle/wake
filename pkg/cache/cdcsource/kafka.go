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

// SASL mechanisms.
const (
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLScramSHA512 = "SCRAM-SHA-512"
)

// Kafka defaults.
const (
	DefaultKafkaMaxBatch = 500
	maxTopicLength       = 249
	kafkaCommitTimeout   = 10 * time.Second
	kafkaMetadataMaxAge  = 30 * time.Second
)

// KafkaConfig configures a KafkaSource.
type KafkaConfig struct {
	Brokers []string
	// TopicPrefix is the producer's topic prefix.
	TopicPrefix string
	Group       string
	TLS         *tls.Config
	SASL        SASL
	// MaxBatch bounds the records per batch, DefaultKafkaMaxBatch by default.
	MaxBatch int
}

// SASL holds SASL/SCRAM credentials.
type SASL struct {
	// Mechanism selects the SCRAM variant. Empty disables SASL.
	Mechanism string
	User      string
	Password  string
}

type kafkaClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	AllowRebalance()
	Ping(ctx context.Context) error
	Close()
}

// KafkaSource consumes CDC events from Kafka as a member of a consumer group.
type KafkaSource struct {
	topics    []string
	kopts     []kgo.Opt
	maxBatch  int
	log       *slog.Logger
	newClient func(opts ...kgo.Opt) (kafkaClient, error)

	mu       sync.Mutex
	client   kafkaClient
	fetchErr error
}

// NewKafkaSource validates cfg and returns a source for the topics of the mapped tables.
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

// Consume implements Source.
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
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), kafkaCommitTimeout)
	defer cancel()
	if err := cl.CommitRecords(cctx, recs...); err != nil {
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

// Check reports whether the source is consuming and a broker answers.
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
