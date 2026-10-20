// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/thnhdatle/wake/internal/config"
)

// client is the part of franz-go the sink uses; unit tests replace it.
type client interface {
	// Produce, Flush and Close are kgo.Client's.
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
	// describeTopic reads the topic's partition count and
	// max.message.bytes; exists is false for an unknown topic.
	describeTopic(ctx context.Context, topic string) (topicInfo, error)
	// createTopic creates the topic; -1 selects the broker default.
	createTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16) error
}

// topicInfo is what the sink knows about a topic.
type topicInfo struct {
	exists     bool
	partitions int
	// maxMessageBytes is the topic's effective max.message.bytes; 0 when
	// the principal may not describe topic configs.
	maxMessageBytes int64
}

// clientFactory builds a client from franz-go options.
type clientFactory func(opts []kgo.Opt) (client, error)

// kgoClient is the production client: one kgo.Client for producing and a
// kadm.Client over it for metadata and topic creation.
type kgoClient struct {
	*kgo.Client
	adm *kadm.Client
}

func newKgoClient(opts []kgo.Opt) (client, error) {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkasink: create client: %w", err)
	}
	return &kgoClient{Client: cl, adm: kadm.NewClient(cl)}, nil
}

// Flush waits for every buffered record.
func (c *kgoClient) Flush(ctx context.Context) error {
	if err := c.Client.Flush(ctx); err != nil {
		return fmt.Errorf("kafkasink: flush: %w", err)
	}
	return nil
}

func (c *kgoClient) describeTopic(ctx context.Context, topic string) (topicInfo, error) {
	details, err := c.adm.ListTopics(ctx, topic)
	if err != nil {
		return topicInfo{}, fmt.Errorf("kafkasink: metadata of topic %s: %w", topic, err)
	}
	d, ok := details[topic]
	if !ok || errors.Is(d.Err, kerr.UnknownTopicOrPartition) {
		return topicInfo{}, nil
	}
	if d.Err != nil {
		return topicInfo{}, fmt.Errorf("kafkasink: metadata of topic %s: %w", topic, d.Err)
	}
	info := topicInfo{exists: true, partitions: len(d.Partitions)}
	limit, err := c.maxMessageBytes(ctx, topic)
	if err != nil {
		return topicInfo{}, err
	}
	info.maxMessageBytes = limit
	return info, nil
}

// maxMessageBytes reads the topic's max.message.bytes; a principal without
// DescribeConfigs permission yields 0 (unknown) instead of an error.
func (c *kgoClient) maxMessageBytes(ctx context.Context, topic string) (int64, error) {
	rc, err := c.topicConfig(ctx, topic)
	if errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.ClusterAuthorizationFailed) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("kafkasink: configs of topic %s: %w", topic, err)
	}
	for _, cfg := range rc.Configs {
		if cfg.Key != "max.message.bytes" || cfg.Value == nil {
			continue
		}
		if n, perr := strconv.ParseInt(*cfg.Value, 10, 64); perr == nil {
			return n, nil
		}
	}
	return 0, nil
}

func (c *kgoClient) topicConfig(ctx context.Context, topic string) (kadm.ResourceConfig, error) {
	rcs, err := c.adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return kadm.ResourceConfig{}, err //nolint:wrapcheck // wrapped by the caller
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return kadm.ResourceConfig{}, err //nolint:wrapcheck // wrapped by the caller
	}
	return rc, rc.Err
}

func (c *kgoClient) createTopic(ctx context.Context, topic string, partitions int32, rf int16) error {
	_, err := c.adm.CreateTopic(ctx, partitions, rf, nil, topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("kafkasink: create topic %s: %w", topic, err)
	}
	return nil
}

// connSettings are the connection options New reads from the config and
// the environment.
type connSettings struct {
	brokers []string
	tls     *tls.Config
	sasl    sasl.Mechanism
}

func loadConnSettings(k *config.KafkaSink, getenv func(string) string) (connSettings, error) {
	cs := connSettings{brokers: k.Brokers}
	if k.TLS.Enabled {
		t, err := loadTLS(k.TLS)
		if err != nil {
			return connSettings{}, err
		}
		cs.tls = t
	}
	mech := k.SASL.Mechanism
	switch mech {
	case config.SASLNone:
		return cs, nil
	case config.SASLScramSHA256, config.SASLScramSHA512:
	default:
		return connSettings{}, fmt.Errorf("%w: unsupported kafka.sasl.mechanism %q", ErrInvalidConfig, mech)
	}
	user, pass := getenv(k.SASL.UsernameEnv), getenv(k.SASL.PasswordEnv)
	if user == "" || pass == "" {
		return connSettings{}, fmt.Errorf("%w: environment variables %s and %s must be set",
			ErrMissingCredentials, k.SASL.UsernameEnv, k.SASL.PasswordEnv)
	}
	auth := scram.Auth{User: user, Pass: pass}
	cs.sasl = auth.AsSha512Mechanism()
	if mech == config.SASLScramSHA256 {
		cs.sasl = auth.AsSha256Mechanism()
	}
	return cs, nil
}

// loadTLS builds a client TLS configuration from PEM files: ca_file replaces
// the system roots, cert_file and key_file present a client certificate.
func loadTLS(c config.TLS) (*tls.Config, error) {
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%w: kafka.tls.ca_file: %w", ErrInvalidConfig, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%w: kafka.tls.ca_file %s holds no PEM certificate", ErrInvalidConfig, c.CAFile)
		}
		out.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%w: kafka.tls cert_file/key_file: %w", ErrInvalidConfig, err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	return out, nil
}

// kgoOptions returns the franz-go options of the sink: idempotent
// producer (the franz-go default, which keeps up to five produce requests in
// flight per broker without reordering), acks=all, unlimited record retries,
// no delivery timeout and Kafka-compatible key hashing.
func (s *Sink) kgoOptions() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.conn.brokers...),
		kgo.ClientID("wake-" + s.name),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(math.MaxInt),
		kgo.RecordDeliveryTimeout(0),
		kgo.UnknownTopicRetries(unknownTopicRetries),
		kgo.ProducerLinger(s.linger),
		kgo.ProducerBatchMaxBytesFn(s.batchMaxBytes),
		kgo.MaxBufferedRecords(2*s.maxRecords + fanoutAllowance),
		kgo.RecordPartitioner(newPartitioner()),
		kgo.WithHooks(brokerHook{s: s}),
		kgo.DialTimeout(s.requestTimeout),
	}
	if s.conn.tls != nil {
		opts = append(opts, kgo.DialTLSConfig(s.conn.tls.Clone()))
	}
	if s.conn.sasl != nil {
		opts = append(opts, kgo.SASL(s.conn.sasl))
	}
	return opts
}

// batchMaxBytes caps franz-go batches per topic: max_bytes, raised so that
// one event of max_event_bytes fits, and lowered to the topic's
// max.message.bytes once known, so a batch of small records never trips the
// broker limit.
func (s *Sink) batchMaxBytes(topic string) int32 {
	limit := max(s.maxBatchBytes, s.maxEventBytes+batchOverhead)
	if topic != "" {
		s.mu.Lock()
		info, ok := s.topics[topic]
		s.mu.Unlock()
		if ok && info.maxMessageBytes > 0 {
			limit = min(limit, info.maxMessageBytes)
		}
	}
	return int32(min(max(limit, minBatchBytes), maxBatchBytes)) //nolint:gosec // G115: clamped to int32 range
}

// brokerHook remembers the last connection error for the stall reason.
type brokerHook struct{ s *Sink }

// OnBrokerConnect implements kgo.HookBrokerConnect.
func (h brokerHook) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	h.s.mu.Lock()
	h.s.lastConnErr = err
	h.s.mu.Unlock()
}
