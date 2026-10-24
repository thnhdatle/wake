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

type client interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
	describeTopic(ctx context.Context, topic string) (topicInfo, error)
	createTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16) error
}

type topicInfo struct {
	exists          bool
	partitions      int
	maxMessageBytes int64
}

type clientFactory func(opts []kgo.Opt) (client, error)

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

type brokerHook struct{ s *Sink }

func (h brokerHook) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	h.s.mu.Lock()
	h.s.lastConnErr = err
	h.s.mu.Unlock()
}
