// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKafka struct {
	mu        sync.Mutex
	polls     []kgo.Fetches
	commits   [][]*kgo.Record
	commitErr error
	pingErr   error
	allowed   int
	closed    bool
	idle      chan struct{}
	idleOnce  sync.Once
}

func newFakeKafka(polls ...kgo.Fetches) *fakeKafka {
	return &fakeKafka{polls: polls, idle: make(chan struct{})}
}

func (f *fakeKafka) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	f.mu.Lock()
	if len(f.polls) > 0 {
		p := f.polls[0]
		f.polls = f.polls[1:]
		f.mu.Unlock()
		return p
	}
	f.mu.Unlock()
	f.idleOnce.Do(func() { close(f.idle) })
	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (f *fakeKafka) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, rs)
	return f.commitErr
}

func (f *fakeKafka) AllowRebalance() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.allowed++
}

func (f *fakeKafka) Ping(context.Context) error { return f.pingErr }

func (f *fakeKafka) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeKafka) state() (commits [][]*kgo.Record, allowed int, closed bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits, f.allowed, f.closed
}

func fetchOf(partition int32, err error, values ...[]byte) kgo.Fetches {
	const topic = "cdc.public.users"
	recs := make([]*kgo.Record, len(values))
	for i, v := range values {
		recs[i] = &kgo.Record{Topic: topic, Partition: partition, Offset: int64(i), Value: v}
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      topic,
		Partitions: []kgo.FetchPartition{{Partition: partition, Records: recs, Err: err}},
	}}}}
}

func kafkaConfig() KafkaConfig {
	return KafkaConfig{Brokers: []string{"127.0.0.1:9092"}, TopicPrefix: "cdc", Group: "g"}
}

func newTestKafkaSource(t *testing.T, cl *fakeKafka) *KafkaSource {
	t.Helper()
	s, err := NewKafkaSource(kafkaConfig(), mustMapping(t, usersMapping))
	require.NoError(t, err)
	s.newClient = func(...kgo.Opt) (kafkaClient, error) { return cl, nil }
	return s
}

type recorder struct {
	mu      sync.Mutex
	batches [][][]byte
	err     error
}

func (r *recorder) handle(_ context.Context, payloads [][]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, payloads)
	return r.err
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.batches)
}

func TestNewKafkaSource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*KafkaConfig)
		want   string
	}{
		{"no brokers", func(c *KafkaConfig) { c.Brokers = nil }, "no brokers"},
		{"bad prefix", func(c *KafkaConfig) { c.TopicPrefix = "a.b" }, "topic prefix"},
		{"no group", func(c *KafkaConfig) { c.Group = "" }, "empty consumer group"},
		{"negative batch", func(c *KafkaConfig) { c.MaxBatch = -1 }, "negative max batch"},
		{"bad mechanism", func(c *KafkaConfig) { c.SASL.Mechanism = "PLAIN" }, "unsupported SASL mechanism"},
		{"missing password", func(c *KafkaConfig) {
			c.SASL = SASL{Mechanism: SASLScramSHA256, User: "u"}
		}, "needs a user and a password"},
		{"topic too long", func(c *KafkaConfig) { c.TopicPrefix = strings.Repeat("p", 240) }, "longer than 249"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := kafkaConfig()
			tc.mutate(&cfg)

			s, err := NewKafkaSource(cfg, mustMapping(t, usersMapping))

			require.ErrorIs(t, err, ErrInvalidConfig)
			assert.Contains(t, err.Error(), tc.want)
			assert.Nil(t, s)
		})
	}

	_, err := NewKafkaSource(kafkaConfig(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNewKafkaSource_Topics(t *testing.T) {
	t.Parallel()
	m := mustMapping(t, map[string][]string{"public.users": {"u:{id}"}, "app.orders": {"o:{id}"}})

	s, err := NewKafkaSource(kafkaConfig(), m)

	require.NoError(t, err)
	assert.Equal(t, []string{"cdc.app.orders", "cdc.public.users"}, s.Topics())
	assert.Equal(t, DefaultKafkaMaxBatch, s.maxBatch)
}

func TestKafkaSource_Consume_CommitsAfterHandler(t *testing.T) {
	t.Parallel()
	payload := evt("c", "public.users", `{"id":1}`, "null", `{"id":1}`)
	cl := newFakeKafka(
		fetchOf(0, nil, payload, nil),
		fetchOf(1, nil),
	)
	s := newTestKafkaSource(t, cl)
	var rec recorder
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- s.Consume(ctx, rec.handle) }()
	<-cl.idle
	require.NoError(t, s.Check(t.Context()))
	cancel()

	require.NoError(t, <-done)
	commits, allowed, closed := cl.state()
	require.Len(t, commits, 1)
	assert.Len(t, commits[0], 2, "the whole batch, tombstone included")
	assert.Equal(t, 3, allowed, "every poll releases the rebalance")
	assert.True(t, closed)
	require.Equal(t, 1, rec.count())
	assert.Equal(t, [][]byte{payload, nil}, rec.batches[0])
	require.ErrorIs(t, s.Check(t.Context()), ErrSourceNotRunning)
}

func TestKafkaSource_Consume_HandlerErrorSkipsCommit(t *testing.T) {
	t.Parallel()
	cl := newFakeKafka(fetchOf(0, nil, []byte("x")))
	s := newTestKafkaSource(t, cl)
	rec := recorder{err: context.Canceled}

	err := s.Consume(t.Context(), rec.handle)

	require.ErrorIs(t, err, context.Canceled)
	commits, allowed, _ := cl.state()
	assert.Empty(t, commits)
	assert.Equal(t, 1, allowed)
}

func TestKafkaSource_Consume_CommitErrorContinues(t *testing.T) {
	t.Parallel()
	cl := newFakeKafka(fetchOf(0, nil, []byte("a")), fetchOf(0, nil, []byte("b")))
	cl.commitErr = errors.New("rebalance in progress")
	s := newTestKafkaSource(t, cl)
	var rec recorder
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- s.Consume(ctx, rec.handle) }()
	<-cl.idle
	cancel()

	require.NoError(t, <-done)
	commits, _, _ := cl.state()
	assert.Len(t, commits, 2)
	assert.Equal(t, 2, rec.count())
}

func TestKafkaSource_Check_FetchErrors(t *testing.T) {
	t.Parallel()
	cl := newFakeKafka(fetchOf(3, kerr.UnknownTopicOrPartition))
	cl.pingErr = nil
	s := newTestKafkaSource(t, cl)
	var rec recorder
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- s.Consume(ctx, rec.handle) }()
	<-cl.idle

	err := s.Check(t.Context())
	require.ErrorIs(t, err, kerr.UnknownTopicOrPartition)
	assert.Contains(t, err.Error(), "cdc.public.users[3]")
	cl.mu.Lock()
	cl.pingErr = errors.New("dial refused")
	cl.mu.Unlock()
	require.ErrorContains(t, s.Check(t.Context()), "no broker reachable")
	cancel()
	require.NoError(t, <-done)
	assert.Zero(t, rec.count())
}

func TestKafkaSource_Consume_ClientClosed(t *testing.T) {
	t.Parallel()
	cl := newFakeKafka(kgo.NewErrFetch(kgo.ErrClientClosed))
	s := newTestKafkaSource(t, cl)

	err := s.Consume(t.Context(), (&recorder{}).handle)

	require.ErrorIs(t, err, kgo.ErrClientClosed)
}

func TestKafkaSource_Consume_ClientError(t *testing.T) {
	t.Parallel()
	s := newTestKafkaSource(t, nil)
	s.newClient = func(...kgo.Opt) (kafkaClient, error) { return nil, errors.New("bad options") }

	require.EqualError(t, s.Consume(t.Context(), (&recorder{}).handle), "bad options")
}

func TestKafkaSource_Consume_RealClientUnreachable(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	for _, mech := range []string{SASLScramSHA256, SASLScramSHA512} {
		cfg := kafkaConfig()
		cfg.Brokers = []string{addr}
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		cfg.SASL = SASL{Mechanism: mech, User: "u", Password: "p"}
		cfg.MaxBatch = 10
		s, err := NewKafkaSource(cfg, mustMapping(t, usersMapping))
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)

		err = s.Consume(ctx, (&recorder{}).handle)

		cancel()
		require.NoError(t, err)
	}
}
