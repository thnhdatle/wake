// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMsg is a jetstream.Msg recording acknowledgements.
type fakeMsg struct {
	data []byte

	mu                   sync.Mutex
	acks, naks, progress int
	ackErr               error
	progressed           chan struct{}
}

func newFakeMsg(data []byte) *fakeMsg {
	return &fakeMsg{data: data, progressed: make(chan struct{}, 1)}
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return &jetstream.MsgMetadata{}, nil }
func (m *fakeMsg) Data() []byte                              { return m.data }
func (m *fakeMsg) Headers() nats.Header                      { return nil }
func (m *fakeMsg) Subject() string                           { return "cdc.public.users" }
func (m *fakeMsg) Reply() string                             { return "" }
func (m *fakeMsg) DoubleAck(context.Context) error           { return m.Ack() }
func (m *fakeMsg) NakWithDelay(time.Duration) error          { return m.Nak() }
func (m *fakeMsg) Term() error                               { return nil }
func (m *fakeMsg) TermWithReason(string) error               { return nil }

func (m *fakeMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks++
	return m.ackErr
}

func (m *fakeMsg) Nak() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.naks++
	return nil
}

func (m *fakeMsg) InProgress() error {
	m.mu.Lock()
	m.progress++
	m.mu.Unlock()
	select {
	case m.progressed <- struct{}{}:
	default:
	}
	return nil
}

func (m *fakeMsg) counts() (acks, naks int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acks, m.naks
}

// fakeBatch is a finished jetstream.MessageBatch.
type fakeBatch struct {
	ch  chan jetstream.Msg
	err error
}

func batchOf(err error, msgs ...*fakeMsg) *fakeBatch {
	ch := make(chan jetstream.Msg, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	return &fakeBatch{ch: ch, err: err}
}

func (b *fakeBatch) Messages() <-chan jetstream.Msg { return b.ch }
func (b *fakeBatch) Error() error                   { return b.err }

// fetchResult is one scripted Fetch or FetchNoWait answer.
type fetchResult struct {
	batch *fakeBatch
	err   error
}

// fakeConsumer answers Fetch and FetchNoWait from scripts, then blocks Fetch
// until its FetchContext ends.
type fakeConsumer struct {
	mu      sync.Mutex
	fetches []fetchResult
	noWaits []fetchResult
	// idle is closed when the Fetch script is exhausted; later Fetch calls
	// block until done is closed.
	idle     chan struct{}
	idleOnce sync.Once
	done     <-chan struct{}
}

func (c *fakeConsumer) Fetch(batch int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	if batch != 1 {
		return nil, errors.New("unexpected batch size")
	}
	c.mu.Lock()
	if len(c.fetches) > 0 {
		r := c.fetches[0]
		c.fetches = c.fetches[1:]
		c.mu.Unlock()
		if r.err != nil {
			return nil, r.err
		}
		return r.batch, nil
	}
	done := c.done
	c.mu.Unlock()
	if c.idle != nil {
		c.idleOnce.Do(func() { close(c.idle) })
	}
	// Like the real client once FetchContext ends: an empty batch with the
	// context's error.
	<-done
	ch := make(chan jetstream.Msg)
	close(ch)
	return &fakeBatch{ch: ch, err: context.Canceled}, nil
}

func (c *fakeConsumer) FetchNoWait(int) (jetstream.MessageBatch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.noWaits) == 0 {
		return batchOf(nil), nil
	}
	r := c.noWaits[0]
	c.noWaits = c.noWaits[1:]
	if r.err != nil {
		return nil, r.err
	}
	return r.batch, nil
}

// fakeNATSConn hands out consumers, failing the first consumerErrs calls.
type fakeNATSConn struct {
	mu           sync.Mutex
	consumers    []*fakeConsumer
	consumerErrs []error
	opened       int
	cfgs         []jetstream.ConsumerConfig
	up           bool
	closed       bool
}

func (c *fakeNATSConn) consumer(_ context.Context, stream string, cfg jetstream.ConsumerConfig) (natsConsumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if stream != "wake_app" {
		return nil, errors.New("unexpected stream " + stream)
	}
	c.cfgs = append(c.cfgs, cfg)
	if len(c.consumerErrs) > 0 {
		err := c.consumerErrs[0]
		c.consumerErrs = c.consumerErrs[1:]
		return nil, err
	}
	cons := c.consumers[min(c.opened, len(c.consumers)-1)]
	c.opened++
	return cons, nil
}

func (c *fakeNATSConn) connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.up
}

func (c *fakeNATSConn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
}

func natsConfig() NATSConfig {
	return NATSConfig{URL: "nats://127.0.0.1:4222", SubjectPrefix: "cdc", Stream: "wake_app", Durable: "inval"}
}

func newTestNATSSource(t *testing.T, conn *fakeNATSConn, cfg NATSConfig) *NATSSource {
	t.Helper()
	s, err := NewNATSSource(cfg, mustMapping(t, usersMapping))
	require.NoError(t, err)
	s.dial = func(*NATSConfig) (natsConn, error) { return conn, nil }
	s.retryMin, s.retryMax = time.Millisecond, 2*time.Millisecond
	return s
}

// consumeUntilIdle runs Consume until cons has served its script, then
// stops it and returns Consume's result.
func consumeUntilIdle(t *testing.T, s *NATSSource, cons *fakeConsumer, handle Handler) error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cons.done = ctx.Done()
	done := make(chan error, 1)
	go func() { done <- s.Consume(ctx, handle) }()
	<-cons.idle
	cancel()
	return <-done
}

func TestNewNATSSource_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*NATSConfig)
		want   string
	}{
		{"no url", func(c *NATSConfig) { c.URL = "" }, "empty URL"},
		{"bad prefix", func(c *NATSConfig) { c.SubjectPrefix = "a.b" }, "subject prefix"},
		{"no stream", func(c *NATSConfig) { c.Stream = "" }, "stream name"},
		{"bad durable", func(c *NATSConfig) { c.Durable = "a.b" }, "durable name"},
		{"negative ack wait", func(c *NATSConfig) { c.AckWait = -time.Second }, "negative ack wait"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := natsConfig()
			tc.mutate(&cfg)

			s, err := NewNATSSource(cfg, mustMapping(t, usersMapping))

			require.ErrorIs(t, err, ErrInvalidConfig)
			assert.Contains(t, err.Error(), tc.want)
			assert.Nil(t, s)
		})
	}

	_, err := NewNATSSource(natsConfig(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNATSSource_Consume_AcksAfterHandler(t *testing.T) {
	t.Parallel()
	m1, m2, m3 := newFakeMsg([]byte("a")), newFakeMsg([]byte("b")), newFakeMsg(nil)
	m2.ackErr = errors.New("connection closed")
	cons := &fakeConsumer{
		idle:    make(chan struct{}),
		fetches: []fetchResult{{batch: batchOf(nil, m1)}, {batch: batchOf(context.DeadlineExceeded)}},
		noWaits: []fetchResult{{batch: batchOf(nats.ErrConnectionClosed, m2, m3)}},
	}
	conn := &fakeNATSConn{consumers: []*fakeConsumer{cons}, up: true}
	s := newTestNATSSource(t, conn, natsConfig())
	var rec recorder

	require.NoError(t, consumeUntilIdle(t, s, cons, rec.handle))

	require.Equal(t, 1, rec.count())
	assert.Equal(t, [][]byte{[]byte("a"), []byte("b"), nil}, rec.batches[0])
	for _, m := range []*fakeMsg{m1, m2, m3} {
		acks, naks := m.counts()
		assert.Equal(t, 1, acks)
		assert.Zero(t, naks)
	}
	assert.True(t, conn.closed)
	require.Len(t, conn.cfgs, 1)
	cfg := conn.cfgs[0]
	assert.Equal(t, "inval", cfg.Durable)
	assert.Equal(t, []string{"cdc.public.users"}, cfg.FilterSubjects)
	assert.Equal(t, jetstream.AckExplicitPolicy, cfg.AckPolicy)
	assert.Equal(t, jetstream.DeliverAllPolicy, cfg.DeliverPolicy)
	assert.Equal(t, DefaultNATSAckWait, cfg.AckWait)
	assert.Equal(t, []string{"cdc.public.users"}, s.Subjects())
}

func TestNATSSource_Consume_HandlerErrorNaks(t *testing.T) {
	t.Parallel()
	m1 := newFakeMsg([]byte("a"))
	cons := &fakeConsumer{fetches: []fetchResult{{batch: batchOf(nil, m1)}}}
	conn := &fakeNATSConn{consumers: []*fakeConsumer{cons}, up: true}
	cfg := natsConfig()
	cfg.MaxBatch = 1
	s := newTestNATSSource(t, conn, cfg)

	err := s.Consume(t.Context(), (&recorder{err: context.Canceled}).handle)

	require.ErrorIs(t, err, context.Canceled)
	acks, naks := m1.counts()
	assert.Zero(t, acks)
	assert.Equal(t, 1, naks)
}

// TestNATSSource_Consume_KeepsSlowBatchInProgress checks that the messages
// of a batch whose handler is still retrying are kept in progress.
func TestNATSSource_Consume_KeepsSlowBatchInProgress(t *testing.T) {
	t.Parallel()
	m1 := newFakeMsg([]byte("a"))
	cons := &fakeConsumer{idle: make(chan struct{}), fetches: []fetchResult{{batch: batchOf(nil, m1)}}}
	conn := &fakeNATSConn{consumers: []*fakeConsumer{cons}, up: true}
	cfg := natsConfig()
	cfg.AckWait = 30 * time.Millisecond
	s := newTestNATSSource(t, conn, cfg)
	handle := func(context.Context, [][]byte) error {
		<-m1.progressed // returns once InProgress was sent
		return nil
	}

	require.NoError(t, consumeUntilIdle(t, s, cons, handle))

	acks, _ := m1.counts()
	assert.Equal(t, 1, acks)
}

func TestNATSSource_Consume_RecoversFromErrors(t *testing.T) {
	t.Parallel()
	m1 := newFakeMsg([]byte("a"))
	broken := &fakeConsumer{fetches: []fetchResult{{err: jetstream.ErrConsumerDeleted}}}
	failing := &fakeConsumer{fetches: []fetchResult{{batch: batchOf(errors.New("409 leadership change"))}}}
	good := &fakeConsumer{
		idle:    make(chan struct{}),
		fetches: []fetchResult{{batch: batchOf(nil, m1)}},
		noWaits: []fetchResult{{err: nats.ErrTimeout}},
	}
	conn := &fakeNATSConn{
		consumers:    []*fakeConsumer{broken, failing, good},
		consumerErrs: []error{jetstream.ErrStreamNotFound},
		up:           true,
	}
	s := newTestNATSSource(t, conn, natsConfig())
	var rec recorder

	require.NoError(t, consumeUntilIdle(t, s, good, rec.handle))

	assert.Equal(t, 1, rec.count())
	assert.Len(t, conn.cfgs, 4, "a failed setup and every fetch error re-create the consumer")
	acks, _ := m1.counts()
	assert.Equal(t, 1, acks)
}

func TestNATSSource_Check(t *testing.T) {
	t.Parallel()
	cons := &fakeConsumer{idle: make(chan struct{})}
	conn := &fakeNATSConn{consumers: []*fakeConsumer{cons}, consumerErrs: []error{errors.New("no responders")}}
	s := newTestNATSSource(t, conn, natsConfig())
	require.ErrorIs(t, s.Check(t.Context()), ErrSourceNotRunning)
	ctx, cancel := context.WithCancel(t.Context())
	cons.done = ctx.Done()
	done := make(chan error, 1)

	go func() { done <- s.Consume(ctx, (&recorder{}).handle) }()
	<-cons.idle
	require.EqualError(t, s.Check(t.Context()), "cdcsource: nats: not connected")
	conn.mu.Lock()
	conn.up = true
	conn.mu.Unlock()
	require.NoError(t, s.Check(t.Context()))
	cancel()

	require.NoError(t, <-done)
	require.ErrorIs(t, s.Check(t.Context()), ErrSourceNotRunning)
}

func TestNATSSource_Consume_StopsDuringSetup(t *testing.T) {
	t.Parallel()
	conn := &fakeNATSConn{consumerErrs: []error{errors.New("timeout"), errors.New("timeout"), errors.New("timeout")}}
	s := newTestNATSSource(t, conn, natsConfig())
	s.retryMin, s.retryMax = time.Hour, time.Hour
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- s.Consume(ctx, (&recorder{}).handle) }()
	eventually(t, func() bool {
		return s.Check(t.Context()) != nil && !errors.Is(s.Check(t.Context()), ErrSourceNotRunning)
	})
	require.ErrorContains(t, s.Check(t.Context()), "nats: not connected")
	cancel()

	require.NoError(t, <-done)
}

func TestNATSSource_Consume_DialError(t *testing.T) {
	t.Parallel()
	s := newTestNATSSource(t, nil, natsConfig())
	s.dial = func(*NATSConfig) (natsConn, error) { return nil, errors.New("bad credentials file") }

	require.EqualError(t, s.Consume(t.Context(), (&recorder{}).handle), "bad credentials file")
}

// TestNATSSource_Consume_RealClientUnreachable runs nats.go against a closed
// port: the connection keeps retrying and Consume returns nil when ctx ends.
func TestNATSSource_Consume_RealClientUnreachable(t *testing.T) {
	t.Parallel()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	cfg := natsConfig()
	cfg.URL = "nats://" + addr
	cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	s, err := NewNATSSource(cfg, mustMapping(t, usersMapping))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	require.NoError(t, s.Consume(ctx, (&recorder{}).handle))

	cfg.CredentialsFile = "/nonexistent/creds"
	s, err = NewNATSSource(cfg, mustMapping(t, usersMapping))
	require.NoError(t, err)
	require.ErrorContains(t, s.Consume(t.Context(), (&recorder{}).handle), "nats connect")
}
