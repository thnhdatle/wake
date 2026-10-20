// SPDX-License-Identifier: Apache-2.0

//go:build integration

package natssink_test

import (
	"context"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/natssink"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/natstest"
)

const (
	longWait  = 2 * time.Minute
	shortWait = 30 * time.Second
)

type ackCounter struct {
	mu    sync.Mutex
	acked map[uint64]int
}

func (a *ackCounter) Ack(tok pipeline.AckToken) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acked[tok.Tx]++
	return nil
}

func (a *ackCounter) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.acked)
}

func (a *ackCounter) total() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, c := range a.acked {
		n += c
	}
	return n
}

type env struct {
	n      *natstest.NATS
	sink   *natssink.Sink
	acks   *ackCounter
	prefix string
	stream string
}

func sinkConfig(n *natstest.NATS, prefix string) config.Sink {
	cfg := config.Sink{
		Name: "nats-" + prefix, Type: config.SinkNATS, Tables: []string{"*"},
		MaxEvents: 1000, MaxBytes: 1 << 20, Linger: config.Duration(5 * time.Millisecond),
		NATS: &config.NATSSink{
			URL: n.URL, SubjectPrefix: prefix, CreateStream: true, Stream: "wake_" + prefix,
			DuplicateWindow: config.Duration(2 * time.Minute), CredentialsFile: n.CredsFile,
		},
	}
	if n.TLS != nil {
		cfg.NATS.TLS = config.TLS{
			Enabled: true, CAFile: n.TLS.CAFile, CertFile: n.TLS.ClientCertFile, KeyFile: n.TLS.ClientKeyFile,
		}
	}
	return cfg
}

func runSink(t *testing.T, n *natstest.NATS, cfg config.Sink) *env {
	t.Helper()
	opts := []natssink.Option{
		natssink.WithBackoff(pipeline.Backoff{Initial: 50 * time.Millisecond, Max: 500 * time.Millisecond}),
		natssink.WithStallTimeout(2 * time.Second),
		natssink.WithRequestTimeout(5 * time.Second),
		natssink.WithAckTimeout(5 * time.Second),
	}
	s, err := natssink.New(cfg, clock.System{}, testutil.DiscardLogger(), opts...)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(longWait):
			t.Error("Run did not return")
		}
	})
	return &env{
		n: n, sink: s, acks: &ackCounter{acked: map[uint64]int{}}, prefix: cfg.NATS.SubjectPrefix,
		stream: cfg.NATS.Stream,
	}
}

func (e *env) send(t *testing.T, rec pipeline.Record, tx uint64) {
	t.Helper()
	rec.Ack = pipeline.NewAckHandle(e.acks, pipeline.AckToken{Tx: tx})
	ctx, cancel := context.WithTimeout(t.Context(), longWait)
	defer cancel()
	require.NoError(t, e.sink.Send(ctx, rec))
}

func (e *env) waitAcks(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool { return e.acks.count() >= n }, timeout, 10*time.Millisecond,
		"acks %d, want %d; health %+v", e.acks.count(), n, e.sink.Health())
}

func (e *env) waitHealth(t *testing.T, state pipeline.SinkState) pipeline.SinkHealth {
	t.Helper()
	require.Eventually(t, func() bool { return e.sink.Health().State == state }, longWait, 10*time.Millisecond,
		"health %+v, want %v", e.sink.Health(), state)
	return e.sink.Health()
}

// messages reads up to n messages of the stream in stream order.
func (e *env) messages(t *testing.T, n int) []jetstream.Msg {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), longWait)
	defer cancel()
	cons, err := e.n.JetStream(t).CreateConsumer(ctx, e.stream, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckNonePolicy, DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	require.NoError(t, err)
	var out []jetstream.Msg
	for len(out) < n {
		batch, err := cons.Fetch(min(n-len(out), 1000), jetstream.FetchMaxWait(time.Second))
		require.NoError(t, err)
		got := 0
		for m := range batch.Messages() {
			out = append(out, m)
			got++
		}
		require.NoError(t, batch.Error())
		if got == 0 {
			break
		}
	}
	return out
}

func (e *env) streamInfo(t *testing.T) *jetstream.StreamInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), shortWait)
	defer cancel()
	st, err := e.n.JetStream(t).Stream(ctx, e.stream)
	require.NoError(t, err)
	info, err := st.Info(ctx)
	require.NoError(t, err)
	return info
}

func rowEvent(op change.Op, tx uint64, table, key string) pipeline.Record {
	ev := &change.Event{
		ID:            eventIDOf(tx),
		Source:        change.Source{DB: "app", Schema: "public", Table: table},
		Op:            op,
		TsMs:          time.Now().UnixMilli(),
		SchemaVersion: 1,
		Key:           change.Row{{Name: "id", Value: []byte(key)}},
		After:         change.Row{{Name: "id", Value: []byte(key)}, {Name: "ver", Value: []byte(strconv.FormatUint(tx, 10))}},
	}
	return pipeline.NewEventRecord(ev)
}

func eventIDOf(tx uint64) change.ID { return change.StreamID(lsn.LSN(0x1000000+tx*0x100), 0) }

func TestNATSSink_Plain(t *testing.T) {
	t.Parallel()
	n := natstest.Start(t)
	t.Run("publishes with the event id and creates the stream", func(t *testing.T) {
		t.Parallel()
		testPublish(t, n)
	})
	t.Run("duplicate ids are stored once", func(t *testing.T) {
		t.Parallel()
		testDedup(t, n)
	})
	t.Run("oversized event blocks without ack", func(t *testing.T) {
		t.Parallel()
		testOversized(t, n)
	})
	t.Run("missing stream recovers when created", func(t *testing.T) {
		t.Parallel()
		testMissingStream(t, n)
	})
	t.Run("per-key order under load", func(t *testing.T) {
		t.Parallel()
		testOrderUnderLoad(t, n)
	})
}

func testPublish(t *testing.T, n *natstest.NATS) {
	t.Helper()
	e := runSink(t, n, sinkConfig(n, "pub"))
	rec := rowEvent(change.OpCreate, 1, "users", "1")
	e.send(t, rec, 1)
	e.send(t, rowEvent(change.OpCreate, 2, "orders", "1"), 2)
	e.waitAcks(t, 2, longWait)

	info := e.streamInfo(t)
	assert.Equal(t, []string{"pub.>"}, info.Config.Subjects)
	assert.Equal(t, 2*time.Minute, info.Config.Duplicates)
	msgs := e.messages(t, 2)
	require.Len(t, msgs, 2)
	assert.Equal(t, "pub.public.users", msgs[0].Subject())
	assert.Equal(t, eventIDOf(1).String(), msgs[0].Headers().Get(jetstream.MsgIDHeader))
	assert.Equal(t, string(rec.Payload), string(msgs[0].Data()))
	assert.Equal(t, "pub.public.orders", msgs[1].Subject())
	assert.True(t, e.sink.Health().IsHealthy())
}

func testDedup(t *testing.T, n *natstest.NATS) {
	t.Helper()
	e := runSink(t, n, sinkConfig(n, "dup"))
	rec := rowEvent(change.OpCreate, 1, "users", "1")
	e.send(t, rec, 1)
	e.send(t, rec, 2) // the same id again, as after a restart
	e.send(t, rowEvent(change.OpUpdate, 2, "users", "1"), 3)
	e.waitAcks(t, 3, longWait)
	assert.Equal(t, uint64(2), e.streamInfo(t).State.Msgs, "Nats-Msg-Id deduplicates by event id")
}

func testOversized(t *testing.T, n *natstest.NATS) {
	t.Helper()
	cfg := sinkConfig(n, "big")
	cfg.MaxEventBytes = 1024
	e := runSink(t, n, cfg)
	e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
	e.waitAcks(t, 1, longWait)
	big := rowEvent(change.OpCreate, 2, "users", "2")
	big.Payload = []byte(`{"blob":"` + strings.Repeat("x", 4096) + `"}`)
	e.send(t, big, 2)
	e.send(t, rowEvent(change.OpCreate, 3, "users", "3"), 3)
	h := e.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeEventTooLarge, h.Code)
	assert.Contains(t, h.Reason, "table public.users")
	assert.Contains(t, h.Reason, `key {"id":2}`)
	assert.Contains(t, h.Reason, "lsn 0/1000200")
	assert.Never(t, func() bool { return e.acks.count() > 1 }, 2*time.Second, 50*time.Millisecond)
	assert.Equal(t, uint64(1), e.streamInfo(t).State.Msgs)
}

func testMissingStream(t *testing.T, n *natstest.NATS) {
	t.Helper()
	cfg := sinkConfig(n, "own")
	cfg.NATS.CreateStream = false
	e := runSink(t, n, cfg)
	e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
	h := e.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, h.Code)
	assert.Contains(t, h.Reason, "create_stream is false")
	assert.Zero(t, e.acks.count())

	ctx, cancel := context.WithTimeout(t.Context(), shortWait)
	defer cancel()
	_, err := n.JetStream(t).CreateStream(ctx, jetstream.StreamConfig{Name: e.stream, Subjects: []string{"own.>"}})
	require.NoError(t, err)
	e.waitAcks(t, 1, longWait)
	e.waitHealth(t, pipeline.SinkStateHealthy)
}

// keyedLoad sends n updates over keys keys, round robin, and returns the
// tx numbers per key in send order.
func keyedLoad(t *testing.T, e *env, n, keys int, between func(i int)) map[string][]uint64 {
	t.Helper()
	want := map[string][]uint64{}
	for i := range n {
		tx := uint64(i + 1)
		key := strconv.Itoa(i % keys)
		e.send(t, rowEvent(change.OpUpdate, tx, "load", key), tx)
		want[key] = append(want[key], tx)
		if between != nil {
			between(i)
		}
	}
	return want
}

// checkOrder verifies that the stream holds every sent id once and, per key,
// in send order. The key is recovered from the id (tx % keys).
func checkOrder(t *testing.T, msgs []jetstream.Msg, want map[string][]uint64, keys int) {
	t.Helper()
	byID := map[string]uint64{}
	for _, txs := range want {
		for _, tx := range txs {
			byID[eventIDOf(tx).String()] = tx
		}
	}
	got := map[string][]uint64{}
	seen := map[string]bool{}
	for _, m := range msgs {
		id := m.Headers().Get(jetstream.MsgIDHeader)
		require.False(t, seen[id], "id %s stored twice", id)
		seen[id] = true
		tx, ok := byID[id]
		require.True(t, ok, "unexpected id %s", id)
		key := strconv.Itoa(int((tx - 1) % uint64(keys))) //nolint:gosec // G115: small test numbers
		got[key] = append(got[key], tx)
	}
	violations := 0
	for key, txs := range want {
		if !slices.Equal(txs, got[key]) {
			violations++
		}
	}
	assert.Zero(t, violations, "keys with missing or reordered events")
}

func testOrderUnderLoad(t *testing.T, n *natstest.NATS) {
	t.Helper()
	const count, keys = 20_000, 200
	e := runSink(t, n, sinkConfig(n, "ord"))
	start := time.Now()
	want := keyedLoad(t, e, count, keys, nil)
	e.waitAcks(t, count, longWait)
	t.Logf("%d events acknowledged in %s", count, time.Since(start).Round(time.Millisecond))
	assert.Equal(t, uint64(count), e.streamInfo(t).State.Msgs)
	checkOrder(t, e.messages(t, count), want, keys)
}

func TestNATSSink_ServerMaxPayload(t *testing.T) {
	t.Parallel()
	n := natstest.Start(t, natstest.WithMaxPayload(4096))
	e := runSink(t, n, sinkConfig(n, "pay"))
	e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
	e.waitAcks(t, 1, longWait)
	big := rowEvent(change.OpCreate, 2, "users", "2")
	big.Payload = []byte(`{"blob":"` + strings.Repeat("x", 8192) + `"}`)
	e.send(t, big, 2)
	h := e.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeEventTooLarge, h.Code, "max_event_bytes 0 means the server's max_payload")
	assert.Contains(t, h.Reason, "max_event_bytes 4096")
	assert.Equal(t, 1, e.acks.count())
}

func TestNATSSink_OutageAndRestart(t *testing.T) {
	t.Parallel()
	const count, keys = 3000, 50
	n := natstest.Start(t)
	e := runSink(t, n, sinkConfig(n, "out"))
	var ackedAtOutage int
	want := keyedLoad(t, e, count, keys, func(i int) {
		switch i {
		case 999:
			e.waitAcks(t, 1000, longWait)
			n.Outage()
			ackedAtOutage = e.acks.count()
		case 1199:
			e.waitHealth(t, pipeline.SinkStateRetrying)
			assert.Equal(t, ackedAtOutage, e.acks.count(), "nothing is acknowledged while NATS is unreachable")
			n.Restore()
		case 1999:
			n.Restart(t)
		}
	})
	e.waitAcks(t, count, 3*longWait)
	e.waitHealth(t, pipeline.SinkStateHealthy)
	assert.Equal(t, count, e.acks.total(), "every event acknowledged exactly once")
	checkOrder(t, e.messages(t, count+1), want, keys)
}

func TestNATSSink_TLSAndCredentials(t *testing.T) {
	t.Parallel()
	n := natstest.Start(t, natstest.WithTLS(), natstest.WithCredentials())
	t.Run("delivers over TLS with a credentials file", func(t *testing.T) {
		t.Parallel()
		e := runSink(t, n, sinkConfig(n, "sec"))
		e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
		e.waitAcks(t, 1, longWait)
		msgs := e.messages(t, 1)
		require.Len(t, msgs, 1)
		assert.Equal(t, eventIDOf(1).String(), msgs[0].Headers().Get(jetstream.MsgIDHeader))
	})
	t.Run("rejected credentials recover without restart", func(t *testing.T) {
		t.Parallel()
		creds := n.ForeignCredsFile(t, "rotating")
		cfg := sinkConfig(n, "rot")
		cfg.NATS.CredentialsFile = creds
		e := runSink(t, n, cfg)
		e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
		h := e.waitHealth(t, pipeline.SinkStateFatal)
		assert.Equal(t, pipeline.CodeSinkFatal, h.Code)
		assert.Contains(t, strings.ToLower(h.Reason), "authorization violation")
		assert.Zero(t, e.acks.count())

		valid, err := os.ReadFile(n.NewCredsFile(t, "valid"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(creds, valid, 0o600), "rotate the credentials file in place")
		e.waitAcks(t, 1, longWait)
		e.waitHealth(t, pipeline.SinkStateHealthy)
	})
}
