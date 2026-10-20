// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/tlsgen"
)

func TestSink_Send_PublishesWithMsgIDAndAcks(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	h := startSink(t, baseConfig(), fc)

	rec := event(change.OpCreate, 0, "42")
	h.send(t, rec, 1)
	h.waitAcks(t, 1)

	all := fc.snapshot()
	require.Len(t, all, 1)
	assert.Equal(t, "cdc.public.users", all[0].msg.Subject)
	assert.Equal(t, "0/16B3748-0", all[0].id, "Nats-Msg-Id is the event id, never the key")
	assert.Equal(t, string(rec.Payload), string(all[0].msg.Data))
	fc.mu.Lock()
	assert.Equal(t, []streamSpec{{name: testStream, subject: "cdc.>", window: 2 * time.Minute}}, fc.ensured)
	fc.mu.Unlock()
	h.rec.mu.Lock()
	assert.Equal(t, 1, h.rec.sent)
	assert.Equal(t, 1, h.rec.acked)
	assert.Equal(t, []int64{rec.TsMs}, h.rec.tsMs)
	assert.Equal(t, 0, h.rec.inflight)
	h.rec.mu.Unlock()
	assert.True(t, h.sink.Health().IsHealthy())
	assert.Equal(t, "nats-main", h.sink.Name())
}

func TestSink_SchemaAndMessageSubjects(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	h := startSink(t, baseConfig(), fc)

	sc := pipeline.NewSchemaChangeRecord(&change.SchemaChange{
		ID: change.SchemaID(0x16C0000, 0), Source: change.Source{DB: "app", Schema: "public", Table: "users"},
		SchemaVersion: 4,
	})
	msg := pipeline.NewMessageRecord(&change.Message{ID: change.MessageID(0x100, 1), Prefix: "p", Content: []byte("c")})
	h.send(t, sc, 1)
	h.send(t, msg, 2)
	h.waitAcks(t, 2)
	all := fc.snapshot()
	assert.Equal(t, "cdc._schema", all[0].msg.Subject)
	assert.Equal(t, "0/16C0000-s0", all[0].id)
	assert.Equal(t, "cdc._messages", all[1].msg.Subject)
}

func TestSink_StreamMissing_FatalUntilCreated(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	cfg := baseConfig()
	cfg.NATS.CreateStream = false
	h := startSink(t, cfg, fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	hl := h.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, hl.Code)
	assert.Empty(t, fc.snapshot())
	fc.set(func(f *fakeConn) { f.streams[testStream] = true })
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
	assert.Positive(t, h.rec.retry(RetryReasonUnknownStream))
}

func TestSink_EnsureStreamError_Retrying(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	fc.ensureErr = nats.ErrTimeout
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	h.waitHealth(t, pipeline.SinkStateRetrying)
	fc.set(func(f *fakeConn) { f.ensureErr = nil })
	h.waitAcks(t, 1)
}

func TestSink_EventTooLarge_BlocksWithoutAck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		maxEvent config.ByteSize
		payload  int64
	}{
		{"max_event_bytes", 200, 1 << 20},
		{"server max_payload by default", 0, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeConn(true)
			fc.payload = tc.payload
			cfg := baseConfig()
			cfg.MaxEventBytes = tc.maxEvent
			h := startSink(t, cfg, fc)

			big := event(change.OpCreate, 0, "77")
			big.Payload = []byte(`{"blob":"` + strings.Repeat("x", 300) + `"}`)
			h.send(t, big, 1)
			h.send(t, event(change.OpCreate, 1, "78"), 2)
			hl := h.waitHealth(t, pipeline.SinkStateFatal)
			assert.Equal(t, pipeline.CodeEventTooLarge, hl.Code)
			assert.Contains(t, hl.Reason, "table public.users")
			assert.Contains(t, hl.Reason, `key {"id":77}`)
			assert.Contains(t, hl.Reason, "lsn 0/16B3748")
			assert.NotContains(t, hl.Reason, "xxxx")
			require.Eventually(t, func() bool { return h.rec.retry(RetryReasonEventTooLarge) >= 2 },
				waitTimeout, time.Millisecond)
			assert.Empty(t, fc.snapshot())
			assert.Zero(t, h.acks.total())
		})
	}
}

func TestSink_ServerPayloadLimit_FatalUntilRaised(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	fc.payload = 150
	cfg := baseConfig()
	cfg.MaxEventBytes = 1 << 20
	h := startSink(t, cfg, fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	hl := h.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, hl.Code)
	assert.Contains(t, hl.Reason, "max_payload 150")
	fc.set(func(f *fakeConn) { f.payload = 1 << 20 })
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
}

func TestSink_NotConnectedYet_Retrying(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	fc.payload = 0
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	hl := h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Contains(t, hl.Reason, "not connected")
	fc.set(func(f *fakeConn) { f.payload = 1 << 20 })
	h.waitAcks(t, 1)
}

func TestSink_FailedPublish_RepublishedInOrderBeforeNewer(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(false)
	h := startSink(t, baseConfig(), fc)

	for k := range uint64(3) {
		h.send(t, event(change.OpUpdate, k, "5"), k+1)
	}
	fc.waitPublished(t, 3)
	fc.complete(1, nil)
	fc.complete(2, jetstream.ErrNoStreamResponse)
	hl := h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Contains(t, hl.Reason, "no response from stream")

	h.send(t, event(change.OpUpdate, 3, "5"), 4)
	all := fc.waitPublished(t, 5)
	assert.Equal(t, []string{"0/16B3748-1", "0/16B3748-2"}, ids(all[3:5]))
	assert.Never(t, func() bool { return len(fc.snapshot()) > 5 }, 50*time.Millisecond, time.Millisecond,
		"nothing newer is published while re-published messages are unresolved")
	fc.mu.Lock()
	assert.Len(t, fc.ensured, 2, "a missing stream response re-checks the stream")
	fc.mu.Unlock()
	fc.complete(2, nil)
	all = fc.waitPublished(t, 6)
	assert.Equal(t, "0/16B3748-3", all[5].id)
	fc.complete(1, nil)
	h.waitAcks(t, 4)
	h.waitHealth(t, pipeline.SinkStateHealthy)
	assert.Equal(t, 2, h.rec.retry(RetryReasonServer))
}

func TestSink_Reconnect_WaitsForOldConnection(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(false)
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpUpdate, 0, "5"), 1)
	fc.waitPublished(t, 1)
	fc.set(func(f *fakeConn) { f.ep++ })
	h.send(t, event(change.OpUpdate, 1, "5"), 2)
	assert.Never(t, func() bool { return len(fc.snapshot()) > 1 }, 50*time.Millisecond, time.Millisecond,
		"no publish on a new connection while the old one has unresolved messages")
	fc.complete(1, nats.ErrDisconnected)
	all := fc.waitPublished(t, 2)
	assert.Equal(t, "0/16B3748-0", all[1].id, "the lost message goes first")
	fc.complete(1, nil)
	all = fc.waitPublished(t, 3)
	assert.Equal(t, "0/16B3748-1", all[2].id)
	fc.complete(1, nil)
	h.waitAcks(t, 2)
}

func TestSink_SyncPublishError_ReportsLoginRejection(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	fc.pubErr = nats.ErrReconnectBufExceeded
	h := startSink(t, baseConfig(), fc)
	h.sink.noteConnErr(nats.ErrAuthorization)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	hl := h.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, hl.Code)
	assert.Contains(t, hl.Reason, "authorization violation")
	fc.set(func(f *fakeConn) { f.pubErr = nil })
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
}

func TestSink_Watch_ConnectionState(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	fc.connected = false
	fc.lastErr = nats.ErrAuthorization
	h := startSink(t, baseConfig(), fc, WithStallTimeout(20*time.Millisecond))

	hl := h.waitHealth(t, pipeline.SinkStateFatal)
	assert.Contains(t, hl.Reason, "authorization violation")
	fc.set(func(f *fakeConn) { f.lastErr = nil })
	hl = h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Contains(t, hl.Reason, "not connected")
	fc.set(func(f *fakeConn) { f.connected = true })
	h.waitHealth(t, pipeline.SinkStateHealthy)
}

func TestSink_Watch_StalledAcks(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(false)
	h := startSink(t, baseConfig(), fc, WithStallTimeout(20*time.Millisecond))

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	fc.waitPublished(t, 1)
	hl := h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Contains(t, hl.Reason, "no PubAck")
	fc.complete(1, nil)
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
}

func TestSink_Send_BlocksAtBufferBound(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(false)
	h := startSink(t, baseConfig(), fc, WithMaxBuffered(1, 1<<20))

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, h.sink.Send(ctx, event(change.OpCreate, 1, "2")), context.DeadlineExceeded)
	fc.waitPublished(t, 1)
	fc.complete(1, nil)
	h.send(t, event(change.OpCreate, 1, "2"), 2)
	fc.waitPublished(t, 2)
	fc.complete(1, nil)
	h.waitAcks(t, 2)
}

func TestSink_Send_BlocksAtByteBound(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(false)
	h := startSink(t, baseConfig(), fc, WithMaxBuffered(10, 50))
	h.send(t, event(change.OpCreate, 0, "1"), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, h.sink.Send(ctx, event(change.OpCreate, 1, "2")), context.DeadlineExceeded)
}

func TestSink_Dial_RetriesThenRuns(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	attempts := 0
	dial := func(*Sink) (conn, error) {
		attempts++
		if attempts < 3 {
			return nil, nats.ErrNoServers
		}
		return fc, nil
	}
	h := startSink(t, baseConfig(), fc, withDialer(dial))
	h.send(t, event(change.OpCreate, 0, "1"), 1)
	h.waitAcks(t, 1)
	assert.Equal(t, 2, h.rec.retry(RetryReasonNetwork))
}

func TestSink_Run_TwiceShutdownAndCancelledDial(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	s, err := New(baseConfig(), clock.System{}, testutil.DiscardLogger(), withDialer(fc.dialer()))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	require.NoError(t, s.Send(ctx, event(change.OpCreate, 0, "1")))
	fc.waitPublished(t, 1)
	require.ErrorIs(t, s.Run(ctx), ErrAlreadyRunning)
	cancel()
	require.NoError(t, <-done)
	fc.mu.Lock()
	assert.True(t, fc.closed)
	fc.mu.Unlock()

	never := func(*Sink) (conn, error) { return nil, nats.ErrNoServers }
	s, err = New(baseConfig(), clock.System{}, testutil.DiscardLogger(), withDialer(never), WithBackoff(fastBackoff))
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.NoError(t, s.Run(ctx), "Run returns nil when ctx ends before a connection")
}

func TestSink_AckError_IsLogged(t *testing.T) {
	t.Parallel()
	fc := newFakeConn(true)
	h := startSink(t, baseConfig(), fc)
	h.acks.err = errInjected
	h.send(t, event(change.OpCreate, 0, "1"), 1)
	h.waitAcks(t, 1)
}

func TestNew_Invalid(t *testing.T) {
	t.Parallel()
	missing := "/nonexistent/wake-test.pem"
	tests := []struct {
		name   string
		mutate func(*config.Sink)
	}{
		{"no nats section", func(c *config.Sink) { c.NATS = nil }},
		{"no name", func(c *config.Sink) { c.Name = "" }},
		{"wrong type", func(c *config.Sink) { c.Type = config.SinkKafka }},
		{"no url", func(c *config.Sink) { c.NATS.URL = " " }},
		{"bad prefix", func(c *config.Sink) { c.NATS.SubjectPrefix = "a.b" }},
		{"no stream", func(c *config.Sink) { c.NATS.Stream = "" }},
		{"bad stream", func(c *config.Sink) { c.NATS.Stream = "a.b" }},
		{"negative window", func(c *config.Sink) { c.NATS.DuplicateWindow = -1 }},
		{"missing CA", func(c *config.Sink) { c.NATS.TLS = config.TLS{Enabled: true, CAFile: missing} }},
		{"missing key pair", func(c *config.Sink) {
			c.NATS.TLS = config.TLS{Enabled: true, CertFile: missing, KeyFile: missing}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := baseConfig()
			tc.mutate(&cfg)
			_, err := New(cfg, clock.System{}, testutil.DiscardLogger())
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
	_, err := New(baseConfig(), nil, testutil.DiscardLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNew_TLS(t *testing.T) {
	t.Parallel()
	b := tlsgen.Generate(t, "127.0.0.1")
	cfg := baseConfig()
	cfg.NATS.TLS = config.TLS{Enabled: true, CAFile: b.CAFile, CertFile: b.ClientCertFile, KeyFile: b.ClientKeyFile}
	s, err := New(cfg, clock.System{}, testutil.DiscardLogger())
	require.NoError(t, err)
	require.NotNil(t, s.tls)
	assert.NotNil(t, s.tls.RootCAs)
	assert.Len(t, s.tls.Certificates, 1)

	cfg.NATS.TLS = config.TLS{Enabled: true, CAFile: b.ClientKeyFile}
	_, err = New(cfg, clock.System{}, testutil.DiscardLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestUnit_SizeCountsHeaders(t *testing.T) {
	t.Parallel()
	s, err := New(baseConfig(), clock.System{}, testutil.DiscardLogger())
	require.NoError(t, err)
	rec := event(change.OpCreate, 12, "1")
	u := s.newUnit(rec)
	m := &nats.Msg{Subject: "s", Data: rec.Payload, Header: nats.Header{}}
	m.Header.Set(jetstream.MsgIDHeader, u.id)
	m.Header.Set(jetstream.ExpectedStreamHeader, testStream)
	assert.Equal(t, int64(m.Size()-len(m.Subject)), u.size, "the size is what the server's max_payload counts")
}
