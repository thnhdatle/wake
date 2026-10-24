// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/tlsgen"
)

func headerMap(r *kgo.Record) map[string]string {
	m := map[string]string{}
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

func TestSink_Send_WritesTopicKeyHeadersAndAcks(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 3})
	h := startSink(t, baseConfig(), fc)

	rec := event(change.OpCreate, 0, "42")
	h.send(t, rec, 1)
	h.waitAcks(t, 1)

	all := fc.snapshot()
	require.Len(t, all, 1)
	r := all[0].rec
	assert.Equal(t, usersTopic, r.Topic)
	assert.Equal(t, `{"id":42}`, string(r.Key))
	assert.Equal(t, string(rec.Payload), string(r.Value))
	assert.Equal(t, map[string]string{HeaderEventID: "0/16B3748-0", HeaderSchemaVersion: "3"}, headerMap(r))
	assert.Equal(t, int32(keyPartition(r.Key, 3)), r.Partition)
	assert.Equal(t, 1, h.acks.count(1))
	h.rec.mu.Lock()
	assert.Equal(t, 1, h.rec.sent)
	assert.Equal(t, 1, h.rec.acked)
	assert.Equal(t, 1, h.rec.latency)
	assert.Equal(t, 0, h.rec.inflight)
	assert.Equal(t, []int64{rec.TsMs}, h.rec.tsMs)
	h.rec.mu.Unlock()
	assert.True(t, h.sink.Health().IsHealthy())
	assert.Equal(t, "kafka-main", h.sink.Name())
}

func TestSink_Delete_AckedOnlyAfterTombstone(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 4})
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpDelete, 0, "43"), 7)
	all := fc.waitRecords(t, 2)
	del, tomb := all[0].rec, all[1].rec
	assert.Equal(t, `{"id":43}`, string(del.Key))
	assert.NotNil(t, del.Value)
	assert.Equal(t, del.Key, tomb.Key)
	assert.Nil(t, tomb.Value, "tombstone has a null value")
	assert.Empty(t, tomb.Headers, "tombstone has no headers (event fixture)")
	assert.Equal(t, del.Partition, tomb.Partition)

	fc.complete(1, nil)
	assert.Never(t, func() bool { return h.acks.total() > 0 }, 50*time.Millisecond, time.Millisecond,
		"a d event is acknowledged only with its tombstone")
	fc.complete(1, nil)
	h.waitAcks(t, 1)
	assert.Equal(t, 1, h.acks.count(7))
}

func TestSink_Delete_NoTombstoneWhenDisabled(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	cfg := baseConfig()
	cfg.Kafka.Tombstones = false
	h := startSink(t, cfg, fc)

	h.send(t, event(change.OpDelete, 0, "43"), 1)
	h.waitAcks(t, 1)
	all := fc.snapshot()
	require.Len(t, all, 1)
	assert.NotNil(t, all[0].rec.Value)
}

func TestSink_Truncate_FansOutToEveryPartition(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 3})
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpTruncate, 5, ""), 9)
	all := fc.waitRecords(t, 3)
	parts := make([]int32, 0, len(all))
	for _, p := range all {
		assert.Nil(t, p.rec.Key)
		assert.Equal(t, "0/16B3748-5", headerMap(p.rec)[HeaderEventID])
		parts = append(parts, p.rec.Partition)
	}
	assert.ElementsMatch(t, []int32{0, 1, 2}, parts)

	fc.complete(2, nil)
	assert.Never(t, func() bool { return h.acks.total() > 0 }, 50*time.Millisecond, time.Millisecond,
		"a t event is acknowledged only when every partition write is")
	fc.complete(1, nil)
	h.waitAcks(t, 1)
}

func TestSink_Truncate_ReadsCurrentPartitionCount(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 2})
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	h.waitAcks(t, 1)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 5})
	h.send(t, event(change.OpTruncate, 1, ""), 2)
	h.waitAcks(t, 2)
	all := fc.snapshot()
	assert.Len(t, all, 1+5)
}

func TestSink_Messages_PartitionByID(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	topic := testTopicPrefix + "." + pipeline.DestinationMessages
	fc.setTopic(topic, topicInfo{exists: true, partitions: 8})
	h := startSink(t, baseConfig(), fc)

	msg := pipeline.NewMessageRecord(&change.Message{
		ID: change.MessageID(lsn.LSN(0x100), 1), Prefix: "app", Content: []byte("x"), TsMs: 5,
	})
	h.send(t, msg, 1)
	h.waitAcks(t, 1)
	all := fc.snapshot()
	require.Len(t, all, 1)
	r := all[0].rec
	assert.Equal(t, topic, r.Topic)
	assert.Nil(t, r.Key)
	assert.Equal(t, int32(keyPartition([]byte(msg.ID.String()), 8)), r.Partition)
}

func TestSink_EventTooLarge_BlocksWithoutAck(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	cfg := baseConfig()
	cfg.MaxEventBytes = 200
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
	assert.NotContains(t, hl.Reason, "xxxx", "no row values in the reason")
	require.Eventually(t, func() bool { return h.rec.retry(RetryReasonEventTooLarge) >= 2 }, waitTimeout, time.Millisecond)
	all := fc.snapshot()
	assert.Empty(t, all, "nothing after the oversized event is produced")
	assert.Zero(t, h.acks.total())
}

func TestSink_UnknownTopic_FatalUntilCreated(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	hl := h.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, hl.Code)
	assert.Contains(t, hl.Reason, usersTopic)
	assert.Zero(t, h.acks.total())

	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
	assert.Positive(t, h.rec.retry(RetryReasonUnknownTopic))
}

func TestSink_AutoCreate_CreatesTopicWithConfiguredLayout(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	cfg := baseConfig()
	cfg.Kafka.AutoCreateTopics = true
	cfg.Kafka.Partitions = 6
	cfg.Kafka.ReplicationFactor = 1
	h := startSink(t, cfg, fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	h.waitAcks(t, 1)
	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.Equal(t, []string{usersTopic}, fc.created)
	assert.Equal(t, [][2]int{{6, 1}}, fc.createdArgs)
}

func TestSink_TopicLimit_FatalUntilRaised(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1, maxMessageBytes: 150})
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	hl := h.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, hl.Code)
	assert.Contains(t, hl.Reason, "max.message.bytes 150")

	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1, maxMessageBytes: 1 << 20})
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
}

func TestSink_DescribeError_Retrying(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.describeErr = kerr.LeaderNotAvailable
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	hl := h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Empty(t, hl.Code)
	fc.mu.Lock()
	fc.describeErr = nil
	fc.topics[usersTopic] = topicInfo{exists: true, partitions: 1}
	fc.mu.Unlock()
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
}

func TestSink_FailedRecords_ReproducedInOrderBeforeNewer(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	h := startSink(t, baseConfig(), fc)

	for k := range uint64(3) {
		h.send(t, event(change.OpUpdate, k, "5"), k+1)
	}
	fc.waitRecords(t, 3)
	fc.complete(3, kerr.TopicAuthorizationFailed)
	hl := h.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, hl.Code)
	assert.Contains(t, hl.Reason, "TOPIC_AUTHORIZATION_FAILED")

	h.send(t, event(change.OpUpdate, 3, "5"), 4)
	all := fc.waitRecords(t, 6)
	assert.Never(t, func() bool { return len(fc.snapshot()) > 6 }, 50*time.Millisecond, time.Millisecond,
		"nothing newer is produced while re-produced records are unresolved")
	ids := func(ps []produced) []string {
		out := make([]string, 0, len(ps))
		for _, p := range ps {
			out = append(out, headerMap(p.rec)[HeaderEventID])
		}
		return out
	}
	assert.Equal(t, []string{"0/16B3748-0", "0/16B3748-1", "0/16B3748-2"}, ids(all[3:6]))
	assert.Zero(t, h.acks.total())

	fc.complete(3, nil)
	all = fc.waitRecords(t, 7)
	assert.Equal(t, "0/16B3748-3", ids(all[6:])[0])
	fc.complete(1, nil)
	h.waitAcks(t, 4)
	h.waitHealth(t, pipeline.SinkStateHealthy)
	for tx := uint64(1); tx <= 4; tx++ {
		assert.Equal(t, 1, h.acks.count(tx), "tx %d acknowledged once", tx)
	}
	assert.Equal(t, 3, h.rec.retry(RetryReasonAuth))
}

func TestSink_RetriableFailure_ReportsRetrying(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	fc.waitRecords(t, 1)
	fc.complete(1, kerr.NotEnoughReplicas)
	hl := h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Contains(t, hl.Reason, "NOT_ENOUGH_REPLICAS")
	fc.waitRecords(t, 2)
	fc.complete(1, nil)
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
	assert.Equal(t, 1, h.rec.retry(RetryReasonBroker))
}

func TestSink_Stall_ReportsRetryingUntilAck(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	h := startSink(t, baseConfig(), fc, WithStallTimeout(20*time.Millisecond))

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	fc.waitRecords(t, 1)
	h.sink.mu.Lock()
	h.sink.lastConnErr = context.DeadlineExceeded
	h.sink.mu.Unlock()
	hl := h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Contains(t, hl.Reason, "no acknowledgement from Kafka")
	fc.complete(1, nil)
	h.waitAcks(t, 1)
	h.waitHealth(t, pipeline.SinkStateHealthy)
}

func TestSink_Send_BlocksAtBufferBound(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	h := startSink(t, baseConfig(), fc, WithMaxBuffered(2, 1<<20))

	h.send(t, event(change.OpCreate, 0, "1"), 1)
	h.send(t, event(change.OpCreate, 1, "2"), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := h.sink.Send(ctx, event(change.OpCreate, 2, "3"))
	require.ErrorIs(t, err, context.DeadlineExceeded)

	fc.waitRecords(t, 2)
	fc.complete(1, nil)
	h.send(t, event(change.OpCreate, 2, "3"), 3)
	fc.complete(1, nil)
	fc.waitRecords(t, 3)
	fc.complete(1, nil)
	h.waitAcks(t, 3)
}

func TestSink_Send_BlocksAtByteBound(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	rec := event(change.OpCreate, 0, "1")
	h := startSink(t, baseConfig(), fc, WithMaxBuffered(100, eventSize(&rec)))

	h.send(t, rec, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, h.sink.Send(ctx, event(change.OpCreate, 1, "2")), context.DeadlineExceeded)
	fc.waitRecords(t, 1)
	fc.complete(1, nil)
	h.send(t, event(change.OpCreate, 1, "2"), 2)
}

func TestSink_Run_TwiceAndShutdown(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(false)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
	s, err := New(baseConfig(), getenv, clock.System{}, testutil.DiscardLogger(), withClientFactory(fc.factory()))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	require.NoError(t, s.Send(ctx, event(change.OpCreate, 0, "1")))
	fc.waitRecords(t, 1)
	require.ErrorIs(t, s.Run(ctx), ErrAlreadyRunning)
	cancel()
	require.NoError(t, <-done)
	fc.mu.Lock()
	defer fc.mu.Unlock()
	assert.True(t, fc.flushed)
	assert.True(t, fc.closed)
	assert.True(t, s.Health().IsHealthy(), "failures while closing are dropped")
}

func TestSink_AckError_IsLogged(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 1})
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
		want   error
	}{
		{"no kafka section", func(c *config.Sink) { c.Kafka = nil }, ErrInvalidConfig},
		{"no name", func(c *config.Sink) { c.Name = "" }, ErrInvalidConfig},
		{"wrong type", func(c *config.Sink) { c.Type = config.SinkNATS }, ErrInvalidConfig},
		{"no brokers", func(c *config.Sink) { c.Kafka.Brokers = nil }, ErrInvalidConfig},
		{"bad prefix", func(c *config.Sink) { c.Kafka.TopicPrefix = "a.b" }, ErrInvalidConfig},
		{"zero partitions", func(c *config.Sink) { c.Kafka.Partitions = 0 }, ErrInvalidConfig},
		{"negative linger", func(c *config.Sink) { c.Linger = -1 }, ErrInvalidConfig},
		{"missing CA", func(c *config.Sink) { c.Kafka.TLS = config.TLS{Enabled: true, CAFile: missing} }, ErrInvalidConfig},
		{"missing key pair", func(c *config.Sink) {
			c.Kafka.TLS = config.TLS{Enabled: true, CertFile: missing, KeyFile: missing}
		}, ErrInvalidConfig},
		{"missing SASL env", func(c *config.Sink) {
			c.Kafka.SASL = config.SASL{Mechanism: config.SASLScramSHA256, UsernameEnv: "U", PasswordEnv: "P"}
		}, ErrMissingCredentials},
		{"bad mechanism", func(c *config.Sink) {
			c.Kafka.SASL = config.SASL{Mechanism: "PLAIN", UsernameEnv: "U", PasswordEnv: "P"}
		}, ErrInvalidConfig},
	}
	env := func(k string) string {
		if k == "U" || k == "P" {
			return ""
		}
		return "x"
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := baseConfig()
			tc.mutate(&cfg)
			_, err := New(cfg, env, clock.System{}, testutil.DiscardLogger())
			require.ErrorIs(t, err, tc.want)
		})
	}
	_, err := New(baseConfig(), nil, clock.System{}, testutil.DiscardLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNew_BadMechanismWithCredentials(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Kafka.SASL = config.SASL{Mechanism: "PLAIN", UsernameEnv: "U", PasswordEnv: "P"}
	_, err := New(cfg, func(string) string { return "x" }, clock.System{}, testutil.DiscardLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNew_TLSAndSCRAM(t *testing.T) {
	t.Parallel()
	b := tlsgen.Generate(t, "127.0.0.1", "localhost")
	for _, mech := range []config.SASLMechanism{config.SASLScramSHA256, config.SASLScramSHA512} {
		cfg := baseConfig()
		cfg.Kafka.TLS = config.TLS{Enabled: true, CAFile: b.CAFile, CertFile: b.ClientCertFile, KeyFile: b.ClientKeyFile}
		cfg.Kafka.SASL = config.SASL{Mechanism: mech, UsernameEnv: "U", PasswordEnv: "P"}
		s, err := New(cfg, func(string) string { return "secret" }, clock.System{}, testutil.DiscardLogger())
		require.NoError(t, err)
		require.NotNil(t, s.conn.tls)
		assert.NotNil(t, s.conn.tls.RootCAs)
		assert.Len(t, s.conn.tls.Certificates, 1)
		require.NotNil(t, s.conn.sasl)
		assert.Equal(t, string(mech), s.conn.sasl.Name())
		cl, err := kgo.NewClient(s.kgoOptions()...)
		require.NoError(t, err, "the options are accepted by franz-go")
		cl.Close()
	}
}

func TestNew_CAWithoutPEM(t *testing.T) {
	t.Parallel()
	b := tlsgen.Generate(t, "127.0.0.1")
	cfg := baseConfig()
	cfg.Kafka.TLS = config.TLS{Enabled: true, CAFile: b.ClientKeyFile}
	_, err := New(cfg, getenv, clock.System{}, testutil.DiscardLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestSink_BatchMaxBytes(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.MaxBytes = 64 << 10
	cfg.MaxEventBytes = 256 << 10
	s, err := New(cfg, getenv, clock.System{}, testutil.DiscardLogger())
	require.NoError(t, err)
	assert.Equal(t, int32(256<<10+batchOverhead), s.batchMaxBytes(""), "one max-size event fits")
	s.topics["t"] = topicInfo{exists: true, maxMessageBytes: 100_000}
	assert.Equal(t, int32(100_000), s.batchMaxBytes("t"), "the topic limit caps batches")
	s.topics["tiny"] = topicInfo{exists: true, maxMessageBytes: 100}
	assert.Equal(t, int32(minBatchBytes), s.batchMaxBytes("tiny"))
	cfg.MaxEventBytes = 1 << 40
	s, err = New(cfg, getenv, clock.System{}, testutil.DiscardLogger())
	require.NoError(t, err)
	assert.Equal(t, int32(maxBatchBytes), s.batchMaxBytes(""))
}

func TestEventSizeAndReason(t *testing.T) {
	t.Parallel()
	rec := event(change.OpCreate, 12, "42")
	want := len(rec.Key) + len(rec.Payload) + len(HeaderEventID) + len("0/16B3748-12") + len(HeaderSchemaVersion) + 1
	assert.Equal(t, int64(want), eventSize(&rec))
	rec.Key = []byte(strings.Repeat("k", 300))
	reason := tooLargeReason(&rec, 1000, 10)
	assert.Contains(t, reason, strings.Repeat("k", maxReasonKey))
	assert.NotContains(t, reason, strings.Repeat("k", maxReasonKey+1))
	msg := pipeline.NewMessageRecord(&change.Message{ID: change.MessageID(1, 0), Prefix: "p", Content: []byte("c")})
	assert.Contains(t, tooLargeReason(&msg, 1, 0), "table _messages")
}

func TestSink_TopicWithoutPartitions_Retrying(t *testing.T) {
	t.Parallel()
	fc := newFakeClient(true)
	fc.setTopic(usersTopic, topicInfo{exists: true})
	h := startSink(t, baseConfig(), fc)

	h.send(t, event(change.OpTruncate, 0, ""), 1)
	hl := h.waitHealth(t, pipeline.SinkStateRetrying)
	assert.Contains(t, hl.Reason, "no partitions")
	assert.Empty(t, fc.snapshot())
	fc.setTopic(usersTopic, topicInfo{exists: true, partitions: 2})
	h.waitAcks(t, 1)
	assert.Len(t, fc.snapshot(), 2)
}
