// SPDX-License-Identifier: Apache-2.0

//go:build integration

package kafkasink_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/adapter/kafkasink"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
)

const (
	userEnv   = "WAKE_TEST_KAFKA_USER"
	passEnv   = "WAKE_TEST_KAFKA_PASSWORD"
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

func (a *ackCounter) snapshot() map[uint64]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[uint64]int, len(a.acked))
	for k, v := range a.acked {
		out[k] = v
	}
	return out
}

type env struct {
	k      *kafkatest.Kafka
	sink   *kafkasink.Sink
	acks   *ackCounter
	prefix string
}

func sinkConfig(k *kafkatest.Kafka, prefix string) config.Sink {
	cfg := config.Sink{
		Name: "kafka-" + prefix, Type: config.SinkKafka, Tables: []string{"*"},
		MaxEvents: 1000, MaxBytes: 1 << 20, Linger: config.Duration(5 * time.Millisecond),
		Kafka: &config.KafkaSink{
			Brokers: k.Brokers, TopicPrefix: prefix, Tombstones: true, Partitions: -1, ReplicationFactor: -1,
		},
	}
	if k.TLS != nil {
		cfg.Kafka.TLS = config.TLS{
			Enabled: true, CAFile: k.TLS.CAFile, CertFile: k.TLS.ClientCertFile, KeyFile: k.TLS.ClientKeyFile,
		}
		cfg.Kafka.SASL = config.SASL{Mechanism: config.SASLScramSHA512, UsernameEnv: userEnv, PasswordEnv: passEnv}
	}
	return cfg
}

func credentials(user, pass string) func(string) string {
	return func(k string) string {
		switch k {
		case userEnv:
			return user
		case passEnv:
			return pass
		}
		return ""
	}
}

func runSink(t *testing.T, k *kafkatest.Kafka, cfg config.Sink, getenv func(string) string) *env {
	t.Helper()
	opts := []kafkasink.Option{
		kafkasink.WithBackoff(pipeline.Backoff{Initial: 50 * time.Millisecond, Max: 500 * time.Millisecond}),
		kafkasink.WithStallTimeout(2 * time.Second),
		kafkasink.WithRequestTimeout(5 * time.Second),
	}
	s, err := kafkasink.New(cfg, getenv, clock.System{}, testutil.DiscardLogger(), opts...)
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
	return &env{k: k, sink: s, acks: &ackCounter{acked: map[uint64]int{}}, prefix: cfg.Kafka.TopicPrefix}
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

func (e *env) topic(table string) string { return e.prefix + ".public." + table }

func rowEvent(op change.Op, tx uint64, table, key string) pipeline.Record {
	ev := &change.Event{
		ID:            change.StreamID(lsn.LSN(0x1000000+tx*0x100), 0),
		Source:        change.Source{DB: "app", Schema: "public", Table: table},
		Op:            op,
		TsMs:          time.Now().UnixMilli(),
		SchemaVersion: 1,
	}
	if key != "" {
		ev.Key = change.Row{{Name: "id", Value: []byte(key)}}
	}
	if op == change.OpCreate || op == change.OpUpdate {
		ev.After = change.Row{{Name: "id", Value: []byte(key)}, {Name: "ver", Value: []byte(strconv.FormatUint(tx, 10))}}
	}
	return pipeline.NewEventRecord(ev)
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func eventID(tx uint64) string { return change.StreamID(lsn.LSN(0x1000000+tx*0x100), 0).String() }

//nolint:paralleltest,tparallel // see above
func TestKafkaSink_Plain(t *testing.T) {
	k := kafkatest.Start(t)
	t.Run("records tombstones and truncate fan-out", func(t *testing.T) {
		t.Parallel()
		testRecords(t, k)
	})
	t.Run("schema and message records", func(t *testing.T) {
		t.Parallel()
		testSchemaAndMessages(t, k)
	})
	t.Run("oversized event blocks without ack", func(t *testing.T) {
		t.Parallel()
		testOversized(t, k)
	})
	t.Run("unknown topic recovers when created", func(t *testing.T) {
		t.Parallel()
		testUnknownTopic(t, k)
	})
	t.Run("topic limit recovers when raised", func(t *testing.T) {
		t.Parallel()
		testTopicLimit(t, k)
	})
	t.Run("auto-create topics", func(t *testing.T) {
		t.Parallel()
		testAutoCreate(t, k)
	})
	t.Run("per-key order under load", func(t *testing.T) {
		t.Parallel()
		testOrderUnderLoad(t, k)
	})
}

func testRecords(t *testing.T, k *kafkatest.Kafka) {
	t.Helper()
	e := runSink(t, k, sinkConfig(k, "rec"), credentials("", ""))
	k.CreateTopic(t, e.topic("users"), 3, nil)
	e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
	e.send(t, rowEvent(change.OpUpdate, 2, "users", "1"), 2)
	e.send(t, rowEvent(change.OpDelete, 3, "users", "2"), 3)
	e.send(t, rowEvent(change.OpTruncate, 4, "users", ""), 4)
	e.waitAcks(t, 4, longWait)

	recs := k.Consume(t, e.topic("users"), 4+1+2, longWait)
	require.Len(t, recs, 7, "c, u, d, tombstone and one t per partition")
	var tParts []int32
	byPartition := map[int32][]*kgo.Record{}
	for _, r := range recs {
		byPartition[r.Partition] = append(byPartition[r.Partition], r)
		if header(r, kafkasink.HeaderEventID) == eventID(4) {
			assert.Nil(t, r.Key)
			assert.Equal(t, "1", header(r, kafkasink.HeaderSchemaVersion))
			tParts = append(tParts, r.Partition)
		}
	}
	assert.ElementsMatch(t, []int32{0, 1, 2}, tParts, "the t event reaches every partition with the same id")
	for _, part := range byPartition {
		for i, r := range part {
			if header(r, kafkasink.HeaderEventID) != eventID(3) {
				continue
			}
			assert.Equal(t, `{"id":2}`, string(r.Key))
			require.Greater(t, len(part), i+1, "a tombstone follows the d event")
			tomb := part[i+1]
			assert.Equal(t, r.Key, tomb.Key)
			assert.Nil(t, tomb.Value)
			assert.Empty(t, tomb.Headers)
		}
	}
	for _, r := range recs {
		if header(r, kafkasink.HeaderEventID) == eventID(1) {
			assert.Equal(t, `{"id":1}`, string(r.Key))
			assert.Contains(t, string(r.Value), `"op":"c"`)
		}
	}
	assert.True(t, e.sink.Health().IsHealthy())
}

func testSchemaAndMessages(t *testing.T, k *kafkatest.Kafka) {
	t.Helper()
	e := runSink(t, k, sinkConfig(k, "meta"), credentials("", ""))
	k.CreateTopic(t, e.prefix+"._schema", 1, nil)
	k.CreateTopic(t, e.prefix+"._messages", 2, nil)
	sc := pipeline.NewSchemaChangeRecord(&change.SchemaChange{
		ID: change.SchemaID(0x2000000, 0), Source: change.Source{DB: "app", Schema: "public", Table: "users"},
		SchemaVersion: 2, TsMs: 1,
	})
	msg := pipeline.NewMessageRecord(&change.Message{
		ID: change.MessageID(0x2000100, 0), Prefix: "p", Content: []byte("c"),
	})
	e.send(t, sc, 1)
	e.send(t, msg, 2)
	e.waitAcks(t, 2, longWait)

	schema := k.Consume(t, e.prefix+"._schema", 1, longWait)
	require.Len(t, schema, 1)
	assert.JSONEq(t, `{"schema":"public","table":"users"}`, string(schema[0].Key))
	assert.Equal(t, "0/2000000-s0", header(schema[0], kafkasink.HeaderEventID))
	messages := k.Consume(t, e.prefix+"._messages", 1, longWait)
	require.Len(t, messages, 1)
	assert.Nil(t, messages[0].Key)
}

func testOversized(t *testing.T, k *kafkatest.Kafka) {
	t.Helper()
	cfg := sinkConfig(k, "big")
	cfg.MaxEventBytes = 1024
	e := runSink(t, k, cfg, credentials("", ""))
	k.CreateTopic(t, e.topic("users"), 1, nil)
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
	assert.Never(t, func() bool { return e.acks.count() > 1 }, 2*time.Second, 50*time.Millisecond,
		"nothing at or after the oversized event is acknowledged")
	recs := k.Consume(t, e.topic("users"), 2, 5*time.Second)
	assert.Len(t, recs, 1, "nothing after the oversized event reaches Kafka")
}

func testUnknownTopic(t *testing.T, k *kafkatest.Kafka) {
	t.Helper()
	e := runSink(t, k, sinkConfig(k, "unk"), credentials("", ""))
	e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
	h := e.waitHealth(t, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, h.Code)
	assert.Contains(t, h.Reason, "auto_create_topics is false")
	assert.Zero(t, e.acks.count())

	k.CreateTopic(t, e.topic("users"), 1, nil)
	e.waitAcks(t, 1, longWait)
	e.waitHealth(t, pipeline.SinkStateHealthy)
	assert.Len(t, k.Consume(t, e.topic("users"), 1, longWait), 1)
}

func testTopicLimit(t *testing.T, k *kafkatest.Kafka) {
	t.Helper()
	e := runSink(t, k, sinkConfig(k, "lim"), credentials("", ""))
	small := "2048"
	k.CreateTopic(t, e.topic("users"), 1, map[string]*string{"max.message.bytes": &small})
	big := rowEvent(change.OpCreate, 1, "users", "1")
	big.Payload = []byte(`{"blob":"` + strings.Repeat("y", 8192) + `"}`)
	e.send(t, big, 1)
	require.Eventually(t, func() bool {
		h := e.sink.Health()
		return h.IsFatal() && strings.Contains(h.Reason, "max.message.bytes 2048")
	}, longWait, 10*time.Millisecond, "health %+v", e.sink.Health())
	assert.Equal(t, pipeline.CodeSinkFatal, e.sink.Health().Code)

	large := "1048588"
	ctx, cancel := context.WithTimeout(t.Context(), shortWait)
	defer cancel()
	_, err := k.Admin(t).AlterTopicConfigs(ctx,
		[]kadm.AlterConfig{{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &large}}, e.topic("users"))
	require.NoError(t, err)
	e.waitAcks(t, 1, longWait)
	e.waitHealth(t, pipeline.SinkStateHealthy)
}

func testAutoCreate(t *testing.T, k *kafkatest.Kafka) {
	t.Helper()
	cfg := sinkConfig(k, "auto")
	cfg.Kafka.AutoCreateTopics = true
	cfg.Kafka.Partitions = 3
	cfg.Kafka.ReplicationFactor = 1
	e := runSink(t, k, cfg, credentials("", ""))
	e.send(t, rowEvent(change.OpCreate, 1, "orders", "1"), 1)
	e.waitAcks(t, 1, longWait)
	ctx, cancel := context.WithTimeout(t.Context(), shortWait)
	defer cancel()
	topics, err := k.Admin(t).ListTopics(ctx, e.topic("orders"))
	require.NoError(t, err)
	assert.Len(t, topics[e.topic("orders")].Partitions, 3)
}

func keyedLoad(t *testing.T, e *env, n, keys int, between func(i int)) map[string][]uint64 {
	t.Helper()
	want := map[string][]uint64{}
	for i := range n {
		tx := uint64(i + 1)
		key := strconv.Itoa(i % keys)
		e.send(t, rowEvent(change.OpUpdate, tx, "load", key), tx)
		want[`{"id":`+key+`}`] = append(want[`{"id":`+key+`}`], tx)
		if between != nil {
			between(i)
		}
	}
	return want
}

func checkOrder(t *testing.T, recs []*kgo.Record, want map[string][]uint64) {
	t.Helper()
	got := map[string][]string{}
	seen := map[string]bool{}
	for _, r := range recs {
		id := header(r, kafkasink.HeaderEventID)
		if seen[id] {
			continue
		}
		seen[id] = true
		got[string(r.Key)] = append(got[string(r.Key)], id)
	}
	violations := 0
	for key, txs := range want {
		ids := make([]string, len(txs))
		for i, tx := range txs {
			ids[i] = eventID(tx)
		}
		if !slices.Equal(ids, got[key]) {
			violations++
			if violations <= 3 {
				t.Errorf("key %s: got %d ids, want %d in order", key, len(got[key]), len(ids))
			}
		}
	}
	assert.Zero(t, violations, "keys with missing or reordered events")
}

func testOrderUnderLoad(t *testing.T, k *kafkatest.Kafka) {
	t.Helper()
	const n, keys = 20_000, 200
	e := runSink(t, k, sinkConfig(k, "ord"), credentials("", ""))
	k.CreateTopic(t, e.topic("load"), 6, nil)
	start := time.Now()
	want := keyedLoad(t, e, n, keys, nil)
	e.waitAcks(t, n, longWait)
	t.Logf("%d events acknowledged in %s", n, time.Since(start).Round(time.Millisecond))
	recs := k.Consume(t, e.topic("load"), n, longWait)
	require.Len(t, recs, n)
	checkOrder(t, recs, want)
}

//nolint:paralleltest // one broker at a time, see TestKafkaSink_Plain
func TestKafkaSink_OutageAndRestart(t *testing.T) {
	const n, keys = 3000, 50
	k := kafkatest.Start(t)
	e := runSink(t, k, sinkConfig(k, "out"), credentials("", ""))
	k.CreateTopic(t, e.topic("load"), 3, nil)

	var ackedAtOutage int
	want := keyedLoad(t, e, n, keys, func(i int) {
		switch i {
		case 999:
			e.waitAcks(t, 1000, longWait)
			k.Outage()
			ackedAtOutage = e.acks.count()
		case 1199:
			h := e.waitHealth(t, pipeline.SinkStateRetrying)
			assert.Contains(t, h.Reason, "no acknowledgement from Kafka")
			assert.Equal(t, ackedAtOutage, e.acks.count(), "nothing is acknowledged while Kafka is unreachable")
			k.Restore()
		case 1999:
			k.Restart(t)
		}
	})
	e.waitAcks(t, n, 3*longWait)
	e.waitHealth(t, pipeline.SinkStateHealthy)
	for tx, c := range e.acks.snapshot() {
		require.Equal(t, 1, c, "tx %d acknowledged exactly once", tx)
	}
	recs := k.Consume(t, e.topic("load"), n, longWait)
	require.GreaterOrEqual(t, len(recs), n)
	checkOrder(t, recs, want)
}

//nolint:paralleltest,tparallel // one broker at a time, see TestKafkaSink_Plain
func TestKafkaSink_TLSAndSCRAM(t *testing.T) {
	k := kafkatest.Start(t, kafkatest.WithTLSAndSCRAM(
		kafkatest.User{Name: "admin", Password: "adminsecret"},
		kafkatest.User{Name: "wake", Password: "wakesecret"},
	))
	t.Run("delivers with SCRAM-SHA-512 and SCRAM-SHA-256", func(t *testing.T) {
		t.Parallel()
		for i, mech := range []config.SASLMechanism{config.SASLScramSHA512, config.SASLScramSHA256} {
			cfg := sinkConfig(k, fmt.Sprintf("tls%d", i))
			cfg.Kafka.SASL.Mechanism = mech
			e := runSink(t, k, cfg, credentials("wake", "wakesecret"))
			k.CreateTopic(t, e.topic("users"), 1, nil)
			e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
			e.waitAcks(t, 1, longWait)
			recs := k.Consume(t, e.topic("users"), 1, longWait)
			require.Len(t, recs, 1)
			assert.Equal(t, `{"id":1}`, string(recs[0].Key))
		}
	})
	t.Run("rejected credentials recover without restart", func(t *testing.T) {
		t.Parallel()
		cfg := sinkConfig(k, "auth")
		e := runSink(t, k, cfg, credentials("late", "latesecret"))
		k.CreateTopic(t, e.topic("users"), 1, nil)
		e.send(t, rowEvent(change.OpCreate, 1, "users", "1"), 1)
		h := e.waitHealth(t, pipeline.SinkStateFatal)
		assert.Equal(t, pipeline.CodeSinkFatal, h.Code)
		assert.Contains(t, h.Reason, "SASL_AUTHENTICATION_FAILED")
		assert.Zero(t, e.acks.count())

		ctx, cancel := context.WithTimeout(t.Context(), shortWait)
		defer cancel()
		_, err := k.Admin(t).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{
			{User: "late", Mechanism: kadm.ScramSha512, Iterations: 4096, Password: "latesecret"},
		})
		require.NoError(t, err)
		e.waitAcks(t, 1, longWait)
		e.waitHealth(t, pipeline.SinkStateHealthy)
	})
}
