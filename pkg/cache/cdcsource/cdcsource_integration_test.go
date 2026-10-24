// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cdcsource

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
	"github.com/thnhdatle/wake/internal/testutil/natstest"
	"github.com/thnhdatle/wake/internal/testutil/redistest"
	"github.com/thnhdatle/wake/pkg/cache"
)

const (
	wait     = 90 * time.Second
	pollTick = 20 * time.Millisecond
)

func uniqueName(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	tick := time.NewTicker(pollTick)
	defer tick.Stop()
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		<-tick.C
	}
}

type cluster struct {
	deleter cache.Cache[string]
	readers []cache.Cache[string]
}

func newCluster(t *testing.T, ns string, client func() redis.UniversalClient) *cluster {
	t.Helper()
	open := func() cache.Cache[string] {
		c, err := cache.New[string](ns, cache.JSONCodec[string](), cache.WithL2(client()),
			cache.WithDegradedL1TTL(2*time.Second))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		return c
	}
	cl := &cluster{deleter: open()}
	for range 3 {
		cl.readers = append(cl.readers, open())
	}
	waitUntil(t, "readers observe a probe invalidation", func() bool {
		cl.warm(t, "probe")
		require.NoError(t, cl.deleter.Delete(t.Context(), "probe"))
		return cl.cachedBy("probe") == 0
	})
	return cl
}

func (c *cluster) warm(t *testing.T, keys ...string) {
	t.Helper()
	for _, r := range c.readers {
		for _, k := range keys {
			v, err := r.GetOrLoad(t.Context(), k, func(context.Context) (string, error) { return "v:" + k, nil })
			require.NoError(t, err)
			require.Equal(t, "v:"+k, v)
		}
	}
}

func (c *cluster) cachedBy(key string) int {
	n := 0
	for _, r := range c.readers {
		if _, ok, _ := r.Get(context.Background(), key); ok {
			n++
		}
	}
	return n
}

func (c *cluster) cachedAny(keys ...string) bool {
	for _, k := range keys {
		if c.cachedBy(k) > 0 {
			return true
		}
	}
	return false
}

func runInvalidator(t *testing.T, src Source, m *Mapping, del Deleter, reg prometheus.Registerer) *Invalidator {
	t.Helper()
	inv, err := New(src, m, del, WithMetrics(reg), WithRetry(50*time.Millisecond, 500*time.Millisecond),
		WithLogger(slog.New(slog.NewTextHandler(testWriter{t}, nil))))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- inv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
	return inv
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

func userEvents(n int) (payloads [][]byte, keys []string) {
	for i := 1; i <= n; i++ {
		payloads = append(payloads, evt("u", "public.users", fmt.Sprintf(`{"id":%d}`, i),
			fmt.Sprintf(`{"id":%d,"email":"old%d@example.com"}`, i, i),
			fmt.Sprintf(`{"id":%d,"email":"new%d@example.com"}`, i, i)))
		keys = append(keys, fmt.Sprintf("user:%d", i), fmt.Sprintf("user:email:old%d@example.com", i),
			fmt.Sprintf("user:email:new%d@example.com", i))
	}
	return payloads, keys
}

func produce(t *testing.T, cl *kgo.Client, topic string, payloads ...[]byte) {
	t.Helper()
	for i, p := range payloads {
		rec := &kgo.Record{Topic: topic, Key: fmt.Appendf(nil, `{"id":%d}`, i), Value: p}
		require.NoError(t, cl.ProduceSync(t.Context(), rec).FirstErr())
	}
}

func committed(t *testing.T, k *kafkatest.Kafka, group, topic string) int64 {
	t.Helper()
	resps, err := k.Admin(t).FetchOffsets(t.Context(), group)
	require.NoError(t, err)
	var sum int64
	for _, r := range resps[topic] {
		require.NoError(t, r.Err)
		sum += r.At
	}
	return sum
}

//nolint:paralleltest,tparallel // see above
func TestIntegration_Kafka(t *testing.T) {
	user := kafkatest.User{Name: "inval", Password: "secret42"}
	k := kafkatest.Start(t, kafkatest.WithTLSAndSCRAM(user))
	srv := redistest.StartSingle(t)
	kafkaSource := func(t *testing.T, prefix, group string, m *Mapping) *KafkaSource {
		t.Helper()
		src, err := NewKafkaSource(KafkaConfig{
			Brokers: k.Brokers, TopicPrefix: prefix, Group: group, TLS: k.TLS.ClientTLS(t),
			SASL: SASL{Mechanism: SASLScramSHA256, User: user.Name, Password: user.Password},
		}, m)
		require.NoError(t, err)
		return src
	}

	t.Run("three instances", func(t *testing.T) {
		prefix, group := uniqueName("cdc"), uniqueName("inval")
		topic := prefix + ".public.users"
		k.CreateTopic(t, topic, 3, nil)
		ns := uniqueName("users")
		cl := newCluster(t, ns, func() redis.UniversalClient { return srv.Client(t) })
		payloads, keys := userEvents(20)
		cl.warm(t, keys...)
		require.Equal(t, 3, cl.cachedBy(keys[0]))
		m := mustMapping(t, usersMapping)
		reg := prometheus.NewRegistry()
		inv := runInvalidator(t, kafkaSource(t, prefix, group, m), m, cl.deleter, reg)

		produce(t, k.Client(t), topic, append(payloads, nil)...)

		waitUntil(t, "keys gone from every reader", func() bool { return !cl.cachedAny(keys...) })
		waitUntil(t, "offsets committed", func() bool { return committed(t, k, group, topic) == int64(len(payloads)+1) })
		assert.InDelta(t, 1, counterValue(t, reg, "wake_cdcsource_events_skipped_total",
			map[string]string{"reason": "tombstone"}), 0)
		require.NoError(t, inv.Ready(t.Context()))
	})

	t.Run("redis outage defers the commit", func(t *testing.T) {
		prefix, group := uniqueName("cdc"), uniqueName("inval")
		topic := prefix + ".public.users"
		k.CreateTopic(t, topic, 1, nil)
		ns := uniqueName("users")
		cl := newCluster(t, ns, func() redis.UniversalClient { return srv.Client(t) })
		payloads, keys := userEvents(1)
		cl.warm(t, keys...)
		m := mustMapping(t, usersMapping)
		reg := prometheus.NewRegistry()
		runInvalidator(t, kafkaSource(t, prefix, group, m), m, cl.deleter, reg)
		produce(t, k.Client(t), topic, evt("c", "public.users", `{"id":0}`, "null", `{"id":0,"email":"z@example.com"}`))
		waitUntil(t, "first event committed", func() bool { return committed(t, k, group, topic) == 1 })

		srv.Stop(t)
		produce(t, k.Client(t), topic, payloads...)
		failures := func() float64 {
			return counterValue(t, reg, "wake_cdcsource_delete_failures_total", map[string]string{"kind": "deferred"}) +
				counterValue(t, reg, "wake_cdcsource_delete_failures_total", map[string]string{"kind": "error"})
		}
		waitUntil(t, "Delete retried while Redis is down", func() bool { return failures() >= 3 })
		assert.Equal(t, int64(1), committed(t, k, group, topic), "no commit while Delete fails")

		srv.Start(t)
		waitUntil(t, "commit after recovery", func() bool { return committed(t, k, group, topic) == 2 })
		assert.InDelta(t, 3+2, counterValue(t, reg, "wake_cdcsource_keys_deleted_total", nil), 0)
		waitUntil(t, "keys invalid after recovery", func() bool { return !cl.cachedAny(keys...) })
	})
}

func TestIntegration_NATS(t *testing.T) {
	t.Parallel()
	n := natstest.Start(t, natstest.WithTLS(), natstest.WithCredentials())
	srv := redistest.StartSingle(t)
	js := n.JetStream(t)
	stream := uniqueName("wake")
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: stream, Subjects: []string{"cdc.>"}})
	require.NoError(t, err)
	cl := newCluster(t, uniqueName("users"), func() redis.UniversalClient { return srv.Client(t) })
	payloads, keys := userEvents(20)
	cl.warm(t, keys...)
	m := mustMapping(t, usersMapping)
	durable := uniqueName("inval")
	src, err := NewNATSSource(NATSConfig{
		URL: n.URL, SubjectPrefix: "cdc", Stream: stream, Durable: durable,
		CredentialsFile: n.CredsFile, TLS: n.TLS.ClientTLS(t),
	}, m)
	require.NoError(t, err)
	inv := runInvalidator(t, src, m, cl.deleter, prometheus.NewRegistry())

	for i, p := range payloads {
		_, perr := js.Publish(t.Context(), "cdc.public.users", p, jetstream.WithMsgID(fmt.Sprintf("0/1-%d", i)))
		require.NoError(t, perr)
	}
	_, err = js.Publish(t.Context(), "cdc.public.orders", evt("c", "public.orders", `{"id":1}`, "null", `{"id":1}`))
	require.NoError(t, err)

	waitUntil(t, "keys gone from every reader", func() bool { return !cl.cachedAny(keys...) })
	waitUntil(t, "every message acknowledged", func() bool {
		cons, err := js.Consumer(t.Context(), stream, durable)
		require.NoError(t, err)
		info, err := cons.Info(t.Context())
		require.NoError(t, err)
		return info.AckFloor.Consumer == uint64(len(payloads)) && info.NumAckPending == 0 && info.NumPending == 0
	})
	waitUntil(t, "source healthy", func() bool { return inv.Ready(t.Context()) == nil })
}

func TestIntegration_TLSRedis(t *testing.T) {
	t.Parallel()
	srv := redistest.StartTLS(t)
	cl := newCluster(t, uniqueName("users"), func() redis.UniversalClient { return srv.Client(t) })
	payloads, keys := userEvents(5)
	cl.warm(t, keys...)
	m := mustMapping(t, usersMapping)

	runInvalidator(t, &fakeSource{batches: [][][]byte{payloads}}, m, cl.deleter, prometheus.NewRegistry())

	waitUntil(t, "keys gone from every reader", func() bool { return !cl.cachedAny(keys...) })
}
