// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ns   string
		opts []Option
	}{
		{"empty namespace", "", nil},
		{"colon in namespace", "a:b", nil},
		{"brace in namespace", "a{b}", nil},
		{"space in namespace", "a b", nil},
		{"unicode namespace", "café", nil},
		{"l1 entries", "ok", []Option{WithL1(0, 1)}},
		{"l1 bytes", "ok", []Option{WithL1(1, 0)}},
		{"ttl", "ok", []Option{WithTTL(0, 0)}},
		{"jitter negative", "ok", []Option{WithTTL(time.Second, -1)}},
		{"jitter not below ttl", "ok", []Option{WithTTL(time.Second, time.Second)}},
		{"negative ttl", "ok", []Option{WithNegativeTTL(0)}},
		{"stale", "ok", []Option{WithStaleIfError(0)}},
		{"bus", "ok", []Option{WithInvalidationBus("")}},
		{"lock timeout", "ok", []Option{WithLockTimeout(0)}},
		{"degraded ttl", "ok", []Option{WithDegradedL1TTL(-time.Second)}},
		{"max pending", "ok", []Option{WithMaxPendingInvalidations(-1)}},
		{"compression", "ok", []Option{WithCompression(0)}},
		{"metrics", "ok", []Option{WithMetrics(nil)}},
		{"clock", "ok", []Option{WithClock(nil)}},
		{"backend", "ok", []Option{withBackend(nil)}},
		{"redis client", "ok", []Option{WithL2(nil)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New[int](tc.ns, JSONCodec[int](), tc.opts...)
			require.ErrorIs(t, err, errInvalidConfig)
		})
	}
	_, err := New[int]("ok", nil)
	require.ErrorIs(t, err, errInvalidConfig)
}

func TestNew_ValidNamespaces(t *testing.T) {
	t.Parallel()
	for _, ns := range []string{"a", "Users_v2", "orders-eu", "0"} {
		t.Run(ns, func(t *testing.T) {
			t.Parallel()
			c, err := New[int](ns, JSONCodec[int]())
			require.NoError(t, err)
			require.NoError(t, c.Close())
		})
	}
}

func TestNew_BackendFactoryError(t *testing.T) {
	t.Parallel()
	boom := errors.New("dial")
	_, err := New[int]("ns", JSONCodec[int](), withBackend(func(l2.Config) (l2.Backend, error) { return nil, boom }))
	require.ErrorIs(t, err, boom)
}

func TestNew_BackendGetsNamespaceChannelAndHealthCheck(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var got []l2.Config
	for _, opts := range [][]Option{nil, {WithInvalidationBus("custom"), WithDegradedL1TTL(time.Second)}} {
		all := append([]Option{WithClock(e.clk), withBackend(func(cfg l2.Config) (l2.Backend, error) {
			got = append(got, cfg)
			return e.srv.Client(cfg), nil
		})}, opts...)
		c, err := New[int]("users", JSONCodec[int](), all...)
		require.NoError(t, err)
		require.NoError(t, c.Close())
	}
	want := []l2.Config{
		{Namespace: "users", Channel: "wake:inval:users", BusHealthCheck: 2500 * time.Millisecond},
		{Namespace: "users", Channel: "custom", BusHealthCheck: 500 * time.Millisecond},
	}
	assert.Equal(t, want, got)
}

func TestNew_MetricsRegistrationConflict(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{Name: "wake_cache_hits_total", Help: "clash"}))
	_, err := New[int]("ns", JSONCodec[int](), WithMetrics(reg))
	require.Error(t, err)
}

func TestCache_L1Only_GetOrLoadSetDelete(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(start)
	c, err := New[int]("ns", JSONCodec[int](), WithClock(clk), WithTTL(time.Minute, 0))
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	ctx := t.Context()

	calls := 0
	v, err := c.GetOrLoad(ctx, "k", constLoader(7, &calls))
	require.NoError(t, err)
	assert.Equal(t, 7, v)
	v, err = c.GetOrLoad(ctx, "k", constLoader(8, &calls))
	require.NoError(t, err)
	assert.Equal(t, 7, v, "served from L1")
	assert.Equal(t, 1, calls)

	got, ok, err := c.Get(ctx, "k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 7, got)

	require.NoError(t, c.Set(ctx, "k", 9))
	got, _, _ = c.Get(ctx, "k")
	assert.Equal(t, 9, got)

	require.NoError(t, c.Delete(ctx, "k", "absent"))
	_, ok, err = c.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, c.Set(ctx, "short", 1, WithSetTTL(time.Second)))
	clk.Advance(time.Second)
	_, ok, _ = c.Get(ctx, "short")
	assert.False(t, ok, "WithSetTTL overrides WithTTL")
	clk.Advance(time.Minute)
	_, ok, _ = c.Get(ctx, "k")
	assert.False(t, ok)
	require.ErrorIs(t, c.Set(ctx, "k", 1, WithSetTTL(0)), errInvalidConfig)
}

func TestCache_GetOrLoad_NilLoader(t *testing.T) {
	t.Parallel()
	c, err := New[int]("ns", JSONCodec[int]())
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	_, err = c.GetOrLoad(t.Context(), "k", nil)
	require.ErrorIs(t, err, errInvalidConfig)
}

func TestCache_Get_CanceledContextOnMiss(t *testing.T) {
	t.Parallel()
	c, err := New[int]("ns", JSONCodec[int]())
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = c.Get(ctx, "k")
	require.ErrorIs(t, err, context.Canceled)
}

func TestCache_Close_IdempotentAndRejectsCalls(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	n := e.instance()
	require.NoError(t, n.Close())
	require.NoError(t, n.Close())
	ctx := t.Context()
	_, _, err := n.Get(ctx, "k")
	require.ErrorIs(t, err, errClosed)
	_, err = n.GetOrLoad(ctx, "k", nil)
	require.ErrorIs(t, err, errClosed)
	require.ErrorIs(t, n.Set(ctx, "k", 1), errClosed)
	require.ErrorIs(t, n.Delete(ctx, "k"), errClosed)
}

type failingCodec struct{ Codec[int] }

func (failingCodec) Marshal(int) ([]byte, error) { return nil, errors.New("marshal") }

func TestCache_CodecErrors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	n := e.instance()
	ctx := t.Context()
	c, err := New[int]("ns", failingCodec{JSONCodec[int]()})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	require.Error(t, c.Set(ctx, "k", 1))
	_, err = c.GetOrLoad(ctx, "k", func(context.Context) (int, error) { return 1, nil })
	require.Error(t, err)

	n.codec = failingCodec{JSONCodec[int]()}
	_, err = n.GetOrLoad(ctx, "k", func(context.Context) (int, error) { return 1, nil })
	require.Error(t, err)
	assert.False(t, e.srv.Locked(testNS, n.epoch.Load(), "k"), "lock released after an encode error")
}

func TestCodecs_RoundTrip(t *testing.T) {
	t.Parallel()
	type rec struct {
		ID   int64  `json:"id" msgpack:"id"`
		Name string `json:"name" msgpack:"name"`
	}
	in := rec{ID: 1<<62 + 1, Name: "Ada"}
	for name, codec := range map[string]Codec[rec]{"json": JSONCodec[rec](), "msgpack": MsgpackCodec[rec]()} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, err := codec.Marshal(in)
			require.NoError(t, err)
			out, err := codec.Unmarshal(b)
			require.NoError(t, err)
			assert.Equal(t, in, out)
			_, err = codec.Unmarshal([]byte{0xc1, '{'})
			require.Error(t, err)
		})
	}
	t.Run("protobuf", func(t *testing.T) {
		t.Parallel()
		codec := ProtobufCodec[*wrapperspb.StringValue]()
		b, err := codec.Marshal(wrapperspb.String("Ada"))
		require.NoError(t, err)
		out, err := codec.Unmarshal(b)
		require.NoError(t, err)
		assert.Equal(t, "Ada", out.GetValue())
		_, err = codec.Unmarshal([]byte{0xff})
		require.Error(t, err)
	})
	t.Run("json marshal error", func(t *testing.T) {
		t.Parallel()
		_, err := JSONCodec[chan int]().Marshal(make(chan int))
		require.Error(t, err)
		_, err = MsgpackCodec[func()]().Marshal(func() {})
		require.Error(t, err)
	})
}

func TestEnvelope_Compression(t *testing.T) {
	t.Parallel()
	plain, err := newEnvelope(0)
	require.NoError(t, err)
	defer plain.close()
	zip, err := newEnvelope(16)
	require.NoError(t, err)
	defer zip.close()

	big := []byte(strings.Repeat("abcdefgh", 100))
	sealed := zip.seal(big, false)
	assert.Equal(t, flagZstd, sealed[0])
	assert.Less(t, len(sealed), len(big))
	for _, env := range []*envelope{plain, zip} {
		raw, neg, openErr := env.open(sealed)
		require.NoError(t, openErr, "every instance reads compressed payloads")
		assert.False(t, neg)
		assert.Equal(t, big, raw)
	}
	small := zip.seal([]byte("tiny"), false)
	assert.Equal(t, byte(0), small[0])
	_, neg, err := plain.open(plain.seal(nil, true))
	require.NoError(t, err)
	assert.True(t, neg)
	_, _, err = plain.open(nil)
	require.ErrorIs(t, err, errMalformedValue)
	_, _, err = plain.open([]byte{flagZstd, 1, 2, 3})
	require.Error(t, err)
}

func TestCache_Compression_EndToEnd(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance(WithCompression(1))
	b := e.instance()
	ctx := t.Context()
	require.NoError(t, a.Set(ctx, "k", 123456789))
	v, ok := e.srv.Value(testNS, a.epoch.Load(), "k")
	require.True(t, ok)
	assert.Equal(t, flagZstd, v.Payload[0])
	got, ok, err := b.Get(ctx, "k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 123456789, got)
}

func TestCache_Metrics_SharedRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	e := newEnv(t)
	a := e.instance(WithMetrics(reg))
	b := e.instance(WithMetrics(reg))
	other, err := New[int]("other", JSONCodec[int](), WithMetrics(reg))
	require.NoError(t, err)
	defer func() { require.NoError(t, other.Close()) }()
	ctx := t.Context()
	calls := 0
	_, err = a.GetOrLoad(ctx, "k", constLoader(1, &calls))
	require.NoError(t, err)
	_, err = a.GetOrLoad(ctx, "k", constLoader(1, &calls))
	require.NoError(t, err)
	_, _, err = b.Get(ctx, "k")
	require.NoError(t, err)
	require.NoError(t, a.Delete(ctx, "k"))
	e.idle()

	assert.InDelta(t, 1, metricValue(a.m.l1Hits), 0)
	assert.InDelta(t, 2, metricValue(a.m.l1Misses), 0, "a and b share the namespace series")
	assert.InDelta(t, 1, metricValue(a.m.l2Hits), 0)
	assert.InDelta(t, 1, metricValue(a.m.l2Misses), 0)
	assert.InDelta(t, 1, metricValue(a.m.loads), 0)
	assert.InDelta(t, 2, metricValue(a.m.invalidationsReceived), 0)
	assert.InDelta(t, 1, metricValue(a.m.busUp), 0)
	families, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{"wake_cache_hits_total", "wake_cache_breaker_state", "wake_cache_epoch_changes_total"} {
		assert.True(t, names[want], want)
	}
}

//nolint:paralleltest // goleak.VerifyNone must not see other tests' goroutines
func TestNew_CloseStopsGoroutines(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	e := newEnv(t)
	n := e.instance()
	_, err := n.GetOrLoad(t.Context(), "k", func(context.Context) (int, error) { return 1, nil })
	require.NoError(t, err)
	n.cli.SetBusDown(true)
	eventually(t, func() bool { return !n.busUp.Load() })
	require.NoError(t, n.Close())
}

func TestWithL2_StartsDegradedWhileRedisIsUnreachable(t *testing.T) {
	t.Parallel()
	rdb := redis.NewClient(&redis.Options{
		Addr: "offline:6379", MaxRetries: -1, DialerRetries: 1,
		Dialer: func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("offline") },
	})
	t.Cleanup(func() { _ = rdb.Close() })
	c, err := New[int]("users", JSONCodec[int](), WithL2(rdb))
	require.NoError(t, err)
	n := asImpl(t, c)
	ctx := t.Context()
	calls := 0
	for range breakerThreshold + 1 {
		v, err := c.GetOrLoad(ctx, "k", constLoader(7, &calls))
		require.NoError(t, err)
		assert.Equal(t, 7, v)
		n.l1.Flush()
	}
	assert.False(t, n.breaker.allow(), "breaker opened")
	assert.False(t, n.busUp.Load())
	require.ErrorIs(t, c.Delete(ctx, "k"), ErrDeferred)
	require.NoError(t, c.Close())
}
