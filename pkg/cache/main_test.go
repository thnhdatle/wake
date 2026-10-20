// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
	"github.com/thnhdatle/wake/pkg/cache/l1"
)

// TestMain fails the package when any goroutine started by New (bus
// subscriber, maintenance) outlives Close.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var start = time.Unix(1_760_000_000, 0)

const testNS = "test"

// env is one fake Redis shared by several cache instances and a fake clock
// driving both the instances and the server.
type env struct {
	t   *testing.T
	clk *clock.Fake
	srv *l2fake.Server
}

func newEnv(t *testing.T) *env {
	t.Helper()
	clk := clock.NewFake(start)
	return &env{t: t, clk: clk, srv: l2fake.NewServer(clk)}
}

// node is one cache instance with its fake connection.
type node struct {
	*cache[int]
	cli *l2fake.Client
}

// instance creates a cache[int] on the shared server and waits until its
// bus is connected and its epoch known.
func (e *env) instance(opts ...Option) node {
	e.t.Helper()
	var cli *l2fake.Client
	all := append([]Option{
		WithClock(e.clk),
		withBackend(func(cfg l2.Config) (l2.Backend, error) {
			cli = e.srv.Client(cfg)
			return cli, nil
		}),
	}, opts...)
	c, err := New[int](testNS, JSONCodec[int](), all...)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { require.NoError(e.t, c.Close()) })
	n := node{cache: asImpl(e.t, c), cli: cli}
	eventually(e.t, func() bool { return n.busUp.Load() && n.epochOK.Load() })
	require.NoError(e.t, cli.WaitIdle(e.t.Context()))
	return n
}

// admin returns a client outside any cache (an operator tool) whose bus
// subscription is drained so WaitIdle does not wait on it.
func (e *env) admin() *l2fake.Client {
	e.t.Helper()
	cli := e.srv.Client(l2.Config{Namespace: testNS, Channel: l2.DefaultChannel(testNS)})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := cli.Receive(context.Background()); err != nil {
				return
			}
		}
	}()
	e.t.Cleanup(func() {
		require.NoError(e.t, cli.Close())
		<-done
	})
	return cli
}

// idle waits until every instance has handled every published message.
func (e *env) idle() {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(e.t, e.srv.WaitIdle(ctx))
}

// advanceUntil advances the fake clock in steps until cond holds, so
// goroutines waiting on clock.After make progress regardless of when they
// registered their timer.
func (e *env) advanceUntil(step time.Duration, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		if time.Now().After(deadline) {
			e.t.Fatal("condition not reached while advancing the clock")
		}
		e.clk.Advance(step)
		<-tick.C
	}
}

// eventually polls cond until it holds (real time, bounded).
func eventually(tb testing.TB, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatal("condition not reached")
		}
		<-tick.C
	}
}

// constLoader returns a loader that always returns v and counts calls.
func constLoader(v int, calls *int) LoadFunc[int] {
	return func(context.Context) (int, error) {
		*calls++
		return v, nil
	}
}

// l1Peek reads L1 without degrading caps or metrics.
func (n node) l1Peek(key string) (int, bool) {
	it, ok := n.l1.Get(key)
	return it.v, ok && !it.neg
}

// metricValue returns the value of a counter or gauge.
func metricValue(c prometheus.Metric) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		panic(err)
	}
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}

// l1Entry is an L1 entry holding v without expiry.
func l1Entry(v int) l1.Entry[item[int]] {
	return l1.Entry[item[int]]{Value: item[int]{v: v}, Size: 1}
}

// asImpl returns the implementation behind a Cache.
func asImpl[V any](tb testing.TB, c Cache[V]) *cache[V] {
	tb.Helper()
	impl, ok := c.(*cache[V])
	require.True(tb, ok)
	return impl
}
