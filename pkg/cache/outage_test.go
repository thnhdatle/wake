// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
)

func openBreaker(t *testing.T, n node) {
	t.Helper()
	n.cli.SetDown(true)
	for range breakerThreshold {
		_, _, err := n.Get(t.Context(), "probe")
		require.NoError(t, err, "an L2 failure is a miss, never an error")
	}
	require.False(t, n.breaker.allow())
	assert.InDelta(t, float64(breakerOpen), metricValue(n.m.breakerState), 0)
}

func TestCache_Breaker_OpensAndRecovers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	require.NoError(t, a.Set(ctx, "k", 1))
	a.l1.Flush()
	openBreaker(t, a)
	reads := a.cli.Calls(l2fake.OpRead)
	_, ok, err := a.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, reads, a.cli.Calls(l2fake.OpRead), "no L2 call while open")

	epochCalls := a.cli.Calls(l2fake.OpEpoch)
	e.advanceUntil(probeInterval, func() bool { return a.cli.Calls(l2fake.OpEpoch) > epochCalls+1 })
	assert.False(t, a.breaker.allow())

	a.cli.SetDown(false)
	e.advanceUntil(probeInterval, func() bool { return a.breaker.allow() })
	assert.InDelta(t, float64(breakerClosed), metricValue(a.m.breakerState), 0)
	eventually(t, func() bool { return a.busUp.Load() })
	got, ok, err := a.Get(ctx, "k")
	require.NoError(t, err)
	assert.True(t, ok, "L2 used again after recovery")
	assert.Equal(t, 1, got)
}

func TestCache_Breaker_IgnoresCallerCancellation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 2 * breakerThreshold {
		_, _, _ = a.getL2(ctx, "k")
	}
	assert.True(t, a.breaker.allow())
	assert.Zero(t, metricValue(a.m.l2Errors))
}

func TestCache_Delete_DeferredAndReplayed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	require.NoError(t, a.Set(ctx, "k", 1))
	epoch := a.epoch.Load()
	gen, _ := e.srv.Gen(testNS, epoch, "k")
	openBreaker(t, a)

	err := a.Delete(ctx, "k")
	require.ErrorIs(t, err, ErrDeferred)
	_, ok := a.l1Peek("k")
	assert.False(t, ok, "deleted locally at once")
	assert.True(t, a.pending.blocks("k"))
	assert.InDelta(t, 1, metricValue(a.m.deferred), 0)
	assert.InDelta(t, 1, metricValue(a.m.pendingDepth), 0)
	_, stillValid := e.srv.Value(testNS, epoch, "k")
	assert.True(t, stillValid, "not applied to L2 yet")

	a.cli.SetDown(false)
	e.advanceUntil(probeInterval, func() bool { return !a.pending.active.Load() })
	after, _ := e.srv.Gen(testNS, epoch, "k")
	assert.Greater(t, after, gen, "replay incremented the generation")
	_, stillValid = e.srv.Value(testNS, epoch, "k")
	assert.False(t, stillValid)
	e.idle()
	_, ok = b.l1Peek("k")
	assert.False(t, ok, "replay published the invalidation")
	require.NoError(t, a.Delete(ctx, "k"))
}

func TestCache_Delete_SingleFailureDefersWithoutOpenBreaker(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	var fail bool
	a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, op l2fake.Op, _ string) error {
		if op == l2fake.OpInvalidate && !fail {
			fail = true
			return errors.New("timeout")
		}
		return nil
	}})
	require.ErrorIs(t, a.Delete(ctx, "k", "j"), ErrDeferred)
	assert.True(t, a.breaker.allow())
	e.advanceUntil(probeInterval, func() bool { return !a.pending.active.Load() })
}

func TestCache_Delete_DeferralDisabled(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance(WithMaxPendingInvalidations(0))
	openBreaker(t, a)
	err := a.Delete(t.Context(), "k")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrDeferred)
	require.ErrorIs(t, err, errL2Unavailable)
	assert.False(t, a.pending.active.Load())
}

func TestCache_Delete_CanceledContextIsNotDeferred(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx, cancel := context.WithCancel(t.Context())
	a.cli.SetHooks(l2fake.Hooks{Before: func(context.Context, l2fake.Op, string) error {
		cancel()
		return context.Canceled
	}})
	err := a.Delete(ctx, "k")
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrDeferred)
	err = a.Set(ctx, "k", 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCache_Delete_OverflowBumpsEpochOnRecovery(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(WithMaxPendingInvalidations(2)), e.instance()
	ctx := t.Context()
	for _, k := range []string{"x", "y", "z"} {
		require.NoError(t, b.Set(ctx, k, 1))
	}
	old := a.epoch.Load()
	openBreaker(t, a)
	require.ErrorIs(t, a.Delete(ctx, "x", "y", "z"), ErrDeferred)
	assert.InDelta(t, 1, metricValue(a.m.pendingOverflows), 0)
	assert.True(t, a.pending.blocks("never-deleted"), "after an overflow every key may be stale in L2")

	a.cli.SetDown(false)
	e.advanceUntil(probeInterval, func() bool { return !a.pending.active.Load() })
	e.idle()
	assert.Greater(t, a.epoch.Load(), old)
	assert.Equal(t, a.epoch.Load(), b.epoch.Load(), "every instance follows the epoch")
	_, ok, err := b.Get(ctx, "y")
	require.NoError(t, err)
	assert.False(t, ok, "old-epoch values are invisible")
}

func TestCache_Set_DeferredWhileUnreachable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	require.NoError(t, a.Set(ctx, "k", 1))
	openBreaker(t, a)
	err := a.Set(ctx, "k", 2)
	require.ErrorIs(t, err, ErrDeferred)
	_, ok := a.l1Peek("k")
	assert.False(t, ok, "a deferred Set leaves no local value")
	a.cli.SetDown(false)
	e.advanceUntil(probeInterval, func() bool { return !a.pending.active.Load() })
	_, ok = e.srv.Value(testNS, a.epoch.Load(), "k")
	assert.False(t, ok, "the old L2 value was invalidated on recovery")
}

func TestCache_PendingKey_BypassesL2(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	require.NoError(t, a.Set(ctx, "k", 1))
	e.idle()
	a.pending.add("k")
	a.l1.Flush()
	calls := 0
	v, err := a.GetOrLoad(ctx, "k", constLoader(2, &calls))
	require.NoError(t, err)
	assert.Equal(t, 2, v, "this instance never reads a value whose invalidation is pending")
	got, ok, err := a.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 2, got, "the locally loaded value, not L2's")
	e.clk.Advance(defaultDegradedTTL)
	_, ok, err = a.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "L2 still bypassed for the pending key; the local value expired")
}

func TestCache_Outage_StaleIfError(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance(WithTTL(10*time.Second, 0), WithStaleIfError(5*time.Minute))
	ctx := t.Context()
	calls := 0
	_, err := a.GetOrLoad(ctx, "k", constLoader(1, &calls))
	require.NoError(t, err)
	openBreaker(t, a)
	e.clk.Advance(20 * time.Second)
	failing := func(context.Context) (int, error) { return 0, errors.New("db down") }
	v, err := a.GetOrLoad(ctx, "k", failing)
	require.NoError(t, err, "0 errors while a stale value is available")
	assert.Equal(t, 1, v)
	e.clk.Advance(5 * time.Minute)
	_, err = a.GetOrLoad(ctx, "k", failing)
	require.Error(t, err, "stale age never exceeds stale_if_error")

	v, err = a.GetOrLoad(ctx, "other", constLoader(3, &calls))
	require.NoError(t, err, "loads still work L1-only")
	assert.Equal(t, 3, v)
	_, err = a.GetOrLoad(ctx, "nf", func(context.Context) (int, error) { return 0, ErrNotFound })
	require.ErrorIs(t, err, ErrNotFound)
}

func TestCache_Outage_NegativeCachedLocally(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance(WithNegativeTTL(time.Minute))
	openBreaker(t, a)
	calls := 0
	nf := func(context.Context) (int, error) {
		calls++
		return 0, ErrNotFound
	}
	for range 2 {
		_, err := a.GetOrLoad(t.Context(), "k", nf)
		require.ErrorIs(t, err, ErrNotFound)
	}
	assert.Equal(t, 1, calls)
}

func TestCache_Degraded_CapsL1Age(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		degrade func(n node)
	}{
		{"redis down", func(n node) { n.cli.SetDown(true); _, _, _ = n.Get(context.Background(), "x") }},
		{"bus down", func(n node) { n.cli.SetBusDown(true) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			a := e.instance(WithTTL(time.Hour, 0), WithDegradedL1TTL(5*time.Second))
			ctx := t.Context()
			require.NoError(t, a.Set(ctx, "k", 1))
			e.clk.Advance(6 * time.Second)
			_, ok := a.l1Get("k")
			require.True(t, ok)
			tc.degrade(a)
			eventually(t, a.degraded)
			_, ok = a.l1Get("k")
			assert.False(t, ok, "entries older than degraded_l1_ttl are not served while invalidations may be missed")
			calls := 0
			_, err := a.GetOrLoad(ctx, "fresh", constLoader(2, &calls))
			require.NoError(t, err)
			e.clk.Advance(5 * time.Second)
			_, ok = a.l1Get("fresh")
			assert.False(t, ok)
		})
	}
}

func TestCache_BusReconnect_FlushesL1AndRereadsEpoch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	require.NoError(t, a.Set(ctx, "k", 1))
	require.NoError(t, a.Set(ctx, "j", 1))
	e.idle()
	a.cli.SetBusDown(true)
	eventually(t, func() bool { return !a.busUp.Load() })
	assert.InDelta(t, 0, metricValue(a.m.busUp), 0)
	require.NoError(t, b.Delete(ctx, "k"))
	admin := e.admin()
	next, err := admin.BumpEpoch(ctx)
	require.NoError(t, err)
	_, cached := a.l1Peek("j")
	require.True(t, cached)

	a.cli.SetBusDown(false)
	eventually(t, func() bool { return a.busUp.Load() })
	e.idle()
	assert.Zero(t, a.l1.Len(), "L1 flushed on reconnect")
	assert.Equal(t, next, a.epoch.Load(), "epoch re-read on reconnect")
}

func TestCache_BusDown_EpochRefreshedByMaintenance(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	a.cli.SetBusDown(true)
	eventually(t, func() bool { return !a.busUp.Load() })
	next, err := e.admin().BumpEpoch(t.Context())
	require.NoError(t, err)
	e.advanceUntil(probeInterval, func() bool { return a.epoch.Load() == next })
	a.cli.SetBusDown(false)
}

func TestCache_Epoch_ReadFailureOnConnect(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	a.cli.SetBusDown(true)
	eventually(t, func() bool { return !a.busUp.Load() })
	a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, op l2fake.Op, _ string) error {
		if op == l2fake.OpEpoch {
			return errors.New("timeout")
		}
		return nil
	}})
	a.cli.SetBusDown(false)
	eventually(t, func() bool { return a.busUp.Load() })
	assert.False(t, a.epochOK.Load())
	_, ok, err := a.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.False(t, ok)
	a.cli.SetHooks(l2fake.Hooks{})
	_, _, err = a.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.True(t, a.epochOK.Load(), "epoch read on demand")
}

func TestCache_Overflow_AddsDuringBumpSurvive(t *testing.T) {
	t.Parallel()
	p := newPendingSet(1)
	p.add("a")
	assert.True(t, p.add("b"))
	_, overflow, seq := p.snapshot()
	require.True(t, overflow)
	p.add("c")
	p.resolveUpTo(seq)
	assert.True(t, p.blocks("anything"), "overflow kept for the next round")
	_, _, seq = p.snapshot()
	p.resolveUpTo(seq)
	assert.False(t, p.active.Load())
	p.add("d")
	s := p.seq()
	p.add("d")
	p.removeUpTo("d", s)
	assert.True(t, p.blocks("d"))
}

type flakyBus struct {
	*l2fake.Client
	calls atomic.Int32
}

func (f *flakyBus) Receive(ctx context.Context) (l2.Event, error) {
	if f.calls.Add(1) == 2 {
		return l2.Event{}, errors.New("protocol error")
	}
	return f.Client.Receive(ctx)
}

func TestCache_Subscriber_UnexpectedErrorResyncs(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var bus *flakyBus
	c, err := New[int](testNS, JSONCodec[int](), WithClock(e.clk), withBackend(func(cfg l2.Config) (l2.Backend, error) {
		bus = &flakyBus{Client: e.srv.Client(cfg)}
		return bus, nil
	}))
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	n := asImpl(t, c)
	eventually(t, func() bool { return bus.calls.Load() >= 2 && !n.busUp.Load() })
	ctx := t.Context()
	n.l1.Set("stale", l1Entry(1))
	_, err = e.admin().Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: "other", GenTTL: time.Hour})
	require.NoError(t, err)
	e.advanceUntil(busRetryDelay, func() bool { return n.busUp.Load() })
	e.idle()
	assert.Zero(t, n.l1.Len(), "the first event after the error triggered a flush")
	assert.True(t, n.epochOK.Load())
}

func TestCache_Replay_StopsAtFirstFailure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	openBreaker(t, a)
	require.ErrorIs(t, a.Delete(ctx, "x", "y"), ErrDeferred)
	a.cli.SetDown(false)
	var failures atomic.Int32
	a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, op l2fake.Op, _ string) error {
		if op == l2fake.OpInvalidate && failures.Add(1) <= 2 {
			return errors.New("timeout")
		}
		return nil
	}})
	e.advanceUntil(probeInterval, func() bool { return !a.pending.active.Load() })
	assert.GreaterOrEqual(t, failures.Load(), int32(3))
}

func TestCache_Overflow_BumpFailureRetried(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance(WithMaxPendingInvalidations(1))
	ctx := t.Context()
	old := a.epoch.Load()
	openBreaker(t, a)
	require.ErrorIs(t, a.Delete(ctx, "x", "y"), ErrDeferred)
	a.cli.SetDown(false)
	var failures atomic.Int32
	a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, op l2fake.Op, _ string) error {
		if op == l2fake.OpBumpEpoch && failures.Add(1) == 1 {
			return errors.New("timeout")
		}
		return nil
	}})
	e.advanceUntil(probeInterval, func() bool { return !a.pending.active.Load() })
	assert.Greater(t, a.epoch.Load(), old)
}
