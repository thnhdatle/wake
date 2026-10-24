// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
)

func TestCache_GetOrLoad_SharedThroughL2(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	calls := 0
	v, err := a.GetOrLoad(ctx, "k", constLoader(5, &calls))
	require.NoError(t, err)
	assert.Equal(t, 5, v)
	v, err = b.GetOrLoad(ctx, "k", constLoader(6, &calls))
	require.NoError(t, err)
	assert.Equal(t, 5, v, "b reads a's value from L2")
	assert.Equal(t, 1, calls)
	got, ok := b.l1Peek("k")
	assert.True(t, ok, "L2 hit populates L1")
	assert.Equal(t, 5, got)
	sv, ok := e.srv.Value(testNS, a.epoch.Load(), "k")
	require.True(t, ok)
	g, _ := e.srv.Gen(testNS, a.epoch.Load(), "k")
	assert.Equal(t, g, sv.Gen)
	ttl, _ := e.srv.GenTTL(testNS, a.epoch.Load(), "k")
	assert.Equal(t, defaultTTL+defaultLockTimeout+genGrace, ttl, "generation TTL = value TTL + lock_timeout + 1 min")
	assert.False(t, e.srv.Locked(testNS, a.epoch.Load(), "k"))
}

func TestCache_GetOrLoad_NegativeCaching(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		opts       []Option
		wantCalls  int
		wantCached bool
	}{
		{"with negative ttl", []Option{WithNegativeTTL(time.Minute)}, 1, true},
		{"without negative ttl", nil, 3, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			a, b := e.instance(tc.opts...), e.instance(tc.opts...)
			ctx := t.Context()
			calls := 0
			notFound := func(context.Context) (int, error) {
				calls++
				return 0, ErrNotFound
			}
			_, err := a.GetOrLoad(ctx, "k", notFound)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = b.GetOrLoad(ctx, "k", notFound)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = a.GetOrLoad(ctx, "k", notFound)
			require.ErrorIs(t, err, ErrNotFound)
			assert.Equal(t, tc.wantCalls, calls)
			_, ok, err := b.Get(ctx, "k")
			require.NoError(t, err)
			assert.False(t, ok, "a negative entry is a miss for Get")
			_, cached := e.srv.Value(testNS, a.epoch.Load(), "k")
			assert.Equal(t, tc.wantCached, cached)
		})
	}
}

func TestCache_GetOrLoad_NegativeExpires(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance(WithNegativeTTL(time.Second))
	ctx := t.Context()
	_, err := a.GetOrLoad(ctx, "k", func(context.Context) (int, error) { return 0, ErrNotFound })
	require.ErrorIs(t, err, ErrNotFound)
	e.clk.Advance(time.Second)
	calls := 0
	v, err := a.GetOrLoad(ctx, "k", constLoader(3, &calls))
	require.NoError(t, err)
	assert.Equal(t, 3, v)
}

type blockingLoader struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	value   int
	err     error
}

func newBlockingLoader(v int) *blockingLoader {
	return &blockingLoader{entered: make(chan struct{}, 16), release: make(chan struct{}), value: v}
}

func (b *blockingLoader) load(ctx context.Context) (int, error) {
	b.calls.Add(1)
	b.entered <- struct{}{}
	select {
	case <-b.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return b.value, b.err
}

func TestCache_GetOrLoad_WaiterWakesOnLoaded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	holder := newBlockingLoader(42)
	done := make(chan int, 1)
	go func() {
		v, _ := a.GetOrLoad(ctx, "k", holder.load)
		done <- v
	}()
	<-holder.entered
	waiterCalls := 0
	got := make(chan int, 1)
	go func() {
		v, _ := b.GetOrLoad(ctx, "k", constLoader(-1, &waiterCalls))
		got <- v
	}()
	eventually(t, func() bool { return metricValue(b.m.lockWaits) == 1 })
	close(holder.release)
	assert.Equal(t, 42, <-done)
	assert.Equal(t, 42, <-got)
	assert.Zero(t, waiterCalls)
	assert.Zero(t, metricValue(b.m.fallbackLoads))
}

func TestCache_GetOrLoad_WaiterPollsWhenBusSilent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	holder := newBlockingLoader(42)
	go func() { _, _ = a.GetOrLoad(ctx, "k", holder.load) }()
	<-holder.entered
	b.cli.PauseBus()
	waiterCalls := 0
	got := make(chan int, 1)
	go func() {
		v, _ := b.GetOrLoad(ctx, "k", constLoader(-1, &waiterCalls))
		got <- v
	}()
	eventually(t, func() bool { return metricValue(b.m.lockWaits) == 1 })
	close(holder.release)
	eventually(t, func() bool { _, ok := e.srv.Value(testNS, a.epoch.Load(), "k"); return ok })
	var v int
	e.advanceUntil(pollInitial, func() bool {
		select {
		case v = <-got:
			return true
		default:
			return false
		}
	})
	assert.Equal(t, 42, v)
	assert.Zero(t, waiterCalls)
	b.cli.ResumeBus()
}

func TestCache_GetOrLoad_FallbackAfterLockTimeout(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	b := e.instance()
	ctx := t.Context()
	_, _, lockKey := l2.KeyNames(testNS, b.epoch.Load(), "k")
	e.srv.RawSet(lockKey, "stuck")
	calls := 0
	got := make(chan int, 1)
	go func() {
		v, _ := b.GetOrLoad(ctx, "k", constLoader(2, &calls))
		got <- v
	}()
	eventually(t, func() bool { return metricValue(b.m.lockWaits) == 1 })
	var v int
	e.advanceUntil(time.Second, func() bool {
		select {
		case v = <-got:
			return true
		default:
			return false
		}
	})
	assert.Equal(t, 2, v)
	assert.Equal(t, 1, calls)
	assert.InDelta(t, 1, metricValue(b.m.fallbackLoads), 0)
	sv, ok := e.srv.Value(testNS, b.epoch.Load(), "k")
	require.True(t, ok, "fallback write is generation-checked and succeeds while the generation is unchanged")
	assert.Equal(t, []byte{0, '2'}, sv.Payload)
	assert.True(t, e.srv.Locked(testNS, b.epoch.Load(), "k"), "a fallback load never releases another holder's lock")
}

func TestCache_GetOrLoad_LoaderErrorReleasesLock(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	holder := newBlockingLoader(0)
	holder.err = errors.New("db down")
	errc := make(chan error, 1)
	go func() {
		_, err := a.GetOrLoad(ctx, "k", holder.load)
		errc <- err
	}()
	<-holder.entered
	calls := 0
	got := make(chan int, 1)
	go func() {
		v, _ := b.GetOrLoad(ctx, "k", constLoader(9, &calls))
		got <- v
	}()
	eventually(t, func() bool { return metricValue(b.m.lockWaits) == 1 })
	close(holder.release)
	require.ErrorContains(t, <-errc, "db down")
	assert.Equal(t, 9, <-got, "the waiter takes the released lock and loads")
	assert.Equal(t, 1, calls)
	assert.Zero(t, metricValue(b.m.fallbackLoads))
}

func TestCache_GetOrLoad_SingleflightInProcess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	loader := newBlockingLoader(7)
	var wg sync.WaitGroup
	results := make(chan int, 10)
	for range 10 {
		wg.Go(func() {
			v, err := a.GetOrLoad(ctx, "k", loader.load)
			assert.NoError(t, err)
			results <- v
		})
	}
	<-loader.entered
	close(loader.release)
	wg.Wait()
	close(results)
	for v := range results {
		assert.Equal(t, 7, v)
	}
	assert.Equal(t, int32(1), loader.calls.Load())
}

func TestCache_GetOrLoad_LeaderCanceledFollowerRetries(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	leaderCtx, cancel := context.WithCancel(t.Context())
	loader := newBlockingLoader(7)
	leaderErr := make(chan error, 1)
	go func() {
		_, err := a.GetOrLoad(leaderCtx, "k", loader.load)
		leaderErr <- err
	}()
	<-loader.entered
	got := make(chan int, 1)
	go func() {
		v, _ := a.GetOrLoad(t.Context(), "k", loader.load)
		got <- v
	}()
	eventually(t, func() bool { return metricValue(a.m.l1Misses) == 2 })
	cancel()
	require.ErrorIs(t, <-leaderErr, context.Canceled)
	<-loader.entered
	close(loader.release)
	assert.Equal(t, 7, <-got)
}

func TestCache_GetOrLoad_CallerCanceledWhileWaiting(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	holder := newBlockingLoader(1)
	go func() { _, _ = a.GetOrLoad(t.Context(), "k", holder.load) }()
	<-holder.entered
	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() {
		_, err := b.GetOrLoad(ctx, "k", func(context.Context) (int, error) { return 0, nil })
		errc <- err
	}()
	eventually(t, func() bool { return metricValue(b.m.lockWaits) == 1 })
	cancel()
	require.ErrorIs(t, <-errc, context.Canceled)
	close(holder.release)
	e.idle()
}

func TestCache_GetOrLoad_StaleIfErrorWithHealthyL2(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance(WithTTL(time.Minute, 0), WithStaleIfError(time.Minute))
	ctx := t.Context()
	calls := 0
	_, err := a.GetOrLoad(ctx, "k", constLoader(1, &calls))
	require.NoError(t, err)
	e.clk.Advance(90 * time.Second)
	failing := func(context.Context) (int, error) { return 0, errors.New("db down") }
	v, err := a.GetOrLoad(ctx, "k", failing)
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	assert.InDelta(t, 1, metricValue(a.m.staleServes), 0)
	e.clk.Advance(30 * time.Second)
	_, err = a.GetOrLoad(ctx, "k", failing)
	require.ErrorContains(t, err, "db down", "beyond stale_if_error")
}

func TestCache_CorruptL2Value(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	vkey, _, _ := l2.KeyNames(testNS, a.epoch.Load(), "k")
	r, err := a.cli.Read(ctx, l2.ReadRequest{Epoch: a.epoch.Load(), Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	e.srv.RawSet(vkey, string(l2.EncodeValue(l2.StoredValue{
		Gen: r.Gen, ExpiresAtMS: e.clk.Now().Add(time.Hour).UnixMilli(), Payload: []byte{0, '{'},
	})))
	_, _, err = a.Get(ctx, "k")
	require.Error(t, err, "Get reports an undecodable value")
	calls := 0
	v, err := a.GetOrLoad(ctx, "k", constLoader(4, &calls))
	require.NoError(t, err)
	assert.Equal(t, 4, v, "GetOrLoad treats it as a miss and overwrites it")
	v2, ok, err := a.Get(ctx, "k")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 4, v2)
}

func TestCache_GetOrLoad_L2FailuresFallBackToLocalLoad(t *testing.T) {
	t.Parallel()
	for _, op := range []l2fake.Op{l2fake.OpRead, l2fake.OpLock} {
		t.Run(string(op), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			a := e.instance()
			boom := errors.New("timeout")
			a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, o l2fake.Op, _ string) error {
				if o == op {
					return boom
				}
				return nil
			}})
			calls := 0
			v, err := a.GetOrLoad(t.Context(), "k", constLoader(3, &calls))
			require.NoError(t, err)
			assert.Equal(t, 3, v)
			assert.InDelta(t, 1, metricValue(a.m.l2Errors), 0)
			e.clk.Advance(defaultDegradedTTL)
			_, ok := a.l1Get("k")
			assert.False(t, ok, "a value loaded without an L2 generation lives at most degraded_l1_ttl")
		})
	}
}

func TestCache_GetOrLoad_HolderRecheckFindsValue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, op l2fake.Op, _ string) error {
		if op == l2fake.OpLock {
			a.cli.SetHooks(l2fake.Hooks{})
			require.NoError(t, b.Set(ctx, "k", 8))
		}
		return nil
	}})
	calls := 0
	v, err := a.GetOrLoad(ctx, "k", constLoader(1, &calls))
	require.NoError(t, err)
	assert.Equal(t, 8, v)
	assert.Zero(t, calls)
	assert.False(t, e.srv.Locked(testNS, a.epoch.Load(), "k"))
}

func TestCache_GetOrLoad_FillFailureStillReturnsValue(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, op l2fake.Op, _ string) error {
		if op == l2fake.OpFill {
			return errors.New("timeout")
		}
		return nil
	}})
	calls := 0
	v, err := a.GetOrLoad(t.Context(), "k", constLoader(6, &calls))
	require.NoError(t, err)
	assert.Equal(t, 6, v)
	_, ok := a.l1Peek("k")
	assert.False(t, ok, "nothing cached when the write failed")
}

func TestCache_Get_L2Failure_IsMiss(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	require.NoError(t, a.Set(t.Context(), "k", 1))
	a.l1.Flush()
	a.cli.SetHooks(l2fake.Hooks{Before: func(context.Context, l2fake.Op, string) error { return errors.New("timeout") }})
	_, ok, err := a.Get(t.Context(), "k")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCache_GetOrLoad_HolderFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		failOp l2fake.Op
		nth    int32
		prefil bool
	}{
		{"holder re-check read fails", l2fake.OpRead, 2, false},
		{"unlock after re-check hit fails", l2fake.OpUnlock, 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			a, b := e.instance(), e.instance()
			ctx := t.Context()
			var n atomic.Int32
			a.cli.SetHooks(l2fake.Hooks{Before: func(_ context.Context, op l2fake.Op, _ string) error {
				if tc.prefil && op == l2fake.OpLock {
					require.NoError(t, b.Set(ctx, "k", 5))
				}
				if op == tc.failOp && n.Add(1) == tc.nth {
					return errors.New("timeout")
				}
				return nil
			}})
			calls := 0
			v, err := a.GetOrLoad(ctx, "k", constLoader(5, &calls))
			require.NoError(t, err)
			assert.Equal(t, 5, v)
			assert.InDelta(t, 1, metricValue(a.m.l2Errors), 0)
		})
	}
}
