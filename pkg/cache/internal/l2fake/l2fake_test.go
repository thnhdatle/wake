// SPDX-License-Identifier: Apache-2.0

package l2fake_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var start = time.Unix(1_760_000_000, 0)

const ns = "users"

func newClient(t *testing.T) (*l2fake.Server, *l2fake.Client, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(start)
	srv := l2fake.NewServer(clk)
	cli := srv.Client(l2.Config{Namespace: ns, Channel: l2.DefaultChannel(ns)})
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	return srv, cli, clk
}

func recv(t *testing.T, cli *l2fake.Client) l2.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ev, err := cli.Receive(ctx)
	require.NoError(t, err)
	return ev
}

func TestClient_Read_InitializesGenerationFromTime(t *testing.T) {
	t.Parallel()
	srv, cli, _ := newClient(t)
	ctx := t.Context()
	r1, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Minute})
	require.NoError(t, err)
	assert.False(t, r1.Hit)
	assert.Equal(t, uint64(start.UnixMicro()), r1.Gen)
	r2, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Minute})
	require.NoError(t, err)
	assert.Equal(t, r1.Gen, r2.Gen)
	r3, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "other", GenTTL: time.Minute})
	require.NoError(t, err)
	assert.Greater(t, r3.Gen, r1.Gen)
	g, ok := srv.Gen(ns, 1, "k")
	require.True(t, ok)
	assert.Equal(t, r1.Gen, g)
}

func TestClient_Fill_GenerationChecked(t *testing.T) {
	t.Parallel()
	srv, cli, clk := newClient(t)
	ctx := t.Context()
	r, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)

	stale := l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen + 1, Payload: []byte("x"), TTL: time.Minute}
	stored, err := cli.Fill(ctx, stale)
	require.NoError(t, err)
	assert.False(t, stored, "stale generation must be rejected")

	stored, err = cli.Fill(ctx, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen, Payload: []byte("v"), TTL: time.Minute})
	require.NoError(t, err)
	assert.True(t, stored)
	hit, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	assert.True(t, hit.Hit)
	assert.Equal(t, []byte("v"), hit.Payload)
	assert.Equal(t, time.Minute, hit.TTL)

	clk.Advance(time.Minute)
	miss, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	assert.False(t, miss.Hit, "expired value")
	_, ok := srv.Value(ns, 1, "k")
	assert.False(t, ok)
}

func TestClient_Invalidate_BumpsGenerationAndPublishes(t *testing.T) {
	t.Parallel()
	srv, cli, _ := newClient(t)
	ctx := t.Context()
	require.Equal(t, l2.EventConnected, recv(t, cli).Kind)
	r, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	_, err = cli.Fill(ctx, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen, Payload: []byte("v"), TTL: time.Minute})
	require.NoError(t, err)
	assert.Equal(t, l2.Event{Kind: l2.EventLoaded, Epoch: 1, Key: "k"}, recv(t, cli))

	res, err := cli.Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, r.Gen+1, res.Gen)
	ep, ok := srv.EpochOf(ns)
	require.True(t, ok)
	assert.Equal(t, ep, res.Epoch)
	assert.Equal(t, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: r.Gen + 1, Key: "k"}, recv(t, cli))
	_, ok = srv.Value(ns, 1, "k")
	assert.False(t, ok)

	set, err := cli.Set(ctx, l2.SetRequest{Epoch: 1, Key: "k", Payload: []byte("s"), TTL: time.Minute, GenTTL: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, r.Gen+2, set.Gen)
	v, ok := srv.Value(ns, 1, "k")
	require.True(t, ok)
	assert.Equal(t, set.Gen, v.Gen)
	assert.Equal(t, l2.EventInvalidate, recv(t, cli).Kind)
}

func TestClient_GenTTL_ExtendedNeverShortened(t *testing.T) {
	t.Parallel()
	srv, cli, clk := newClient(t)
	ctx := t.Context()
	_, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	_, err = cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Minute})
	require.NoError(t, err)
	ttl, ok := srv.GenTTL(ns, 1, "k")
	require.True(t, ok)
	assert.Equal(t, time.Hour, ttl)
	clk.Advance(30 * time.Minute)
	_, err = cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	ttl, _ = srv.GenTTL(ns, 1, "k")
	assert.Equal(t, time.Hour, ttl)
}

func TestClient_Lock_NXAndCompareDelete(t *testing.T) {
	t.Parallel()
	srv, cli, clk := newClient(t)
	ctx := t.Context()
	ok, err := cli.Lock(ctx, 1, "k", "a", time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = cli.Lock(ctx, 1, "k", "b", time.Second)
	require.NoError(t, err)
	assert.False(t, ok)
	require.NoError(t, cli.Unlock(ctx, 1, "k", "b"))
	assert.True(t, srv.Locked(ns, 1, "k"), "wrong token must not release")
	require.NoError(t, cli.Unlock(ctx, 1, "k", "a"))
	assert.False(t, srv.Locked(ns, 1, "k"))

	ok, err = cli.Lock(ctx, 1, "k", "c", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	clk.Advance(time.Second)
	assert.False(t, srv.Locked(ns, 1, "k"), "PX expiry")

	ok, err = cli.Lock(ctx, 1, "k", "d", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	r, err := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	stored, err := cli.Fill(ctx, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen + 5, LockToken: "d", TTL: time.Second})
	require.NoError(t, err)
	assert.False(t, stored)
	assert.False(t, srv.Locked(ns, 1, "k"), "fill releases the lock even when rejected")
}

func TestClient_Epoch_InitAndBump(t *testing.T) {
	t.Parallel()
	srv, cli, _ := newClient(t)
	ctx := t.Context()
	require.Equal(t, l2.EventConnected, recv(t, cli).Kind)
	e, err := cli.Epoch(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(start.UnixMicro()), e)
	e2, err := cli.BumpEpoch(ctx)
	require.NoError(t, err)
	assert.Equal(t, e+1, e2)
	assert.Equal(t, l2.Event{Kind: l2.EventEpoch, Epoch: e2}, recv(t, cli))
	srv.Delete(l2.EpochKey(ns))
	e3, err := cli.Epoch(ctx)
	require.NoError(t, err)
	assert.Greater(t, e3, uint64(start.UnixMicro()))
}

func TestClient_SetDown_FailsCommandsAndDisconnectsBus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		down func(srv *l2fake.Server, cli *l2fake.Client, d bool)
	}{
		{"server", func(srv *l2fake.Server, _ *l2fake.Client, d bool) { srv.SetDown(d) }},
		{"client", func(_ *l2fake.Server, cli *l2fake.Client, d bool) { cli.SetDown(d) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, cli, _ := newClient(t)
			ctx := t.Context()
			require.Equal(t, l2.EventConnected, recv(t, cli).Kind)
			tc.down(srv, cli, true)
			_, err := cli.Epoch(ctx)
			require.ErrorIs(t, err, l2fake.ErrUnavailable)
			assert.Equal(t, l2.EventDisconnected, recv(t, cli).Kind)
			tc.down(srv, cli, false)
			assert.Equal(t, l2.EventConnected, recv(t, cli).Kind)
			_, err = cli.Epoch(ctx)
			require.NoError(t, err)
		})
	}
}

func TestClient_SetBusDown_LosesMessages(t *testing.T) {
	t.Parallel()
	srv, cli, _ := newClient(t)
	other := srv.Client(l2.Config{Namespace: ns, Channel: l2.DefaultChannel(ns)})
	t.Cleanup(func() { require.NoError(t, other.Close()) })
	ctx := t.Context()
	require.Equal(t, l2.EventConnected, recv(t, cli).Kind)
	cli.SetBusDown(true)
	_, err := other.Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: "lost", GenTTL: time.Hour})
	require.NoError(t, err)
	cli.SetBusDown(false)
	_, err = other.Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: "seen", GenTTL: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, l2.EventDisconnected, recv(t, cli).Kind)
	assert.Equal(t, l2.EventConnected, recv(t, cli).Kind)
	assert.Equal(t, "seen", recv(t, cli).Key)
}

func TestClient_Receive_OverflowDropsConnection(t *testing.T) {
	t.Parallel()
	srv, cli, _ := newClient(t)
	srv.SetQueueLimit(2)
	ctx := t.Context()
	require.Equal(t, l2.EventConnected, recv(t, cli).Kind)
	for _, k := range []string{"a", "b", "c"} {
		_, err := cli.Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: k, GenTTL: time.Hour})
		require.NoError(t, err)
	}
	assert.Equal(t, l2.EventDisconnected, recv(t, cli).Kind)
	assert.Equal(t, l2.EventConnected, recv(t, cli).Kind)
}

func TestClient_PauseBus_HoldsEventsUntilResume(t *testing.T) {
	t.Parallel()
	_, cli, _ := newClient(t)
	cli.PauseBus()
	got := make(chan l2.Event, 1)
	go func() {
		ev, _ := cli.Receive(t.Context())
		got <- ev
	}()
	select {
	case <-got:
		t.Fatal("event delivered while paused")
	default:
	}
	cli.ResumeBus()
	assert.Equal(t, l2.EventConnected, (<-got).Kind)
}

func TestClient_Receive_ContextAndClose(t *testing.T) {
	t.Parallel()
	_, cli, _ := newClient(t)
	require.Equal(t, l2.EventConnected, recv(t, cli).Kind)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := cli.Receive(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, cli.Close())
	_, err = cli.Receive(t.Context())
	require.ErrorIs(t, err, l2.ErrClosed)
	_, err = cli.Epoch(t.Context())
	require.ErrorIs(t, err, l2fake.ErrUnavailable)
	require.NoError(t, cli.WaitIdle(t.Context()))
}

func TestClient_Hooks_InjectFailuresAndCount(t *testing.T) {
	t.Parallel()
	_, cli, _ := newClient(t)
	boom := errors.New("timeout")
	var after []l2fake.Op
	cli.SetHooks(l2fake.Hooks{
		Before: func(_ context.Context, op l2fake.Op, _ string) error {
			if op == l2fake.OpLock {
				return boom
			}
			return nil
		},
		After: func(op l2fake.Op, _ string) { after = append(after, op) },
	})
	_, err := cli.Lock(t.Context(), 1, "k", "t", time.Second)
	require.ErrorIs(t, err, boom)
	_, err = cli.Epoch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []l2fake.Op{l2fake.OpEpoch}, after)
	assert.Equal(t, 1, cli.Calls(l2fake.OpLock))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = cli.Epoch(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestServer_WaitIdle_AllSubscribers(t *testing.T) {
	t.Parallel()
	srv, cli, _ := newClient(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := cli.Receive(t.Context()); err != nil {
				return
			}
		}
	}()
	_, err := cli.Invalidate(t.Context(), l2.InvalidateRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	require.NoError(t, srv.WaitIdle(t.Context()))
	require.NoError(t, cli.Close())
	<-done
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, srv.WaitIdle(ctx), "no subscribers left")
}

func TestServer_RawSet_CorruptValueIsMiss(t *testing.T) {
	t.Parallel()
	srv, cli, _ := newClient(t)
	v, _, _ := l2.KeyNames(ns, 1, "k")
	srv.RawSet(v, "garbage")
	r, err := cli.Read(t.Context(), l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	assert.False(t, r.Hit)
	_, ok := srv.Value(ns, 1, "k")
	assert.False(t, ok)
	_, ok = srv.Gen(ns, 2, "absent")
	assert.False(t, ok)
	_, ok = srv.GenTTL(ns, 2, "absent")
	assert.False(t, ok)
	_, ok = srv.EpochOf("none")
	assert.False(t, ok)
}

func TestClient_Generation_NeverRepeatsProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		clk := clock.NewFake(start)
		srv := l2fake.NewServer(clk)
		cli := srv.Client(l2.Config{Namespace: ns, Channel: "c"})
		defer func() { _ = cli.Close() }()
		ctx := context.Background()
		var last uint64
		seen := map[uint64]bool{}
		for range rapid.IntRange(1, 50).Draw(rt, "steps") {
			var gen uint64
			switch rapid.IntRange(0, 3).Draw(rt, "op") {
			case 0:
				r, _ := cli.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Second})
				gen = r.Gen
			case 1:
				r, _ := cli.Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: "k", GenTTL: time.Second})
				gen = r.Gen
			case 2:
				clk.Advance(time.Duration(rapid.Int64Range(0, int64(3*time.Second)).Draw(rt, "dt")))
				continue
			default:
				_, g, _ := l2.KeyNames(ns, 1, "k")
				srv.Delete(g)
				continue
			}
			if gen < last || (gen != last && seen[gen]) {
				rt.Fatalf("generation %d after %d", gen, last)
			}
			seen[gen] = true
			last = gen
		}
	})
}
