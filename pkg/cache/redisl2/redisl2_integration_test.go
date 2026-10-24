// SPDX-License-Identifier: Apache-2.0

//go:build integration

package redisl2_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/testutil/redistest"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/redisl2"
)

const eventWait = 15 * time.Second

func newBackend(t *testing.T, rdb redis.UniversalClient, ns string) *redisl2.Backend {
	t.Helper()
	b, err := redisl2.New(rdb, l2.Config{Namespace: ns, Channel: l2.DefaultChannel(ns), BusHealthCheck: busHealthCheck})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	return b
}

func next(t *testing.T, b l2.Backend) l2.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), eventWait)
	defer cancel()
	ev, err := b.Receive(ctx)
	require.NoError(t, err)
	return ev
}

func expectKind(t *testing.T, b l2.Backend, want l2.EventKind) {
	t.Helper()
	require.Equal(t, want, next(t, b).Kind)
}

func exerciseAll(t *testing.T, b *redisl2.Backend, key string) {
	t.Helper()
	ctx := t.Context()
	epoch, err := b.Epoch(ctx)
	require.NoError(t, err)
	r, err := b.Read(ctx, l2.ReadRequest{Epoch: epoch, Key: key, GenTTL: time.Hour})
	require.NoError(t, err)
	ok, err := b.Lock(ctx, epoch, key, "tok", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	stored, err := b.Fill(ctx, l2.FillRequest{
		Epoch: epoch, Key: key, Gen: r.Gen, Payload: []byte("v"), TTL: time.Minute, GenTTL: time.Hour, LockToken: "tok",
	})
	require.NoError(t, err)
	require.True(t, stored)
	require.NoError(t, b.Unlock(ctx, epoch, key, "tok"))
	set, err := b.Set(ctx, l2.SetRequest{
		Epoch: epoch, Key: key, Payload: []byte("s"), TTL: time.Minute, GenTTL: time.Hour,
	})
	require.NoError(t, err)
	assert.Equal(t, l2.WriteResult{Gen: r.Gen + 1, Epoch: epoch}, set)
	inv, err := b.Invalidate(ctx, l2.InvalidateRequest{Epoch: epoch, Key: key, GenTTL: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, l2.WriteResult{Gen: r.Gen + 2, Epoch: epoch}, inv)
	bumped, err := b.BumpEpoch(ctx)
	require.NoError(t, err)
	assert.Equal(t, epoch+1, bumped)
}

func singleClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	return redistest.StartSingle(t).Client(t)
}

func clusterClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	return redistest.StartCluster(t).Client(t)
}

func flushScripts(t *testing.T, rdb redis.UniversalClient) {
	t.Helper()
	require.NoError(t, forEachNode(t.Context(), rdb, func(ctx context.Context, n *redis.Client) error {
		return n.ScriptFlush(ctx).Err()
	}))
}

func TestBackend_ScriptCacheFlushed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		client func(t *testing.T) redis.UniversalClient
	}{
		{"single", singleClient},
		{"cluster", clusterClient},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rdb := tc.client(t)
			b := newBackend(t, rdb, "flush")
			expectKind(t, b, l2.EventConnected)
			for i := range 3 {
				flushScripts(t, rdb)
				exerciseAll(t, b, fmt.Sprintf("key-%d", i))
			}
		})
	}
}

func TestBus_DeadLinkAndRestart(t *testing.T) {
	t.Parallel()
	srv := redistest.StartSingle(t)
	rdb := srv.Client(t)
	b := newBackend(t, rdb, "outage")
	expectKind(t, b, l2.EventConnected)
	ctx := t.Context()
	set, err := b.Set(ctx, l2.SetRequest{Epoch: 1, Key: "k", Payload: []byte("v"), TTL: time.Hour, GenTTL: time.Hour})
	require.NoError(t, err)
	expectKind(t, b, l2.EventInvalidate)

	const freeze = 5 * time.Second
	frozenAt := time.Now()
	done := srv.Freeze(t, freeze)
	expectKind(t, b, l2.EventDisconnected)
	assert.Less(t, time.Since(frozenAt), freeze-time.Second, "detected by the health check, not by the freeze ending")
	require.NoError(t, <-done)
	expectKind(t, b, l2.EventConnected)

	srv.Stop(t)
	expectKind(t, b, l2.EventDisconnected)
	_, err = b.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.ErrorIs(t, err, redisl2.ErrUnavailable)
	srv.Start(t)
	expectKind(t, b, l2.EventConnected)
	r, err := b.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err, "EVAL fallback after the restart emptied the script cache")
	assert.True(t, r.Hit)
	assert.Equal(t, set.Gen, r.Gen, "generation survived the restart")
	inv, err := b.Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: "k", GenTTL: time.Hour})
	require.NoError(t, err)
	assert.Equal(t, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: inv.Gen, Key: "k"}, next(t, b))
}

func TestCluster_KeysSpreadOverEveryShard(t *testing.T) {
	t.Parallel()
	c := redistest.StartCluster(t)
	rdb := c.Client(t)
	b := newBackend(t, rdb, "spread")
	expectKind(t, b, l2.EventConnected)
	ctx := t.Context()
	for i := range 60 {
		key := fmt.Sprintf("user:%d", i)
		res, err := b.Set(ctx, l2.SetRequest{Epoch: 1, Key: key, Payload: []byte(key), TTL: time.Hour, GenTTL: time.Hour})
		require.NoError(t, err)
		assert.Equal(t, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: res.Gen, Key: key}, next(t, b))
		r, err := b.Read(ctx, l2.ReadRequest{Epoch: 1, Key: key, GenTTL: time.Hour})
		require.NoError(t, err)
		require.True(t, r.Hit)
		assert.Equal(t, []byte(key), r.Payload)
	}
	var shards, empty atomic.Int32
	require.NoError(t, rdb.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		n, err := m.DBSize(ctx).Result()
		shards.Add(1)
		if n == 0 {
			empty.Add(1)
		}
		return err
	}))
	assert.Equal(t, int32(3), shards.Load())
	assert.Zero(t, empty.Load(), "every shard holds keys")

	_, err := b.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "", GenTTL: time.Hour})
	require.ErrorIs(t, err, redisl2.ErrUnavailable)
	assert.ErrorContains(t, err, "CROSSSLOT")
}

//nolint:paralleltest // goroutine snapshot must not see other tests' goroutines
func TestBackend_CloseLeavesNoGoroutines(t *testing.T) {
	srv := redistest.StartSingle(t)
	opt := srv.Options()
	before := goleak.IgnoreCurrent()
	rdb := redis.NewClient(opt)
	b, err := redisl2.New(rdb, l2.Config{Namespace: "leak", Channel: "wake:inval:leak", BusHealthCheck: busHealthCheck})
	require.NoError(t, err)
	expectKind(t, b, l2.EventConnected)
	exerciseAll(t, b, "k")
	require.NoError(t, b.Close())
	require.NoError(t, rdb.Close())
	goleak.VerifyNone(t, before)
}
