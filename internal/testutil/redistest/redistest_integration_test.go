// SPDX-License-Identifier: Apache-2.0

//go:build integration

package redistest_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/testutil/redistest"
)

func TestSingle_StopStartKeepsDataAndFreezeBlocks(t *testing.T) {
	t.Parallel()
	srv := redistest.StartSingle(t)
	rdb := srv.Client(t)
	ctx := t.Context()
	require.NoError(t, rdb.Set(ctx, "k", "v", 0).Err())

	srv.Stop(t)
	require.Error(t, rdb.Ping(ctx).Err(), "unreachable while stopped")
	srv.Start(t)
	got, err := rdb.Get(ctx, "k").Result()
	require.NoError(t, err, "the client follows the new port")
	assert.Equal(t, "v", got, "dataset saved on stop and reloaded")

	done := srv.Freeze(t, 2*time.Second)
	err = rdb.Get(ctx, "k").Err()
	require.Error(t, err, "no answer while frozen")
	require.NoError(t, <-done)
	require.NoError(t, rdb.Ping(ctx).Err())
}

func TestCluster_ThreeMastersServeEverySlot(t *testing.T) {
	t.Parallel()
	c := redistest.StartCluster(t)
	rdb := c.Client(t)
	ctx := t.Context()
	var masters atomic.Int32
	require.NoError(t, rdb.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		masters.Add(1)
		return m.Ping(ctx).Err()
	}))
	assert.Equal(t, int32(3), masters.Load())
	for i := range 100 {
		k := fmt.Sprintf("key-%d", i)
		require.NoError(t, rdb.Set(ctx, k, i, time.Minute).Err())
		v, err := rdb.Get(ctx, k).Int()
		require.NoError(t, err)
		assert.Equal(t, i, v)
	}
	shards, err := rdb.ClusterShards(ctx).Result()
	require.NoError(t, err)
	assert.Len(t, shards, 3)
}

func TestTLS_RequiresTrustedServerAndClientCertificate(t *testing.T) {
	t.Parallel()
	srv := redistest.StartTLS(t)
	rdb := srv.Client(t)
	ctx := t.Context()
	require.NoError(t, rdb.Set(ctx, "k", "v", time.Minute).Err())

	tests := []struct {
		name string
		opt  func(*redis.Options)
	}{
		{"plaintext", func(o *redis.Options) { o.TLSConfig = nil }},
		{"no client certificate", func(o *redis.Options) { o.TLSConfig.Certificates = nil }},
		{"untrusted server", func(o *redis.Options) {
			o.TLSConfig = &tls.Config{Certificates: o.TLSConfig.Certificates, MinVersion: tls.VersionTLS12}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opt := srv.Options(t)
			tc.opt(opt)
			bad := redis.NewClient(opt)
			defer func() { _ = bad.Close() }()
			assert.Error(t, bad.Ping(t.Context()).Err())
		})
	}
}
