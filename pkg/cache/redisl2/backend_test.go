// SPDX-License-Identifier: Apache-2.0

package redisl2_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/redisl2"
)

var errOffline = errors.New("offline")

// offlineClient is a client whose every dial fails at once (no network);
// dials counts the attempts.
func offlineClient(t *testing.T, dials *atomic.Int32) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr:          "offline:6379",
		MaxRetries:    -1,
		DialerRetries: 1,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			dials.Add(1)
			return nil, errOffline
		},
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestNew_ValidatesConfig(t *testing.T) {
	t.Parallel()
	var dials atomic.Int32
	rdb := offlineClient(t, &dials)
	tests := []struct {
		name   string
		client redis.UniversalClient
		cfg    l2.Config
	}{
		{"nil client", nil, l2.Config{Namespace: "ns", Channel: "c"}},
		{"no namespace", rdb, l2.Config{Channel: "c"}},
		{"no channel", rdb, l2.Config{Namespace: "ns"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := redisl2.New(tc.client, tc.cfg)
			require.Error(t, err)
			assert.Nil(t, b)
		})
	}
}

// TestBackend_Offline covers the unreachable server: New succeeds, every
// command fails with ErrUnavailable, the bus keeps retrying without ever
// reporting a connection, and Close stops it (goleak in TestMain).
func TestBackend_Offline(t *testing.T) {
	t.Parallel()
	var dials atomic.Int32
	b := offlineBackend(t, &dials)
	ctx := t.Context()
	calls := map[string]func() error{
		"read": func() error {
			_, err := b.Read(ctx, l2.ReadRequest{Epoch: 1, Key: "k"})
			return err
		},
		"fill": func() error {
			_, err := b.Fill(ctx, l2.FillRequest{Epoch: 1, Key: "k", Gen: 1})
			return err
		},
		"set": func() error {
			_, err := b.Set(ctx, l2.SetRequest{Epoch: 1, Key: "k"})
			return err
		},
		"invalidate": func() error {
			_, err := b.Invalidate(ctx, l2.InvalidateRequest{Epoch: 1, Key: "k"})
			return err
		},
		"lock": func() error {
			_, err := b.Lock(ctx, 1, "k", "t", time.Second)
			return err
		},
		"unlock": func() error { return b.Unlock(ctx, 1, "k", "t") },
		"epoch": func() error {
			_, err := b.Epoch(ctx)
			return err
		},
		"bump epoch": func() error {
			_, err := b.BumpEpoch(ctx)
			return err
		},
	}
	for name, call := range calls {
		err := call()
		require.ErrorIs(t, err, redisl2.ErrUnavailable, name)
		require.ErrorIs(t, err, errOffline, name)
	}

	require.Positive(t, dials.Load())
	for range 10 { // the bus keeps retrying in the background (backoff >= 25 ms)
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		_, err := b.Receive(waitCtx)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded, "no event without a connection")
	}
	require.NoError(t, b.Close())
	_, err := b.Receive(ctx)
	require.ErrorIs(t, err, l2.ErrClosed)
	require.NoError(t, b.Close(), "idempotent")
}

func offlineBackend(t *testing.T, dials *atomic.Int32) *redisl2.Backend {
	t.Helper()
	b, err := redisl2.New(offlineClient(t, dials), l2.Config{
		Namespace: "ns", Channel: "wake:inval:ns", BusHealthCheck: time.Millisecond,
	})
	require.NoError(t, err)
	return b
}
