// SPDX-License-Identifier: Apache-2.0

//go:build integration

package redisl2_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2/l2test"
	"github.com/thnhdatle/wake/pkg/cache/redisl2"
)

const busHealthCheck = 200 * time.Millisecond

type redisHarness struct {
	admin     redis.UniversalClient
	newClient func(name string) redis.UniversalClient

	mu    sync.Mutex
	names map[l2.Backend]string
}

var _ l2test.Harness = (*redisHarness)(nil)

func newRedisHarness(admin redis.UniversalClient, newClient func(name string) redis.UniversalClient) *redisHarness {
	return &redisHarness{admin: admin, newClient: newClient, names: make(map[l2.Backend]string)}
}

func randomName() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "wake-test-" + hex.EncodeToString(b[:])
}

func (h *redisHarness) Backend(t *testing.T, cfg l2.Config) l2.Backend {
	t.Helper()
	name := randomName()
	rdb := h.newClient(name)
	cfg.BusHealthCheck = busHealthCheck
	b, err := redisl2.New(rdb, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, b.Close())
		_ = rdb.Close()
	})
	h.mu.Lock()
	h.names[b] = name
	h.mu.Unlock()
	return b
}

func (h *redisHarness) Now(t *testing.T) time.Time {
	t.Helper()
	now, err := h.admin.Time(t.Context()).Result()
	require.NoError(t, err)
	return now
}

func (h *redisHarness) Elapse(t *testing.T, d time.Duration) {
	t.Helper()
	until := h.Now(t).Add(d + time.Millisecond)
	eventually(t, 10*time.Second, func() bool { return !h.Now(t).Before(until) })
}

func (*redisHarness) Tolerance() time.Duration { return 2 * time.Second }

func (h *redisHarness) get(t *testing.T, key string) (string, bool) {
	t.Helper()
	v, err := h.admin.Get(t.Context(), key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false
	}
	require.NoError(t, err)
	return v, true
}

func (h *redisHarness) Gen(t *testing.T, ns string, epoch uint64, key string) (uint64, bool) {
	t.Helper()
	_, g, _ := l2.KeyNames(ns, epoch, key)
	raw, ok := h.get(t, g)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	return n, err == nil
}

func (h *redisHarness) GenTTL(t *testing.T, ns string, epoch uint64, key string) (time.Duration, bool) {
	t.Helper()
	_, g, _ := l2.KeyNames(ns, epoch, key)
	d, err := h.admin.PTTL(t.Context(), g).Result()
	require.NoError(t, err)
	return d, d >= 0
}

func (h *redisHarness) Value(t *testing.T, ns string, epoch uint64, key string) (l2.StoredValue, bool) {
	t.Helper()
	v, _, _ := l2.KeyNames(ns, epoch, key)
	raw, ok := h.get(t, v)
	if !ok {
		return l2.StoredValue{}, false
	}
	sv, err := l2.DecodeValue([]byte(raw))
	return sv, err == nil
}

func (h *redisHarness) Locked(t *testing.T, ns string, epoch uint64, key string) bool {
	t.Helper()
	_, _, l := l2.KeyNames(ns, epoch, key)
	n, err := h.admin.Exists(t.Context(), l).Result()
	require.NoError(t, err)
	return n == 1
}

func (h *redisHarness) EpochOf(t *testing.T, ns string) (uint64, bool) {
	t.Helper()
	raw, ok := h.get(t, l2.EpochKey(ns))
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	return n, err == nil
}

func (h *redisHarness) RawSet(t *testing.T, key, val string) {
	t.Helper()
	require.NoError(t, h.admin.Set(t.Context(), key, val, 0).Err())
}

func (h *redisHarness) Delete(t *testing.T, key string) {
	t.Helper()
	require.NoError(t, h.admin.Del(t.Context(), key).Err())
}

func (h *redisHarness) DropBus(t *testing.T, b l2.Backend) {
	t.Helper()
	h.mu.Lock()
	name := h.names[b]
	h.mu.Unlock()
	require.NotEmpty(t, name, "backend not created by this harness")
	var (
		mu     sync.Mutex
		killed int
	)
	err := forEachNode(t.Context(), h.admin, func(ctx context.Context, node *redis.Client) error {
		n, err := killPubSub(ctx, node, name)
		mu.Lock()
		killed += n
		mu.Unlock()
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 1, killed, "subscription connections named %s", name)
}

func killPubSub(ctx context.Context, node *redis.Client, name string) (int, error) {
	list, err := node.ClientList(ctx).Result()
	if err != nil {
		return 0, err
	}
	killed := 0
	for line := range strings.SplitSeq(list, "\n") {
		fields := strings.Fields(line)
		if !hasField(fields, "name="+name) || !hasField(fields, "flags=P") {
			continue
		}
		for _, f := range fields {
			if id, ok := strings.CutPrefix(f, "id="); ok {
				if err := node.Do(ctx, "CLIENT", "KILL", "ID", id).Err(); err != nil {
					return killed, err
				}
				killed++
			}
		}
	}
	return killed, nil
}

func hasField(fields []string, want string) bool {
	for _, f := range fields {
		if f == want {
			return true
		}
	}
	return false
}

func forEachNode(ctx context.Context, rdb redis.UniversalClient, fn func(context.Context, *redis.Client) error) error {
	switch c := rdb.(type) {
	case *redis.ClusterClient:
		return c.ForEachMaster(ctx, fn)
	case *redis.Client:
		return fn(ctx, c)
	}
	return errors.New("unsupported client type")
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		<-tick.C
	}
}
