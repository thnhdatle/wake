// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/testutil/redistest"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

const redisWait = 30 * time.Second

func uniqueNS(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

func redisNode(t *testing.T, ns string, rdb redis.UniversalClient, opts ...Option) *cache[string] {
	t.Helper()
	all := append([]Option{WithL2(rdb), WithDegradedL1TTL(time.Second)}, opts...)
	c, err := New[string](ns, JSONCodec[string](), all...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	n := asImpl(t, c)
	waitFor(t, "bus up and epoch known", func() bool { return n.busUp.Load() && n.epochOK.Load() })
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(redisWait)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		<-tick.C
	}
}

func valueLoader(v string, calls *atomic.Int32) LoadFunc[string] {
	return func(context.Context) (string, error) {
		calls.Add(1)
		return v, nil
	}
}

func inL1(n *cache[string], key string) (string, bool) {
	it, ok := n.l1.Get(key)
	return it.v, ok && !it.neg
}

func noneCached(nodes []*cache[string], keys int, prefix string) bool {
	for _, n := range nodes {
		for k := range keys {
			key := "user:" + strconv.Itoa(k)
			if v, ok := inL1(n, key); ok && v == prefix+key {
				return false
			}
		}
	}
	return true
}

func TestRedis_SingleNode(t *testing.T) {
	t.Parallel()
	srv := redistest.StartSingle(t)
	client := func(t *testing.T) redis.UniversalClient {
		t.Helper()
		return srv.Client(t)
	}
	t.Run("stampede", func(t *testing.T) {
		t.Parallel()
		testStampede(t, client)
	})
	t.Run("invalidation_across_instances", func(t *testing.T) {
		t.Parallel()
		testInvalidationAcrossInstances(t, client, 20)
	})
}

func testStampede(t *testing.T, client func(*testing.T) redis.UniversalClient) {
	t.Helper()
	const callers = 1000
	ns := uniqueNS("stampede")
	nodes := []*cache[string]{redisNode(t, ns, client(t)), redisNode(t, ns, client(t)), redisNode(t, ns, client(t))}
	ctx := t.Context()
	const ttl = 300 * time.Millisecond
	require.NoError(t, nodes[0].Set(ctx, "hot", "v1", WithSetTTL(ttl)))
	for _, n := range nodes {
		v, ok, err := n.Get(ctx, "hot")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "v1", v)
	}
	rdb := client(t)
	vkey, _, _ := l2.KeyNames(ns, nodes[0].epoch.Load(), "hot")
	waitFor(t, "value expired in L2 and L1", func() bool {
		gone := rdb.Exists(ctx, vkey).Val() == 0
		for _, n := range nodes {
			_, cached := inL1(n, "hot")
			gone = gone && !cached
		}
		return gone
	})

	var calls atomic.Int32
	load := LoadFunc[string](func(ctx context.Context) (string, error) {
		calls.Add(1)
		waitFor(t, "other instances waiting on the lock", func() bool {
			return metricValue(nodes[0].m.lockWaits)+metricValue(nodes[1].m.lockWaits)+
				metricValue(nodes[2].m.lockWaits) == float64(len(nodes)-1)
		})
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
		}
		return "v2", nil
	})
	gate := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan string, callers)
	for i := range callers {
		n := nodes[i%len(nodes)]
		wg.Go(func() {
			<-gate
			v, err := n.GetOrLoad(ctx, "hot", load)
			assert.NoError(t, err)
			results <- v
		})
	}
	close(gate)
	wg.Wait()
	close(results)
	for v := range results {
		require.Equal(t, "v2", v)
	}
	assert.Equal(t, int32(1), calls.Load(), "loader calls")
	for _, n := range nodes {
		assert.Zero(t, metricValue(n.m.fallbackLoads), "no fallback load")
	}
}

func testInvalidationAcrossInstances(t *testing.T, client func(*testing.T) redis.UniversalClient, keys int) {
	t.Helper()
	ns := uniqueNS("inval")
	nodes := []*cache[string]{redisNode(t, ns, client(t)), redisNode(t, ns, client(t)), redisNode(t, ns, client(t))}
	ctx := t.Context()
	var calls atomic.Int32
	for k := range keys {
		key := "user:" + strconv.Itoa(k)
		for _, n := range nodes {
			v, err := n.GetOrLoad(ctx, key, valueLoader("old-"+key, &calls))
			require.NoError(t, err)
			require.Equal(t, "old-"+key, v)
		}
	}
	assert.Equal(t, int32(keys), calls.Load(), "one load per key across the 3 instances")

	for k := range keys {
		key := "user:" + strconv.Itoa(k)
		writer := nodes[k%len(nodes)]
		if k%2 == 0 {
			require.NoError(t, writer.Delete(ctx, key))
		} else {
			require.NoError(t, writer.Set(ctx, key, "set-"+key))
		}
	}
	waitFor(t, "invalidations delivered to every L1", func() bool { return noneCached(nodes, keys, "old-") })
	for k := range keys {
		key := "user:" + strconv.Itoa(k)
		want := "new-" + key
		if k%2 == 1 {
			want = "set-" + key
		}
		for _, n := range nodes {
			v, err := n.GetOrLoad(ctx, key, valueLoader("new-"+key, &calls))
			require.NoError(t, err)
			assert.Equal(t, want, v, "key %s", key)
		}
	}
}

func TestRedis_OutageDegradesAndRecovers(t *testing.T) {
	t.Parallel()
	srv := redistest.StartSingle(t)
	ns := uniqueNS("outage")
	opts := []Option{WithTTL(2*time.Second, 0), WithStaleIfError(5 * time.Minute)}
	nodes := []*cache[string]{
		redisNode(t, ns, srv.Client(t), opts...),
		redisNode(t, ns, srv.Client(t), opts...),
		redisNode(t, ns, srv.Client(t), opts...),
	}
	ctx := t.Context()
	const keys = 20
	var outage atomic.Bool
	load := func(key string, i int) LoadFunc[string] {
		return func(context.Context) (string, error) {
			if outage.Load() && i%10 == 0 {
				return "", fmt.Errorf("primary unavailable for %s", key)
			}
			return "v-" + key, nil
		}
	}
	readAll := func() {
		for i := range keys {
			key := "k" + strconv.Itoa(i)
			for _, n := range nodes {
				v, err := n.GetOrLoad(ctx, key, load(key, i))
				require.NoError(t, err, "no error returned to callers")
				require.Equal(t, "v-"+key, v)
			}
		}
	}
	readAll()
	raw := srv.Client(t)
	_, gkey, _ := l2.KeyNames(ns, nodes[0].epoch.Load(), "k1")
	genBefore, err := raw.Get(ctx, gkey).Uint64()
	require.NoError(t, err)

	outage.Store(true)
	srv.Stop(t)
	waitFor(t, "breakers open under read load", func() bool {
		readAll()
		open := true
		for _, n := range nodes {
			open = open && !n.breaker.allow() && !n.busUp.Load()
		}
		return open
	})
	require.ErrorIs(t, nodes[0].Delete(ctx, "k1"), ErrDeferred)
	for range 5 {
		readAll()
	}
	for _, n := range nodes {
		assert.Positive(t, metricValue(n.m.staleServes), "stale values served while the loader failed")
	}

	outage.Store(false)
	srv.Start(t)
	back := time.Now()
	waitFor(t, "breakers closed, buses up, deferred delete replayed", func() bool {
		ok := !nodes[0].pending.active.Load()
		for _, n := range nodes {
			ok = ok && n.breaker.allow() && n.busUp.Load()
		}
		return ok
	})
	assert.Less(t, time.Since(back), 30*time.Second, "L2 used again within 30 s")
	genAfter, err := raw.Get(ctx, gkey).Uint64()
	require.NoError(t, err)
	assert.Greater(t, genAfter, genBefore, "the deferred invalidation reached L2")

	v, err := nodes[0].GetOrLoad(ctx, "k2", load("k2", 2))
	require.NoError(t, err)
	assert.Equal(t, "v-k2", v)
	hits := metricValue(nodes[1].m.l2Hits)
	nodes[1].l1.Flush()
	v, err = nodes[1].GetOrLoad(ctx, "k2", load("k2", 2))
	require.NoError(t, err)
	assert.Equal(t, "v-k2", v)
	assert.Greater(t, metricValue(nodes[1].m.l2Hits), hits, "served from L2 again")
}

func TestRedis_Cluster(t *testing.T) {
	t.Parallel()
	c := redistest.StartCluster(t)
	client := func(t *testing.T) redis.UniversalClient {
		t.Helper()
		return c.Client(t)
	}
	t.Run("stampede", func(t *testing.T) {
		t.Parallel()
		testStampede(t, client)
	})
	t.Run("invalidation_across_instances", func(t *testing.T) {
		t.Parallel()
		testInvalidationAcrossInstances(t, client, 60)
	})
}

func TestRedis_TLS(t *testing.T) {
	t.Parallel()
	srv := redistest.StartTLS(t)
	client := func(t *testing.T) redis.UniversalClient {
		t.Helper()
		return srv.Client(t)
	}
	testInvalidationAcrossInstances(t, client, 5)
}

//nolint:paralleltest // goroutine snapshot must not see other tests' goroutines
func TestNew_CloseStopsGoroutinesWithRedis(t *testing.T) {
	srv := redistest.StartSingle(t)
	opt := srv.Options()
	before := goleak.IgnoreCurrent()
	rdb := redis.NewClient(opt)
	c, err := New[string](uniqueNS("leak"), JSONCodec[string](), WithL2(rdb))
	require.NoError(t, err)
	n := asImpl(t, c)
	waitFor(t, "bus up", func() bool { return n.busUp.Load() && n.epochOK.Load() })
	var calls atomic.Int32
	v, err := c.GetOrLoad(t.Context(), "k", valueLoader("v", &calls))
	require.NoError(t, err)
	assert.Equal(t, "v", v)
	require.NoError(t, c.Delete(t.Context(), "k"))
	require.NoError(t, c.Close())
	require.NoError(t, rdb.Close())
	goleak.VerifyNone(t, before)
}
