// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// value64 is the 64-byte value of the L1 hit acceptance benchmark.
type value64 struct {
	A, B, C, D, E, F, G, H int64
}

func newBenchCache(tb testing.TB) Cache[value64] {
	tb.Helper()
	c, err := New[value64]("bench", MsgpackCodec[value64](), WithMetrics(prometheus.NewRegistry()))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, c.Close()) })
	require.NoError(tb, c.Set(context.Background(), "user:42", value64{A: 42}))
	return c
}

// BenchmarkCache_GetL1Hit is the L1 hit acceptance benchmark at unit
// level: Get on an L1 hit,
// single goroutine, metrics enabled with pre-bound counters, V = 64-byte
// struct, string key. Target: < 200 ns/op, 0 allocs/op.
func BenchmarkCache_GetL1Hit(b *testing.B) {
	c := newBenchCache(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, ok, _ := c.Get(ctx, "user:42"); !ok {
			b.Fatal("miss")
		}
	}
}

//nolint:paralleltest // testing.AllocsPerRun panics in parallel tests
func TestCache_Get_L1HitZeroAllocs(t *testing.T) {
	c := newBenchCache(t)
	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		if _, ok, _ := c.Get(ctx, "user:42"); !ok {
			t.Fatal("miss")
		}
	})
	require.Zero(t, allocs)
}
