// SPDX-License-Identifier: Apache-2.0

package l1_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/pkg/cache/l1"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var start = time.Unix(1_760_000_000, 0)

func newCache(tb testing.TB, entries int, bytes int64, opts ...l1.Option) (*l1.Cache[string], *clock.Fake) {
	tb.Helper()
	clk := clock.NewFake(start)
	c, err := l1.New[string](entries, bytes, append([]l1.Option{l1.WithClock(clk)}, opts...)...)
	require.NoError(tb, err)
	return c, clk
}

func entry(v string, ttl time.Duration, version uint64) l1.Entry[string] {
	return l1.Entry[string]{Value: v, Size: len(v), TTL: ttl, Version: version}
}

func TestNew_InvalidConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		entries int
		bytes   int64
		opts    []l1.Option
	}{
		{"zero entries", 0, 1, nil},
		{"zero bytes", 1, 0, nil},
		{"zero shards", 1, 1, []l1.Option{l1.WithShards(0)}},
		{"too many shards", 1, 1, []l1.Option{l1.WithShards(1 << 17)}},
		{"nil clock", 1, 1, []l1.Option{l1.WithClock(nil)}},
		{"negative jitter", 1, 1, []l1.Option{l1.WithJitter(-1, 0)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := l1.New[int](tc.entries, tc.bytes, tc.opts...)
			require.ErrorIs(t, err, l1.ErrInvalidConfig)
		})
	}
}

func TestCache_Get_HitMissExpiry(t *testing.T) {
	t.Parallel()
	c, clk := newCache(t, 100, 1<<20)
	_, ok := c.Get("a")
	assert.False(t, ok)
	require.True(t, c.Set("a", entry("va", time.Second, 0)))
	require.True(t, c.Set("forever", entry("vf", 0, 0)))
	v, ok := c.Get("a")
	require.True(t, ok)
	assert.Equal(t, "va", v)
	clk.Advance(time.Second)
	_, ok = c.Get("a")
	assert.False(t, ok, "expired at ttl")
	_, ok = c.Get("forever")
	assert.True(t, ok)
	assert.Equal(t, 2, c.Len(), "expired entries stay until evicted (stale serving)")
}

func TestCache_GetMaxAge_CapsAge(t *testing.T) {
	t.Parallel()
	c, clk := newCache(t, 100, 1<<20)
	c.Set("a", entry("va", time.Hour, 0))
	clk.Advance(5 * time.Second)
	_, ok := c.GetMaxAge("a", 5*time.Second)
	assert.False(t, ok)
	_, ok = c.GetMaxAge("a", 6*time.Second)
	assert.True(t, ok)
	_, ok = c.GetMaxAge("a", 0)
	assert.True(t, ok, "non-positive max age does not cap")
}

func TestCache_GetStale_WithinMaxStale(t *testing.T) {
	t.Parallel()
	c, clk := newCache(t, 100, 1<<20)
	c.Set("a", entry("va", time.Second, 0))
	v, ok := c.GetStale("a", time.Minute)
	require.True(t, ok, "fresh values qualify")
	assert.Equal(t, "va", v)
	clk.Advance(time.Second + 59*time.Second)
	_, ok = c.GetStale("a", time.Minute)
	assert.True(t, ok)
	clk.Advance(time.Second)
	_, ok = c.GetStale("a", time.Minute)
	assert.False(t, ok)
	c.Set("b", entry("vb", time.Second, 0))
	c.Delete("b")
	_, ok = c.GetStale("b", time.Hour)
	assert.False(t, ok, "invalidated values are never stale-served")
}

func TestCache_SetIfUnchanged_Guard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		invalidate func(c *l1.Cache[string])
		want       bool
	}{
		{"no invalidation", func(*l1.Cache[string]) {}, true},
		{"delete", func(c *l1.Cache[string]) { c.Delete("k") }, false},
		{"invalidate", func(c *l1.Cache[string]) { c.Invalidate("k", 1) }, false},
		{"invalidate kept entry still counts", func(c *l1.Cache[string]) { c.Invalidate("k", 0) }, false},
		{"set", func(c *l1.Cache[string]) { c.Set("k", entry("other", 0, 0)) }, false},
		{"flush", func(c *l1.Cache[string]) { c.Flush() }, false},
		{"other shard", func(*l1.Cache[string]) {}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := newCache(t, 100, 1<<20, l1.WithShards(1))
			tok := c.Token("k")
			tc.invalidate(c)
			assert.Equal(t, tc.want, c.SetIfUnchanged(tok, "k", entry("v", 0, 0)))
		})
	}
}

func TestCache_SetIfUnchanged_TokenOfOtherShardRejected(t *testing.T) {
	t.Parallel()
	c, _ := newCache(t, 1000, 1<<20, l1.WithShards(64))
	tok := c.Token("a")
	for i := range 1000 {
		k := fmt.Sprintf("k%d", i)
		if c.Token(k) != c.Token("a") && !c.SetIfUnchanged(tok, k, entry("v", 0, 0)) {
			return
		}
	}
	t.Fatal("no key in another shard found")
}

func TestCache_Versions_NeverReplacedByLower(t *testing.T) {
	t.Parallel()
	c, _ := newCache(t, 100, 1<<20)
	require.True(t, c.Set("k", entry("v5", 0, 5)))
	assert.False(t, c.Set("k", entry("v4", 0, 4)))
	v, _ := c.Get("k")
	assert.Equal(t, "v5", v)
	assert.True(t, c.SetIfUnchanged(c.Token("k"), "k", entry("v5b", 0, 5)), "equal version replaces")
	c.Invalidate("k", 5)
	v, ok := c.Get("k")
	require.True(t, ok, "entry at the invalidated version is newer than the invalidation")
	assert.Equal(t, "v5b", v)
	c.Invalidate("k", 6)
	_, ok = c.Get("k")
	assert.False(t, ok)
}

func TestCache_Bounds_EvictLRU(t *testing.T) {
	t.Parallel()
	c, _ := newCache(t, 3, 1<<20, l1.WithShards(1))
	for _, k := range []string{"a", "b", "c"} {
		c.Set(k, entry(k, 0, 0))
	}
	_, _ = c.Get("a")
	c.Set("d", entry("d", 0, 0))
	_, ok := c.Get("b")
	assert.False(t, ok, "least recently used evicted")
	for _, k := range []string{"a", "c", "d"} {
		_, ok := c.Get(k)
		assert.True(t, ok, k)
	}

	b, _ := newCache(t, 100, 10, l1.WithShards(1))
	b.Set("x", entry("12345", 0, 0))
	b.Set("y", entry("123456", 0, 0))
	assert.Equal(t, int64(6), b.Bytes())
	assert.Equal(t, 1, b.Len())
	assert.False(t, b.Set("big", entry("12345678901", 0, 0)), "larger than the shard bound")
	b.Set("neg", l1.Entry[string]{Value: "n", Size: -3})
	assert.Equal(t, int64(6), b.Bytes(), "negative sizes count as zero")
}

func TestCache_Flush_RemovesAll(t *testing.T) {
	t.Parallel()
	c, _ := newCache(t, 100, 1<<20)
	for i := range 50 {
		c.Set(strconv.Itoa(i), entry("v", 0, 0))
	}
	c.Flush()
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, int64(0), c.Bytes())
	c.Delete("absent")
}

func TestCache_WithJitter_ShortensWithinBound(t *testing.T) {
	t.Parallel()
	c, clk := newCache(t, 1000, 1<<20, l1.WithJitter(10*time.Second, 42))
	for i := range 200 {
		c.Set(strconv.Itoa(i), entry("v", 100*time.Second, 0))
	}
	clk.Advance(90 * time.Second)
	for i := range 200 {
		_, ok := c.Get(strconv.Itoa(i))
		require.True(t, ok, "jitter never exceeds its bound")
	}
	clk.Advance(5 * time.Second)
	expired := 0
	for i := range 200 {
		if _, ok := c.Get(strconv.Itoa(i)); !ok {
			expired++
		}
	}
	assert.Greater(t, expired, 50, "about half expired between 90 and 95 s")
	assert.Less(t, expired, 150)
	clk.Advance(5 * time.Second)
	for i := range 200 {
		_, ok := c.Get(strconv.Itoa(i))
		require.False(t, ok)
	}
}

func TestCache_DefaultShards_ScaleWithBound(t *testing.T) {
	t.Parallel()
	c, _ := newCache(t, 8, 1<<20)
	for i := range 8 {
		c.Set(strconv.Itoa(i), entry("v", 0, 0))
	}
	assert.Equal(t, 8, c.Len(), "one shard for a tiny cache holds every entry")
}

//nolint:paralleltest // testing.AllocsPerRun panics in parallel tests
func TestCache_Get_ZeroAllocs(t *testing.T) {
	type big struct{ A, B, C, D, E, F, G, H int64 }
	clk := clock.NewFake(start)
	c, err := l1.New[big](100, 1<<20, l1.WithClock(clk))
	require.NoError(t, err)
	c.Set("key", l1.Entry[big]{Value: big{A: 1}, Size: 64})
	allocs := testing.AllocsPerRun(1000, func() {
		if _, ok := c.Get("key"); !ok {
			t.Fatal("miss")
		}
	})
	assert.Zero(t, allocs)
}

func TestCache_Concurrent_Race(t *testing.T) {
	t.Parallel()
	c, _ := newCache(t, 64, 1<<10)
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 500 {
				k := strconv.Itoa((w * i) % 100)
				tok := c.Token(k)
				c.Get(k)
				c.SetIfUnchanged(tok, k, entry("v", time.Minute, uint64(i)))
				if i%7 == 0 {
					c.Invalidate(k, uint64(i))
				}
				if i%101 == 0 {
					c.Flush()
				}
			}
		})
	}
	wg.Wait()
	assert.LessOrEqual(t, c.Len(), 64+64)
}

type guardModel struct {
	c                *l1.Cache[string]
	model            map[string]string
	tokens           map[string]l1.Token
	invalidatedSince map[string]bool
}

func (m *guardModel) guardedInsert(rt *rapid.T, k string) {
	tok, ok := m.tokens[k]
	if !ok {
		return
	}
	v := rapid.StringN(0, 8, -1).Draw(rt, "v")
	stored := m.c.SetIfUnchanged(tok, k, entry(v, 0, 0))
	if stored && m.invalidatedSince[k] {
		rt.Fatalf("guarded insert of %q accepted after an invalidation", k)
	}
	if stored {
		m.model[k] = v
	}
}

func (m *guardModel) set(rt *rapid.T, k string) {
	v := rapid.StringN(0, 8, -1).Draw(rt, "v")
	if m.c.Set(k, entry(v, 0, 0)) {
		m.model[k] = v
	} else {
		delete(m.model, k)
	}
	m.invalidatedSince[k] = true
}

func (m *guardModel) flush() {
	m.c.Flush()
	clear(m.model)
	for k := range m.invalidatedSince {
		m.invalidatedSince[k] = true
	}
}

func (m *guardModel) check(rt *rapid.T, k string) {
	got, ok := m.c.Get(k)
	want, inModel := m.model[k]
	if ok && (!inModel || got != want) {
		rt.Fatalf("Get(%q) = %q, model %q (%v)", k, got, want, inModel)
	}
}

func TestCache_GuardAndBounds_Property(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		maxEntries := rapid.IntRange(1, 20).Draw(rt, "maxEntries")
		maxBytes := rapid.Int64Range(1, 100).Draw(rt, "maxBytes")
		shards := rapid.SampledFrom([]int{1, 2, 4}).Draw(rt, "shards")
		c, err := l1.New[string](maxEntries, maxBytes, l1.WithShards(shards), l1.WithClock(clock.NewFake(start)))
		if err != nil {
			rt.Fatal(err)
		}
		m := &guardModel{c: c, model: map[string]string{}, tokens: map[string]l1.Token{}, invalidatedSince: map[string]bool{}}
		for range rapid.IntRange(1, 60).Draw(rt, "steps") {
			k := rapid.SampledFrom([]string{"a", "b", "c", "d", "e"}).Draw(rt, "key")
			switch rapid.IntRange(0, 5).Draw(rt, "op") {
			case 0:
				m.tokens[k] = c.Token(k)
				m.invalidatedSince[k] = false
			case 1:
				m.guardedInsert(rt, k)
			case 2:
				c.Delete(k)
				delete(m.model, k)
				m.invalidatedSince[k] = true
			case 3:
				m.set(rt, k)
			case 4:
				m.flush()
			default:
				m.check(rt, k)
			}
			if c.Len() > maxEntries+shards || c.Bytes() > maxBytes+int64(shards) {
				rt.Fatalf("bounds exceeded: %d entries, %d bytes", c.Len(), c.Bytes())
			}
		}
	})
}

func BenchmarkCache_Get(b *testing.B) {
	type v64 struct{ A, B, C, D, E, F, G, H int64 }
	c, err := l1.New[v64](10_000, 64<<20)
	require.NoError(b, err)
	c.Set("user:42", l1.Entry[v64]{Value: v64{A: 42}, Size: 64, TTL: time.Hour})
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := c.Get("user:42"); !ok {
			b.Fatal("miss")
		}
	}
}

func TestToken_Seq_ChangesWithInvalidation(t *testing.T) {
	t.Parallel()
	c, _ := newCache(t, 10, 100)
	before := c.Token("k")
	assert.Equal(t, before.Seq(), c.Token("k").Seq())
	c.Delete("k")
	assert.NotEqual(t, before.Seq(), c.Token("k").Seq())
}
