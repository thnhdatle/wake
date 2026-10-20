// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
)

// afterOnce runs fn once, right after the first op executed on cli; fn
// may itself run op on cli.
func afterOnce(cli *l2fake.Client, op l2fake.Op, fn func()) {
	var fired atomic.Bool
	cli.SetHooks(l2fake.Hooks{After: func(o l2fake.Op, _ string) {
		if o == op && fired.CompareAndSwap(false, true) {
			fn()
		}
	}})
}

// Race "L1 populate after invalidation" (cache-consistency.md): a reader
// captures its L1 token, reads the old value from L2, an invalidation is
// processed by its instance, and only then the reader inserts into L1. The
// insert must be refused.
func TestCache_L1Guard_PopulateAfterInvalidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		read func(n node, ctx context.Context) (int, error)
	}{
		{"get", func(n node, ctx context.Context) (int, error) {
			v, _, err := n.Get(ctx, "k")
			return v, err
		}},
		{"get or load", func(n node, ctx context.Context) (int, error) {
			return n.GetOrLoad(ctx, "k", func(context.Context) (int, error) { return -1, nil })
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			reader, writer := e.instance(), e.instance()
			ctx := t.Context()
			require.NoError(t, writer.Set(ctx, "k", 1))
			e.idle()
			reader.l1.Flush()

			afterOnce(reader.cli, l2fake.OpRead, func() {
				// The reader has read value 1 from L2 but not inserted it.
				require.NoError(t, writer.Delete(ctx, "k"))
				require.NoError(t, reader.cli.WaitIdle(ctx)) // reader's subscriber processed it
			})
			v, err := tc.read(reader, ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, v, "the read was concurrent with the Delete: old value allowed")
			_, cached := reader.l1Peek("k")
			assert.False(t, cached, "stale value must not be inserted into L1 after the invalidation")
			_, ok, err := reader.Get(ctx, "k")
			require.NoError(t, err)
			assert.False(t, ok)
		})
	}
}

// Race "load vs Delete": the loader read the old row, then the row changes
// and Delete increments the generation before the loader's write. The
// generation-checked write is rejected and nothing is cached.
func TestCache_LoadVsDelete_WriteRejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		sameInstance bool
	}{
		{"delete from another instance", false},
		{"delete from the loading instance", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			loaderNode, other := e.instance(), e.instance()
			deleter := other
			if tc.sameInstance {
				deleter = loaderNode
			}
			ctx := t.Context()
			db := 1
			load := func(context.Context) (int, error) {
				v := db // read the row (old value)
				db = 2  // a writer commits ...
				require.NoError(t, deleter.Delete(ctx, "k"))
				return v, nil
			}
			v, err := loaderNode.GetOrLoad(ctx, "k", load)
			require.NoError(t, err)
			assert.Equal(t, 1, v, "the loaded value is returned")
			e.idle()
			_, inL2 := e.srv.Value(testNS, loaderNode.epoch.Load(), "k")
			assert.False(t, inL2, "the write under the old generation is rejected")
			_, inL1 := loaderNode.l1Peek("k")
			assert.False(t, inL1)
			calls := 0
			v, err = other.GetOrLoad(ctx, "k", constLoader(db, &calls))
			require.NoError(t, err)
			assert.Equal(t, 2, v)
			assert.Equal(t, 1, calls)
		})
	}
}

// Race "generation expiry": a loader captured generation g; the generation
// key expires (no script touched it for longer than its TTL) and is
// re-created from TIME. The loader's write under g must be rejected.
func TestCache_GenerationExpiry_StaleWriteRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(WithTTL(time.Minute, 0), WithLockTimeout(time.Second)), e.instance()
	ctx := t.Context()
	var captured uint64
	load := func(context.Context) (int, error) {
		captured, _ = e.srv.Gen(testNS, a.epoch.Load(), "k")
		e.clk.Advance(time.Minute + time.Second + genGrace) // generation TTL elapses
		_, ok := e.srv.Gen(testNS, a.epoch.Load(), "k")
		assert.False(t, ok, "generation key expired")
		_, _, err := b.Get(ctx, "k") // re-creates it from TIME
		assert.NoError(t, err)
		return 1, nil
	}
	_, err := a.GetOrLoad(ctx, "k", load)
	require.NoError(t, err)
	fresh, ok := e.srv.Gen(testNS, a.epoch.Load(), "k")
	require.True(t, ok)
	assert.Greater(t, fresh, captured, "a re-created generation never repeats an earlier one")
	_, stored := e.srv.Value(testNS, a.epoch.Load(), "k")
	assert.False(t, stored)
}

// Race "epoch change": incrementing the epoch makes every older L2 entry
// invisible and flushes every instance's L1.
func TestCache_EpochChange_InvalidatesNamespace(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	for _, k := range []string{"x", "y"} {
		require.NoError(t, a.Set(ctx, k, 1))
	}
	_, _, err := b.Get(ctx, "x")
	require.NoError(t, err)
	old := a.epoch.Load()

	admin := e.admin()
	next, err := admin.BumpEpoch(ctx)
	require.NoError(t, err)
	e.idle()

	for _, n := range []node{a, b} {
		assert.Equal(t, next, n.epoch.Load())
		assert.Zero(t, n.l1.Len(), "L1 flushed")
		_, ok, err := n.Get(ctx, "x")
		require.NoError(t, err)
		assert.False(t, ok, "old-epoch entries are invisible")
		assert.InDelta(t, 1, metricValue(n.m.epochChanges), 0)
	}
	_, stillThere := e.srv.Value(testNS, old, "y")
	assert.True(t, stillThere, "old keys are left to expire")
}

// An instance that has not yet seen an epoch change (bus lagging) still
// invalidates the live keyspace: the server reports the epoch with every
// write and the write is repeated there.
func TestCache_Delete_StaleEpochRepeatsAtCurrentEpoch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	a.cli.PauseBus()
	admin := e.admin()
	next, err := admin.BumpEpoch(ctx)
	require.NoError(t, err)
	require.NoError(t, b.cli.WaitIdle(ctx))
	require.Equal(t, next, b.epoch.Load())
	require.NotEqual(t, next, a.epoch.Load())

	calls := 0
	_, err = b.GetOrLoad(ctx, "k", constLoader(1, &calls))
	require.NoError(t, err)
	require.NoError(t, a.Delete(ctx, "k"))
	assert.Equal(t, next, a.epoch.Load(), "a adopted the epoch reported by the server")
	_, ok := e.srv.Value(testNS, next, "k")
	assert.False(t, ok, "the live keyspace was invalidated")
	a.cli.ResumeBus()
	e.idle()
	_, ok = b.l1Peek("k")
	assert.False(t, ok)
}

// Concurrent Sets of one key on one instance: whatever order the local L1
// updates run in, L1 ends with the value of the highest generation, the
// one L2 holds.
func TestCache_Set_ConcurrentLocalSetsKeepNewest(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.instance()
	ctx := t.Context()
	afterOnce(a.cli, l2fake.OpSet, func() {
		// First Set wrote L2 (generation g); a second Set completes
		// entirely (g+1, L1 updated) before the first updates L1.
		require.NoError(t, a.Set(ctx, "k", 2))
	})
	require.NoError(t, a.Set(ctx, "k", 1))
	e.idle()
	v, ok := a.l1Peek("k")
	require.True(t, ok)
	assert.Equal(t, 2, v)
	got, _, err := a.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, 2, got)
	sv, _ := e.srv.Value(testNS, a.epoch.Load(), "k")
	assert.Equal(t, []byte{0, '2'}, sv.Payload)
}

// Set on one instance replaces the value on every instance.
func TestCache_Set_InvalidatesOtherInstances(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a, b := e.instance(), e.instance()
	ctx := t.Context()
	require.NoError(t, a.Set(ctx, "k", 1))
	_, _, err := b.Get(ctx, "k")
	require.NoError(t, err)
	require.NoError(t, a.Set(ctx, "k", 2))
	e.idle()
	got, ok, err := b.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 2, got)
	v, ok := a.l1Peek("k")
	require.True(t, ok, "own invalidation message keeps the newer local entry")
	assert.Equal(t, 2, v)
}
