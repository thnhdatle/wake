// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
)

// TestCache_Protocol_InvalidationProperty checks the consistency invariants
// of the generation and epoch guard over every interleaving of one read
// with one write: a row update followed by Delete (from any instance) is
// injected before or after any L2 operation of the reading instance, with the reader's bus either
// prompt or delayed. Invariants:
//   - the read returns a value at least as new as the row when it started;
//   - a read started on the reader's instance after the write became
//     visible there returns the new value;
//   - once the bus is drained, no instance (L1 or L2) holds a stale value.
func TestCache_Protocol_InvalidationProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		e := newEnv(t)
		pc := &protoCase{rt: rt, nodes: []node{e.instance(), e.instance(), e.instance()}, inner: make(chan int, 1)}
		defer func() {
			for _, n := range pc.nodes {
				_ = n.Close()
			}
		}()
		pc.r = rapid.IntRange(0, 2).Draw(rt, "reader")
		pc.w = rapid.IntRange(0, 2).Draw(rt, "writer")
		warm(rt, e, pc.nodes, pc.r, pc.load)
		pc.op = rapid.SampledFrom([]l2fake.Op{l2fake.OpRead, l2fake.OpLock, l2fake.OpFill, l2fake.OpUnlock}).Draw(rt, "op")
		pc.after = rapid.Bool().Draw(rt, "after")
		pc.prompt = rapid.Bool().Draw(rt, "prompt")
		reader := pc.nodes[pc.r]
		if !pc.prompt {
			reader.cli.PauseBus()
		}
		reader.cli.SetHooks(pc.hooks())
		startRow := pc.current()
		got, err := reader.GetOrLoad(context.Background(), "k", pc.load)
		require.NoError(rt, err)
		if got < startRow {
			rt.Fatalf("read returned %d, row was %d when it started", got, startRow)
		}
		pc.inject(context.Background()) // the hook point was not reached: write after the read
		reader.cli.SetHooks(l2fake.Hooks{})
		reader.cli.ResumeBus()
		if v := <-pc.inner; v < pc.committed {
			rt.Fatalf("read started after the write became visible returned %d < %d", v, pc.committed)
		}
		e.idle()
		checkQuiescent(rt, pc.nodes, pc.current(), pc.load)
	})
}

// protoCase is one drawn interleaving of TestCache_Protocol_InvalidationProperty.
type protoCase struct {
	rt     *rapid.T
	nodes  []node
	r, w   int       // reader and writer instances
	op     l2fake.Op // reader operation around which the write is injected
	after  bool      // inject after (true) or before op
	prompt bool      // the reader's bus delivers at once
	inner  chan int  // result of a read started after the write was visible
	fired  atomic.Bool
	dbMu   sync.Mutex // protects row
	row    int
	// committed is the row version written by the injection.
	committed int
}

func (pc *protoCase) load(context.Context) (int, error) { return pc.current(), nil }

func (pc *protoCase) current() int {
	pc.dbMu.Lock()
	defer pc.dbMu.Unlock()
	return pc.row
}

func (pc *protoCase) hooks() l2fake.Hooks {
	return l2fake.Hooks{
		Before: func(ctx context.Context, o l2fake.Op, _ string) error {
			if o == pc.op && !pc.after {
				pc.inject(ctx)
			}
			return nil
		},
		After: func(o l2fake.Op, _ string) {
			if o == pc.op && pc.after {
				pc.inject(context.Background())
			}
		},
	}
}

// inject commits a row update and invalidates it through the writer; when
// the write is visible on the reader it starts a second read there.
func (pc *protoCase) inject(ctx context.Context) {
	if !pc.fired.CompareAndSwap(false, true) {
		return
	}
	pc.dbMu.Lock()
	pc.row++
	pc.committed = pc.row
	pc.dbMu.Unlock()
	require.NoError(pc.rt, pc.nodes[pc.w].Delete(ctx, "k"))
	if !pc.prompt && pc.w != pc.r {
		pc.inner <- pc.committed // not yet visible on the reader: nothing to check
		return
	}
	if pc.prompt {
		for _, n := range pc.nodes {
			require.NoError(pc.rt, n.cli.WaitIdle(ctx))
		}
	}
	reader := pc.nodes[pc.r]
	go func() { // visible on the reader: a new read must see it
		v, _ := reader.GetOrLoad(ctx, "k", pc.load)
		pc.inner <- v
	}()
}

// warm puts the key into L2 and/or the reader's L1 and optionally expires it.
func warm(rt *rapid.T, e *env, nodes []node, r int, load LoadFunc[int]) {
	ctx := context.Background()
	if rapid.Bool().Draw(rt, "warmL2") {
		_, err := nodes[(r+1)%len(nodes)].GetOrLoad(ctx, "k", load)
		require.NoError(rt, err)
		e.idle()
	}
	if rapid.Bool().Draw(rt, "warmReaderL1") {
		_, err := nodes[r].GetOrLoad(ctx, "k", load)
		require.NoError(rt, err)
	}
	if rapid.Bool().Draw(rt, "expire") {
		e.clk.Advance(defaultTTL + time.Second)
	}
}

func checkQuiescent(rt *rapid.T, nodes []node, row int, load LoadFunc[int]) {
	ctx := context.Background()
	for i, n := range nodes {
		if v, ok := n.l1Peek("k"); ok && v != row {
			rt.Fatalf("instance %d L1 holds %d, row is %d", i, v, row)
		}
		if v, ok, err := n.Get(ctx, "k"); err != nil || (ok && v != row) {
			rt.Fatalf("instance %d Get = %d, %v, %v; row is %d", i, v, ok, err, row)
		}
		if v, err := n.GetOrLoad(ctx, "k", load); err != nil || v != row {
			rt.Fatalf("instance %d GetOrLoad = %d, %v; row is %d", i, v, err, row)
		}
	}
}
