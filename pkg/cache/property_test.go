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
		pc.inject(context.Background())
		reader.cli.SetHooks(l2fake.Hooks{})
		reader.cli.ResumeBus()
		if v := <-pc.inner; v < pc.committed {
			rt.Fatalf("read started after the write became visible returned %d < %d", v, pc.committed)
		}
		e.idle()
		checkQuiescent(rt, pc.nodes, pc.current(), pc.load)
	})
}

type protoCase struct {
	rt        *rapid.T
	nodes     []node
	r, w      int
	op        l2fake.Op
	after     bool
	prompt    bool
	inner     chan int
	fired     atomic.Bool
	dbMu      sync.Mutex
	row       int
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
		pc.inner <- pc.committed
		return
	}
	if pc.prompt {
		for _, n := range pc.nodes {
			require.NoError(pc.rt, n.cli.WaitIdle(ctx))
		}
	}
	reader := pc.nodes[pc.r]
	go func() {
		v, _ := reader.GetOrLoad(ctx, "k", pc.load)
		pc.inner <- v
	}()
}

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
