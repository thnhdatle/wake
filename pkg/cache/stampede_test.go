// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_GetOrLoad_Stampede(t *testing.T) {
	t.Parallel()
	const callers = 1000
	e := newEnv(t)
	nodes := []node{e.instance(), e.instance(), e.instance()}
	ctx := t.Context()
	require.NoError(t, nodes[0].Set(ctx, "hot", 1, WithSetTTL(time.Second)))
	for _, n := range nodes {
		_, _, err := n.Get(ctx, "hot")
		require.NoError(t, err)
	}
	e.idle()
	e.clk.Advance(time.Second)

	var calls atomic.Int32
	load := LoadFunc[int](func(context.Context) (int, error) {
		calls.Add(1)
		eventually(t, func() bool {
			waits := 0.0
			for _, n := range nodes {
				waits += metricValue(n.m.lockWaits)
			}
			return waits == float64(len(nodes)-1)
		})
		return 2, nil
	})
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan int, callers)
	for i := range callers {
		n := nodes[i%len(nodes)]
		wg.Go(func() {
			<-startGate
			v, err := n.GetOrLoad(ctx, "hot", load)
			assert.NoError(t, err)
			results <- v
		})
	}
	close(startGate)
	wg.Wait()
	close(results)
	for v := range results {
		require.Equal(t, 2, v)
	}
	assert.Equal(t, int32(1), calls.Load(), "loader calls")
	for _, n := range nodes {
		assert.Zero(t, metricValue(n.m.fallbackLoads))
	}
}
