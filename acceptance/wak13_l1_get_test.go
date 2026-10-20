// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/pkg/cache"
)

const (
	// wak13Runs is the number of measured runs; the median is reported.
	wak13Runs = 3
	// wak13MinRun is the least duration of one run's timed loop.
	wak13MinRun = time.Second
	// wak13Warmup is the number of untimed calls before a run.
	wak13Warmup = 100_000
	// wak13AllocRuns is the number of calls testing.AllocsPerRun averages.
	wak13AllocRuns = 100_000
)

// wak13Value is the 64-byte value of WAK-13.
type wak13Value struct {
	A, B, C, D, E, F, G, H int64
}

// wak13Run measures Get on an L1 hit: one goroutine, metrics enabled on a
// registry (the cache binds its counters at New), a string key, no L2. The
// loop runs at least wak13MinRun after a warm-up (testing.Benchmark cannot be
// nested in a running benchmark); allocations come from
// testing.AllocsPerRun.
func wak13Run(c cache.Cache[wak13Value], key string) (nsPerOp, allocsPerOp float64, iters int, err error) {
	ctx := context.Background()
	misses := 0
	get := func() {
		if _, ok, _ := c.Get(ctx, key); !ok {
			misses++
		}
	}
	for range wak13Warmup {
		get()
	}
	n := wak13Warmup
	for {
		start := time.Now()
		for range n {
			get()
		}
		d := time.Since(start)
		if d >= wak13MinRun {
			nsPerOp, iters = float64(d.Nanoseconds())/float64(n), n
			break
		}
		n *= 2
	}
	allocsPerOp = testing.AllocsPerRun(wak13AllocRuns, get)
	if misses > 0 {
		return 0, 0, 0, fmt.Errorf("%d L1 misses on %s", misses, key)
	}
	return nsPerOp, allocsPerOp, iters, nil
}

// BenchmarkAcceptance_WAK13_L1Get implements WAK-13: L1 hit path Cache[V].Get under 200 ns/op with zero
// allocations.
func BenchmarkAcceptance_WAK13_L1Get(b *testing.B) {
	const id = "WAK-13"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(b, id, "load criterion: "+err.Error())
	}
	nsMax, err := c.Float("ns_per_op_max")
	if err != nil {
		harness.Fail(b, id, "threshold ns_per_op_max: "+err.Error())
	}
	allocsMax, err := c.Int("allocs_per_op_max")
	if err != nil {
		harness.Fail(b, id, "threshold allocs_per_op_max: "+err.Error())
	}
	valueBytes, err := c.Int("value_bytes")
	if err != nil {
		harness.Fail(b, id, "threshold value_bytes: "+err.Error())
	}
	if size := int64(unsafe.Sizeof(wak13Value{})); size != valueBytes {
		harness.Failf(b, id, "value is %d bytes, criterion asks for %d", size, valueBytes)
	}

	cc, err := cache.New[wak13Value]("wak13", cache.MsgpackCodec[wak13Value](),
		cache.WithMetrics(prometheus.NewRegistry()))
	if err != nil {
		harness.Fail(b, id, "cache.New: "+err.Error())
	}
	b.Cleanup(func() { _ = cc.Close() })
	const key = "user:42"
	if err := cc.Set(context.Background(), key, wak13Value{A: 42}); err != nil {
		harness.Fail(b, id, "Set: "+err.Error())
	}

	ns := make([]float64, 0, wak13Runs)
	allocs := make([]float64, 0, wak13Runs)
	runs := make([]string, 0, wak13Runs)
	for range wak13Runs {
		nsOp, allocsOp, iters, runErr := wak13Run(cc, key)
		if runErr != nil {
			harness.Fail(b, id, runErr.Error())
		}
		ns, allocs = append(ns, nsOp), append(allocs, allocsOp)
		runs = append(runs, fmt.Sprintf("%d calls %.2f ns/op %.0f allocs/op", iters, nsOp, allocsOp))
	}
	nsMed, nsMin, nsMaxRun := median(ns), slices.Min(ns), slices.Max(ns)
	allocsWorst := slices.Max(allocs)
	b.ReportMetric(nsMed, "median-ns/op")
	b.ReportMetric(allocsWorst, "max-allocs/op")
	harness.Finish(b, harness.Result{
		ID: id,
		Measured: map[string]any{
			"ns_per_op": nsMed, "ns_per_op_min": nsMin, "ns_per_op_max_run": nsMaxRun,
			"allocs_per_op": allocsWorst, "value_bytes": valueBytes, "runs": wak13Runs,
		},
		Pass: nsMed < nsMax && allocsWorst <= float64(allocsMax),
		Details: fmt.Sprintf("Cache[V].Get on an L1 hit, one goroutine, metrics on a registry, msgpack codec, "+
			"%d-byte value, string key, no L2; %d timed runs of at least 1s each, median %.2f ns/op (min %.2f, max "+
			"%.2f), allocs/op max over runs %.0f; runs: %v", valueBytes, wak13Runs, nsMed, nsMin, nsMaxRun,
			allocsWorst, runs),
	})
}

// median returns the median of xs (the mean of the middle two for an even
// count); xs is not modified.
func median(xs []float64) float64 {
	s := slices.Sorted(slices.Values(xs))
	n := len(s)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}
