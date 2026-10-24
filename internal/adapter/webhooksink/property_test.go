// SPDX-License-Identifier: Apache-2.0

package webhooksink_test

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

var retryable = []action{
	{status: http.StatusInternalServerError},
	{status: http.StatusServiceUnavailable},
	{status: http.StatusTooManyRequests},
	{status: http.StatusRequestTimeout},
	{reset: true},
}

func TestSink_Property_InjectedFailures(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		w := workload{
			lanes:     rapid.IntRange(1, 8).Draw(rt, "lanes"),
			maxEvents: rapid.IntRange(1, 16).Draw(rt, "max_events"),
			linger:    time.Duration(rapid.IntRange(0, 2).Draw(rt, "linger_ms")) * time.Millisecond,
			seed:      rapid.Uint64().Draw(rt, "seed"),
		}
		keys := rapid.IntRange(1, 24).Draw(rt, "keys")
		w.ops = rapid.SliceOfN(rapid.IntRange(-1, keys-1), 1, 120).Draw(rt, "ops")
		w.run(rt)
	})
}

type workload struct {
	lanes, maxEvents int
	linger           time.Duration
	seed             uint64
	ops              []int
}

func (w *workload) run(rt *rapid.T) {
	clk := clock.NewFake(epoch)
	rng := rand.New(rand.NewPCG(w.seed, 1))
	rv := newReceiver(rt, clk, func(int, *received) action {
		x := rng.Float64()
		switch {
		case x < 0.30:
			return retryable[rng.IntN(len(retryable))]
		case x < 0.35:
			return action{delay: time.Duration(1+rng.IntN(20)) * time.Millisecond}
		default:
			return action{}
		}
	})
	cfg := baseConfig(rv.url())
	cfg.Webhook.Lanes, cfg.MaxEvents, cfg.Linger = w.lanes, w.maxEvents, config.Duration(w.linger)
	backoff := pipeline.Backoff{Initial: time.Millisecond, Max: 8 * time.Millisecond}
	autoAdvance(rt, clk, backoff.Max)
	s := newSink(rt, cfg, clk, webhooksink.WithBackoff(backoff), webhooksink.WithRand(seeded(w.seed)))
	start(rt, s)

	a := newAcker()
	for i, k := range w.ops {
		seq := uint64(i + 1)
		if k < 0 {
			send(rt, s, barrierRec(a, seq))
		} else {
			send(rt, s, keyedRec(a, seq, "k"+strconv.Itoa(k)))
		}
	}
	a.waitAcks(rt, len(w.ops))
	assert.Equal(rt, 0, a.duplicates(), "acknowledged exactly once")
	checkRequests(rt, rv.allRequests())
	checkOrder(rt, w.ops, rv.acceptedRequests())
}

func checkRequests(t require.TestingT, reqs []*received) {
	bodies := map[string]string{}
	ids := map[string]string{}
	for _, r := range reqs {
		require.NoError(t, r.sigErr, "request %d", r.n)
		if b, ok := bodies[r.id]; ok {
			require.Equal(t, b, string(r.body), "delivery id %s reused for another body", r.id)
		}
		if id, ok := ids[string(r.body)]; ok {
			require.Equal(t, id, r.id, "a retried body changed its delivery id")
		}
		bodies[r.id], ids[string(r.body)] = string(r.body), r.id
	}
}

func checkOrder(t require.TestingT, ops []int, accepted []*received) {
	seen := map[string]bool{}
	last := map[string]uint64{}
	var maxSeq uint64
	for _, r := range accepted {
		for _, it := range r.items(t) {
			if seen[it.ID] {
				continue
			}
			seen[it.ID] = true
			if it.Type == "schema_change" {
				require.Less(t, maxSeq, it.N, "barrier %d after a later record", it.N)
				require.Len(t, seen, int(it.N), "barrier %d before an earlier record", it.N) //nolint:gosec // G115: small n
			}
			maxSeq = max(maxSeq, it.N)
			if it.Type == "schema_change" {
				continue
			}
			require.Less(t, last[it.K], it.N, "key %s: %d after %d", it.K, it.N, last[it.K])
			last[it.K] = it.N
		}
	}
	require.Len(t, seen, len(ops), "100% delivered")
}
