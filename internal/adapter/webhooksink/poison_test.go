// SPDX-License-Identifier: Apache-2.0

package webhooksink_test

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

func TestSink_Poison_DeadLetteredAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	autoAdvance(t, clk, pipeline.DefaultBackoffMax)
	rv := newReceiver(t, clk, func(_ int, r *received) action {
		if strings.Contains(string(r.body), `"k":"bad"`) {
			return action{status: http.StatusUnprocessableEntity}
		}
		return action{}
	})
	dir := filepath.Join(t.TempDir(), "dlq")
	cfg := baseConfig(rv.url())
	cfg.Webhook.DeadLetterDir = dir
	cfg.Webhook.Lanes = 1
	obs := newRecorder()
	s := newSink(t, cfg, clk, webhooksink.WithObserver(obs), webhooksink.WithAckObserver(obs))
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "bad"))
	a.waitAcks(t, 1)
	send(t, s, keyedRec(a, 2, "good"))
	a.waitAcks(t, 2)

	reqs := rv.allRequests()
	require.Len(t, reqs, 4, "3 poison attempts, then the next batch")
	data, err := os.ReadFile(webhooksink.DeadLetterPath(dir, reqs[0].id))
	require.NoError(t, err)
	assert.Equal(t, string(keyedRec(a, 1, "bad").Payload)+"\n", string(data))
	assert.True(t, s.Health().IsHealthy())
	obs.mu.Lock()
	defer obs.mu.Unlock()
	assert.Equal(t, 1, obs.deadLettered)
	assert.Equal(t, 2, obs.retries[webhooksink.RetryReasonPoison])
	assert.Len(t, obs.ackTs, 1, "dead-lettered events have no end-to-end latency")
}

func TestSink_Poison_BlocksWithoutDeadLetterDirUntilAccepted(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	accept := make(chan struct{})
	rv := newReceiver(t, clk, func(int, *received) action {
		select {
		case <-accept:
			return action{}
		default:
			return action{status: http.StatusBadRequest}
		}
	})
	cfg := baseConfig(rv.url())
	cfg.Webhook.MaxAttempts = 2
	s := newSink(t, cfg, clk)
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "k"))

	rv.next(t)
	waitTimer(t, clk)
	assert.Equal(t, pipeline.SinkStateRetrying, s.Health().State, "fatal only after max_attempts")
	clk.Advance(pipeline.DefaultBackoffMax)
	for range 3 {
		rv.next(t)
		waitTimer(t, clk)
		h := s.Health()
		assert.Equal(t, pipeline.SinkStateFatal, h.State)
		assert.Equal(t, pipeline.CodeSinkFatal, h.Code)
		assert.Contains(t, h.Reason, "HTTP 400")
		clk.Advance(pipeline.DefaultBackoffMax)
	}
	assert.Equal(t, 0, a.count(), "a blocked poison batch is not acknowledged")

	close(accept)
	autoAdvance(t, clk, pipeline.DefaultBackoffMax)
	a.waitAcks(t, 1)
	waitHealth(t, s, pipeline.SinkStateHealthy)
	send(t, s, keyedRec(a, 2, "k"))
	a.waitAcks(t, 2)
}

func TestSink_Poison_DeadLetterWriteFailureBlocks(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	rv := newReceiver(t, clk, func(int, *received) action { return action{status: http.StatusForbidden} })
	blocker := filepath.Join(t.TempDir(), "dlq")
	require.NoError(t, os.WriteFile(blocker, []byte("not a directory"), 0o600))
	cfg := baseConfig(rv.url())
	cfg.Webhook.DeadLetterDir = blocker
	cfg.Webhook.MaxAttempts = 1
	obs := newRecorder()
	s := newSink(t, cfg, clk, webhooksink.WithObserver(obs))
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "k"))
	rv.next(t)
	h := waitHealth(t, s, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeSinkFatal, h.Code)
	assert.Equal(t, 1, obs.retriesFor(webhooksink.RetryReasonDeadLetter))

	require.NoError(t, os.Remove(blocker))
	waitTimer(t, clk)
	clk.Advance(pipeline.DefaultBackoffMax)
	a.waitAcks(t, 1)
	waitHealth(t, s, pipeline.SinkStateHealthy)
	entries, err := os.ReadDir(blocker)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, rv.allRequests()[1].id+".jsonl", entries[0].Name())
}

func TestSink_Redirect_IsPoison(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	rv := newReceiver(t, clk, func(int, *received) action { return action{status: http.StatusFound} })
	cfg := baseConfig(rv.url())
	cfg.Webhook.MaxAttempts = 1
	s := newSink(t, cfg, clk)
	start(t, s)
	send(t, s, keyedRec(newAcker(), 1, "k"))
	h := waitHealth(t, s, pipeline.SinkStateFatal)
	assert.Contains(t, h.Reason, "HTTP 302")
	require.Len(t, rv.allRequests(), 1, "redirects are not followed")
}
