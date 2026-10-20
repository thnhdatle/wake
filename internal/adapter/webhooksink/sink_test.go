// SPDX-License-Identifier: Apache-2.0

package webhooksink_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
)

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	log := testutil.DiscardLogger()
	cases := []struct {
		name   string
		mutate func(c *config.Sink)
		getenv func(string) string
		want   error
	}{
		{name: "no name", mutate: func(c *config.Sink) { c.Name = "" }, want: webhooksink.ErrInvalidConfig},
		{name: "no webhook section", mutate: func(c *config.Sink) { c.Webhook = nil }, want: webhooksink.ErrInvalidConfig},
		{name: "bad url", mutate: func(c *config.Sink) { c.Webhook.URL = "ftp://x" }, want: webhooksink.ErrInvalidConfig},
		{name: "no host", mutate: func(c *config.Sink) { c.Webhook.URL = "http://" }, want: webhooksink.ErrInvalidConfig},
		{
			name: "no secret env", mutate: func(c *config.Sink) { c.Webhook.SecretEnv = "" },
			want: webhooksink.ErrInvalidConfig,
		},
		{name: "zero lanes", mutate: func(c *config.Sink) { c.Webhook.Lanes = 0 }, want: webhooksink.ErrInvalidConfig},
		{
			name: "negative attempts", mutate: func(c *config.Sink) { c.Webhook.MaxAttempts = -1 },
			want: webhooksink.ErrInvalidConfig,
		},
		{name: "zero max events", mutate: func(c *config.Sink) { c.MaxEvents = 0 }, want: webhooksink.ErrInvalidConfig},
		{name: "negative linger", mutate: func(c *config.Sink) { c.Linger = -1 }, want: webhooksink.ErrInvalidConfig},
		{name: "secret unset", getenv: func(string) string { return "" }, want: webhooksink.ErrMissingSecret},
		{name: "defaults for zero values", mutate: func(c *config.Sink) {
			c.Webhook.Timeout, c.Webhook.MaxAttempts, c.MaxEventBytes = 0, 0, 0
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := baseConfig("http://127.0.0.1:1/hook")
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			ge := getenv
			if tc.getenv != nil {
				ge = tc.getenv
			}
			s, err := webhooksink.New(cfg, ge, clk, log)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				assert.NotContains(t, err.Error(), testSecret)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "hook", s.Name())
			assert.True(t, s.Health().IsHealthy())
			assert.Equal(t, epoch, s.Health().Since)
		})
	}
	_, err := webhooksink.New(baseConfig("http://x"), nil, clk, log)
	require.ErrorIs(t, err, webhooksink.ErrInvalidConfig)
	_, err = webhooksink.New(baseConfig("http://x"), getenv, clk, log, webhooksink.WithQueueCapacity(-5))
	require.NoError(t, err, "a non-positive capacity means the default")
}

func TestSink_Send_DeliversEventsSchemaChangesAndMessages(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	rv := newReceiver(t, clk, nil)
	obs := newRecorder()
	s := newSink(t, baseConfig(rv.url()), clk, webhooksink.WithObserver(obs), webhooksink.WithAckObserver(obs))
	start(t, s)

	a := newAcker()
	src := change.Source{DB: "app", Schema: "public", Table: "users"}
	ev := pipeline.NewEventRecord(&change.Event{
		ID: change.StreamID(lsn.LSN(0x16B3748), 0), Source: src, Op: change.OpCreate, TsMs: 1760600000000,
		Key:   change.Row{{Name: "id", Value: []byte("42")}},
		After: change.Row{{Name: "id", Value: []byte("42")}, {Name: "email", Value: []byte(`"a@example.com"`)}},
	})
	ev.Ack = pipeline.NewAckHandle(a, pipeline.AckToken{Tx: 1})
	sc := pipeline.NewSchemaChangeRecord(&change.SchemaChange{
		ID: change.SchemaID(lsn.LSN(0x16C0000), 0), Source: src, TsMs: 1760600100000, SchemaVersion: 2,
		ColumnsAfter: []change.ColumnType{{Name: "id", Type: "int8"}},
	})
	sc.Ack = pipeline.NewAckHandle(a, pipeline.AckToken{Tx: 2})
	msg := pipeline.NewMessageRecord(&change.Message{
		ID: change.MessageID(lsn.LSN(0x16C0100), 0), TsMs: 1760600200000, Prefix: "app", Content: []byte("hi"),
	})
	msg.Ack = pipeline.NewAckHandle(a, pipeline.AckToken{Tx: 3})
	send(t, s, ev, sc, msg)

	wantTypes := []string{"", "schema_change", "message"}
	for i, want := range wantTypes {
		r := rv.next(t)
		require.NoError(t, r.sigErr, "request %d", i)
		assert.Equal(t, "application/json", r.contentType)
		assert.Len(t, r.id, 26, "ULID")
		items := r.items(t)
		require.Len(t, items, 1, "barriers are sent alone")
		assert.Equal(t, want, items[0].Type)
	}
	a.waitAcks(t, 3)
	assert.Equal(t, []uint64{1, 2, 3}, a.order)
	var first []json.RawMessage
	require.NoError(t, json.Unmarshal(rv.allRequests()[0].body, &first))
	assert.JSONEq(t, string(ev.Payload), string(first[0]), "the body holds the payloads unchanged")
	obs.mu.Lock()
	defer obs.mu.Unlock()
	assert.Equal(t, 3, obs.sent)
	assert.Equal(t, 3, obs.acked)
	assert.Equal(t, []int64{1760600000000, 1760600100000, 1760600200000}, obs.ackTs)
	assert.Equal(t, 3, obs.latencies)
	assert.Equal(t, 0, obs.inflight[len(obs.inflight)-1])
}

func TestSink_Retry_StableDeliveryIDAndBackoff(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	failures := []action{
		{status: http.StatusInternalServerError},
		{status: http.StatusServiceUnavailable},
		{status: http.StatusTooManyRequests},
		{status: http.StatusRequestTimeout},
		{reset: true},
	}
	rv := newReceiver(t, clk, func(n int, _ *received) action {
		if n < len(failures) {
			return failures[n]
		}
		return action{}
	})
	obs := newRecorder()
	s := newSink(t, baseConfig(rv.url()), clk, webhooksink.WithObserver(obs))
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "k1"), keyedRec(a, 2, "k1"))

	jitter := seeded(1)
	first := rv.next(t)
	for attempt := range failures {
		waitTimer(t, clk)
		h := waitHealth(t, s, pipeline.SinkStateRetrying)
		assert.Empty(t, h.Code)
		assert.NotEmpty(t, h.Reason)
		delay := pipeline.Backoff{}.Delay(attempt, jitter.Float64())
		clk.Advance(delay - time.Millisecond)
		require.Equal(t, 1, clk.Waiters(), "attempt %d retried before its backoff", attempt)
		rv.noRequest(t)
		clk.Advance(time.Millisecond)
		r := rv.next(t)
		require.NoError(t, r.sigErr)
		assert.Equal(t, first.id, r.id, "Wake-Delivery-Id is unchanged across retries")
		assert.Equal(t, first.body, r.body)
	}
	a.waitAcks(t, 2)
	waitHealth(t, s, pipeline.SinkStateHealthy)
	assert.Equal(t, 0, a.duplicates())
	assert.Equal(t, 2, obs.retriesFor(webhooksink.RetryReasonServerError))
	assert.Equal(t, 1, obs.retriesFor(webhooksink.RetryReasonRateLimited))
	assert.Equal(t, 1, obs.retriesFor(webhooksink.RetryReasonTimeout))
	assert.Equal(t, 1, obs.retriesFor(webhooksink.RetryReasonNetwork))
}

func TestSink_Retry_HonorsRetryAfter(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	rv := newReceiver(t, clk, func(n int, _ *received) action {
		if n == 0 {
			return action{status: http.StatusTooManyRequests, retryAfter: "3"}
		}
		return action{}
	})
	s := newSink(t, baseConfig(rv.url()), clk)
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "k"))
	rv.next(t)
	waitTimer(t, clk)
	clk.Advance(3*time.Second - time.Millisecond)
	require.Equal(t, 1, clk.Waiters(), "Retry-After is a lower bound of the delay")
	clk.Advance(time.Millisecond)
	rv.next(t)
	a.waitAcks(t, 1)
}

func TestSink_Retry_ClientTimeoutAndRefusedConnection(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	autoAdvance(t, clk, pipeline.DefaultBackoffMax)
	rv := newReceiver(t, clk, func(n int, _ *received) action {
		return action{hang: n == 0}
	})
	cfg := baseConfig(rv.url())
	cfg.Webhook.Timeout = config.Duration(100 * time.Millisecond)
	obs := newRecorder()
	s := newSink(t, cfg, clk, webhooksink.WithObserver(obs))
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "k"))
	a.waitAcks(t, 1)
	assert.Equal(t, 1, obs.retriesFor(webhooksink.RetryReasonNetwork))

	// Nothing listens on a closed listener's port: connection refused.
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	obs2 := newRecorder()
	s2 := newSink(t, baseConfig("http://"+addr+"/hook?token=sekrit"), clk, webhooksink.WithObserver(obs2))
	start(t, s2)
	send(t, s2, keyedRec(newAcker(), 1, "k"))
	h := waitHealth(t, s2, pipeline.SinkStateRetrying)
	assert.NotContains(t, h.Reason, "sekrit", "the URL is not repeated in reasons")
	assert.Positive(t, obs2.retriesFor(webhooksink.RetryReasonNetwork))
}

func TestSink_Send_EventTooLargeBlocks(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	rv := newReceiver(t, clk, nil)
	cfg := baseConfig(rv.url())
	cfg.MaxEventBytes = 16
	s := newSink(t, cfg, clk)
	start(t, s)
	a := newAcker()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Send(ctx, keyedRec(a, 1, "big")) }()

	h := waitHealth(t, s, pipeline.SinkStateFatal)
	assert.Equal(t, pipeline.CodeEventTooLarge, h.Code)
	assert.Contains(t, h.Reason, "public.users")
	assert.Contains(t, h.Reason, "0/16B3748")
	assert.Contains(t, h.Reason, "big")
	select {
	case err := <-done:
		require.FailNow(t, "Send returned", "%v", err)
	default:
	}
	cancel()
	err := <-done
	require.ErrorIs(t, err, webhooksink.ErrEventTooLarge)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, rv.allRequests(), "nothing is sent")
	assert.Equal(t, 0, a.count(), "nothing is acknowledged")
	assert.True(t, s.Health().IsHealthy())
}

// distinctLaneKeys returns n keys that hash into n different lanes.
func distinctLaneKeys(n, lanes int) []string {
	used := map[int]bool{}
	var keys []string
	for i := 0; len(keys) < n; i++ {
		k := "a" + strconv.Itoa(i)
		if l := pipeline.LaneOf([]byte(k), lanes); !used[l] {
			used[l] = true
			keys = append(keys, k)
		}
	}
	return keys
}

func TestSink_Barrier_WaitsForLanesAndIsSentAlone(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	gate := make(chan struct{})
	rv := newReceiver(t, clk, func(_ int, r *received) action {
		if strings.Contains(string(r.body), `"k":"a`) {
			return action{wait: gate}
		}
		return action{}
	})
	cfg := baseConfig(rv.url())
	s := newSink(t, cfg, clk)
	start(t, s)
	a := newAcker()
	for i, k := range distinctLaneKeys(4, cfg.Webhook.Lanes) {
		send(t, s, keyedRec(a, uint64(i+1), k))
	}
	send(t, s, barrierRec(a, 5), keyedRec(a, 6, "z"))
	for range 4 {
		items := rv.next(t).items(t)
		require.Len(t, items, 1)
		assert.Empty(t, items[0].Type, "barrier sent while lanes are busy")
	}
	rv.noRequest(t)
	close(gate)
	items := rv.next(t).items(t)
	require.Len(t, items, 1)
	assert.Equal(t, "schema_change", items[0].Type)
	after := rv.next(t).items(t)
	require.Len(t, after, 1)
	assert.Equal(t, uint64(6), after[0].N)
	a.waitAcks(t, 6)
	assert.Equal(t, uint64(5), a.order[4], "the barrier is acknowledged after every earlier record")
}

func TestSink_Batching_MaxEventsAndMaxBytes(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	gate := make(chan struct{})
	rv := newReceiver(t, clk, func(n int, _ *received) action {
		if n == 0 {
			return action{wait: gate}
		}
		return action{}
	})
	cfg := baseConfig(rv.url())
	cfg.Webhook.Lanes = 1
	cfg.MaxEvents = 3
	s := newSink(t, cfg, clk, webhooksink.WithQueueCapacity(10))
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 0, "k"))
	rv.next(t)
	for i := range uint64(7) {
		send(t, s, keyedRec(a, i+1, "k"))
	}
	close(gate)
	var sizes []int
	for len(sizes) < 3 {
		sizes = append(sizes, len(rv.next(t).items(t)))
	}
	assert.Equal(t, []int{3, 3, 1}, sizes)
	a.waitAcks(t, 8)

	// max_bytes: each payload is ~40 bytes, so a 100-byte limit takes 2.
	gate2 := make(chan struct{})
	rv2 := newReceiver(t, clk, func(n int, _ *received) action {
		if n == 0 {
			return action{wait: gate2}
		}
		return action{}
	})
	cfg2 := baseConfig(rv2.url())
	cfg2.Webhook.Lanes = 1
	cfg2.MaxBytes = 100
	s2 := newSink(t, cfg2, clk)
	start(t, s2)
	a2 := newAcker()
	send(t, s2, keyedRec(a2, 0, "k"))
	rv2.next(t)
	for i := range uint64(4) {
		send(t, s2, keyedRec(a2, i+1, "k"))
	}
	close(gate2)
	assert.Len(t, rv2.next(t).items(t), 2)
	assert.Len(t, rv2.next(t).items(t), 2)
	a2.waitAcks(t, 5)
}

func TestSink_Linger_WaitsForMoreRecords(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	rv := newReceiver(t, clk, nil)
	cfg := baseConfig(rv.url())
	cfg.Linger = config.Duration(10 * time.Millisecond)
	cfg.MaxEvents = 3
	cfg.Webhook.Lanes = 1
	s := newSink(t, cfg, clk)
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "k"))
	waitTimer(t, clk)
	rv.noRequest(t)
	send(t, s, keyedRec(a, 2, "k"))
	clk.Advance(9 * time.Millisecond)
	rv.noRequest(t)
	clk.Advance(time.Millisecond)
	assert.Len(t, rv.next(t).items(t), 2, "records sent within linger share a batch")

	// A full batch leaves at once.
	send(t, s, keyedRec(a, 3, "k"), keyedRec(a, 4, "k"), keyedRec(a, 5, "k"))
	assert.Len(t, rv.next(t).items(t), 3)
	a.waitAcks(t, 5)
}

func TestSink_Send_BlocksWhileQueueFull(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	gate := make(chan struct{})
	rv := newReceiver(t, clk, func(n int, _ *received) action {
		if n == 0 {
			return action{wait: gate}
		}
		return action{}
	})
	cfg := baseConfig(rv.url())
	cfg.Webhook.Lanes = 1
	s := newSink(t, cfg, clk, webhooksink.WithQueueCapacity(1))
	start(t, s)
	a := newAcker()
	send(t, s, keyedRec(a, 1, "k"))
	rv.next(t)
	send(t, s, keyedRec(a, 2, "k")) // queued behind the batch in flight

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.Send(ctx, keyedRec(a, 3, "k")), context.Canceled)

	done := make(chan error, 1)
	go func() { done <- s.Send(context.Background(), keyedRec(a, 3, "k")) }()
	select {
	case err := <-done:
		require.FailNow(t, "Send did not block", "%v", err)
	default:
	}
	close(gate)
	require.NoError(t, <-done)
	a.waitAcks(t, 3)
}

func TestSink_Run_Twice(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	s := newSink(t, baseConfig("http://127.0.0.1:1"), clk)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, s.Run(ctx))
	require.ErrorIs(t, s.Run(context.Background()), webhooksink.ErrAlreadyRunning)
}

func TestStatusError(t *testing.T) {
	t.Parallel()
	err := &webhooksink.StatusError{StatusCode: 418}
	assert.Equal(t, "webhooksink: receiver returned HTTP 418", err.Error())
}

func TestAckObserverFunc(t *testing.T) {
	t.Parallel()
	var got int64
	webhooksink.AckObserverFunc(func(_ string, ts int64) { got = ts }).ObserveAck("x", 7)
	assert.Equal(t, int64(7), got)
}
