// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/pkg/cache"
)

type fakeSource struct {
	batches [][][]byte
	check   error

	mu    sync.Mutex
	acked [][][]byte
}

func (s *fakeSource) Consume(ctx context.Context, handle Handler) error {
	for _, b := range s.batches {
		if err := handle(ctx, b); err != nil {
			return err
		}
		s.mu.Lock()
		s.acked = append(s.acked, b)
		s.mu.Unlock()
	}
	<-ctx.Done()
	return nil
}

func (s *fakeSource) Check(context.Context) error { return s.check }

func (s *fakeSource) ackedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.acked)
}

type failingSource struct{ err error }

func (s failingSource) Consume(context.Context, Handler) error { return s.err }

var usersMapping = map[string][]string{"public.users": {"user:{id}", "user:email:{email}"}}

type harness struct {
	inv *Invalidator
	src *fakeSource
	del *fakeDeleter
	reg *prometheus.Registry
	clk *clock.Fake
	log *bytes.Buffer
}

func newHarness(t *testing.T, del *fakeDeleter, batches [][][]byte, opts ...Option) *harness {
	t.Helper()
	h := &harness{
		src: &fakeSource{batches: batches},
		del: del,
		reg: prometheus.NewRegistry(),
		clk: clock.NewFake(time.Unix(1_760_000_000, 0)),
		log: &bytes.Buffer{},
	}
	all := append([]Option{
		WithMetrics(h.reg), WithClock(h.clk), WithRand(rand.New(rand.NewPCG(1, 2))),
		WithLogger(slog.New(slog.NewJSONHandler(&syncWriter{w: h.log}, nil))),
	}, opts...)
	inv, err := New(h.src, mustMapping(t, usersMapping), del, all...)
	require.NoError(t, err)
	h.inv = inv
	return h
}

func (h *harness) run(t *testing.T) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- h.inv.Run(ctx) }()
	return func() {
		cancel()
		require.NoError(t, <-done)
	}
}

func (h *harness) counter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	return counterValue(t, h.reg, name, labels)
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()

	m := mustMapping(t, usersMapping)
	src, del := &fakeSource{}, newFakeDeleter()
	tests := []struct {
		name string
		call func() (*Invalidator, error)
	}{
		{"nil source", func() (*Invalidator, error) { return New(nil, m, del) }},
		{"nil mapping", func() (*Invalidator, error) { return New(src, nil, del) }},
		{"nil deleter", func() (*Invalidator, error) { return New(src, m, nil) }},
		{"nil logger", func() (*Invalidator, error) { return New(src, m, del, WithLogger(nil)) }},
		{"nil clock", func() (*Invalidator, error) { return New(src, m, del, WithClock(nil)) }},
		{"nil rand", func() (*Invalidator, error) { return New(src, m, del, WithRand(nil)) }},
		{"zero retry", func() (*Invalidator, error) { return New(src, m, del, WithRetry(0, time.Second)) }},
		{"max below min", func() (*Invalidator, error) { return New(src, m, del, WithRetry(time.Second, time.Millisecond)) }},
		{"zero delete timeout", func() (*Invalidator, error) { return New(src, m, del, WithDeleteTimeout(0)) }},
		{"conflicting metric", func() (*Invalidator, error) {
			reg := prometheus.NewRegistry()
			reg.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{Name: "wake_cdcsource_events_total"}))
			return New(src, m, del, WithMetrics(reg))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inv, err := tc.call()

			require.ErrorIs(t, err, ErrInvalidConfig)
			assert.Nil(t, inv)
		})
	}
}

func TestNew_SharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	m := mustMapping(t, usersMapping)
	for range 2 {
		_, err := New(&fakeSource{}, m, newFakeDeleter(), WithMetrics(reg), WithRetry(time.Millisecond, time.Second),
			WithDeleteTimeout(time.Second), WithInvalidateOnSnapshot(true))
		require.NoError(t, err)
	}
}

func TestInvalidator_Run_SkipRules(t *testing.T) {
	t.Parallel()

	snapshot := evt("r", "public.users", `{"id":5}`, "null", `{"id":5,"email":"s@example.com"}`)
	batch := [][]byte{
		nil,
		{},
		[]byte("{"),
		evt("x", "public.users", `{"id":1}`, "null", `{"id":1}`),
		evt("t", "public.users", "null", "null", "null"),
		snapshot,
		evt("c", "public.orders", `{"id":1}`, "null", `{"id":1}`),
		evt("c", "public.users", `{"id":null}`, "null", `{"id":null}`),
		evt("c", "public.users", `{"id":7}`, "null", `{"id":7,"email":"a@example.com"}`),
		evt("c", "public.users", `{"id":7}`, "null", `{"id":7,"email":"a@example.com"}`),
	}
	tests := []struct {
		name       string
		onSnapshot bool
		want       []string
		snapSkips  float64
	}{
		{"snapshot ignored by default", false, []string{"user:7", "user:email:a@example.com"}, 1},
		{
			"snapshot invalidates when enabled", true,
			[]string{"user:5", "user:email:s@example.com", "user:7", "user:email:a@example.com"},
			0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, newFakeDeleter(), [][][]byte{batch}, WithInvalidateOnSnapshot(tc.onSnapshot))

			stop := h.run(t)
			eventually(t, func() bool { return h.src.ackedCount() == 1 })
			stop()

			assert.Equal(t, tc.want, h.del.deleted(), "keys deduplicated across the batch")
			assert.Equal(t, 1, h.del.callCount(), "one Delete per batch")
			assert.InDelta(t, 10, h.counter(t, "wake_cdcsource_events_total", nil), 0)
			for reason, want := range map[string]float64{
				reasonTombstone: 2, reasonInvalid: 1, reasonUnknownOp: 1, reasonTruncate: 1,
				reasonSnapshot: tc.snapSkips, reasonUnmappedTable: 1, reasonNoKeys: 1,
			} {
				got := h.counter(t, "wake_cdcsource_events_skipped_total", map[string]string{"reason": reason})
				assert.InDelta(t, want, got, 0, reason)
			}
			assert.InDelta(t, float64(len(tc.want)), h.counter(t, "wake_cdcsource_keys_deleted_total", nil), 0)
		})
	}
}

func TestInvalidator_Run_BatchWithoutKeysIsAcknowledged(t *testing.T) {
	t.Parallel()
	h := newHarness(t, newFakeDeleter(), [][][]byte{{nil}})

	stop := h.run(t)
	eventually(t, func() bool { return h.src.ackedCount() == 1 })
	stop()

	assert.Zero(t, h.del.callCount())
}

func TestInvalidator_Run_ChunksLargeBatches(t *testing.T) {
	t.Parallel()
	batch := make([][]byte, 0, maxKeysPerDelete)
	for i := range maxKeysPerDelete {
		batch = append(batch, evt("c", "public.users", fmt.Sprintf(`{"id":%d}`, i), "null",
			fmt.Sprintf(`{"id":%d,"email":"u%d@example.com"}`, i, i)))
	}
	h := newHarness(t, newFakeDeleter(), [][][]byte{batch})

	stop := h.run(t)
	eventually(t, func() bool { return h.src.ackedCount() == 1 })
	stop()

	assert.Equal(t, 2, h.del.callCount())
	assert.Len(t, h.del.deleted(), 2*maxKeysPerDelete)
}

func TestInvalidator_Run_RetriesUntilDeleteSucceeds(t *testing.T) {
	t.Parallel()
	deferred := fmt.Errorf("delete user:1: %w", cache.ErrDeferred)
	del := newFakeDeleter(deferred, deferred, errors.New("redis: connection refused"))
	batch := [][]byte{evt("u", "public.users", `{"id":1}`, `{"id":1,"email":"a"}`, `{"id":1,"email":"b"}`)}
	h := newHarness(t, del, [][][]byte{batch}, WithRetry(100*time.Millisecond, 150*time.Millisecond))

	stop := h.run(t)
	defer stop()
	for attempt := 1; attempt <= 3; attempt++ {
		<-del.called
		assert.Zero(t, h.src.ackedCount(), "not acknowledged after failed attempt %d", attempt)
		eventually(t, func() bool { return h.clk.Waiters() == 1 })
		h.clk.Advance(150 * time.Millisecond)
	}
	eventually(t, func() bool { return h.src.ackedCount() == 1 })

	assert.Equal(t, 4, del.callCount())
	assert.Equal(t, []string{"user:1", "user:email:a", "user:email:b"}, del.deleted())
	assert.InDelta(t, 2, h.counter(t, "wake_cdcsource_delete_failures_total", map[string]string{"kind": "deferred"}), 0)
	assert.InDelta(t, 1, h.counter(t, "wake_cdcsource_delete_failures_total", map[string]string{"kind": "error"}), 0)
	assert.Contains(t, h.log.String(), "cache delete failed; retrying")
}

func TestInvalidator_Run_StopsDuringRetryWithoutAck(t *testing.T) {
	t.Parallel()
	del := newFakeDeleter(cache.ErrDeferred)
	h := newHarness(t, del, [][][]byte{{evt("c", "public.users", `{"id":1}`, "null", `{"id":1}`)}})

	stop := h.run(t)
	<-del.called
	eventually(t, func() bool { return h.clk.Waiters() == 1 })
	stop()

	assert.Zero(t, h.src.ackedCount())
}

func TestInvalidator_DeleteWithRetry_ContextEndsDuringDelete(t *testing.T) {
	t.Parallel()
	h := newHarness(t, newFakeDeleter(), nil)
	ctx, cancel := context.WithCancel(t.Context())
	h.inv.deleter = deleterFunc(func(context.Context, ...string) error {
		cancel()
		return context.Canceled
	})

	err := h.inv.deleteWithRetry(ctx, []string{"k"})

	require.ErrorIs(t, err, context.Canceled)
}

type deleterFunc func(ctx context.Context, keys ...string) error

func (f deleterFunc) Delete(ctx context.Context, keys ...string) error { return f(ctx, keys...) }

func TestInvalidator_Backoff(t *testing.T) {
	t.Parallel()
	h := newHarness(t, newFakeDeleter(), nil, WithRetry(100*time.Millisecond, 5*time.Second))

	for attempt, ceiling := range []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
		1600 * time.Millisecond, 3200 * time.Millisecond, 5 * time.Second, 5 * time.Second,
	} {
		d := h.inv.backoff(attempt)
		assert.GreaterOrEqual(t, d, ceiling/2, "attempt %d", attempt)
		assert.LessOrEqual(t, d, ceiling, "attempt %d", attempt)
	}
	assert.LessOrEqual(t, h.inv.backoff(1000), 5*time.Second, "capped for any attempt")
}

func TestInvalidator_FullIdentityDetection(t *testing.T) {
	t.Parallel()
	batch := [][]byte{
		evt("u", "public.users", `{"id":1}`, `{"id":1,"email":"a"}`, `{"id":1,"email":"b"}`),
		evt("c", "public.users", `{"id":2}`, "null", `{"id":2,"email":"c"}`),
		evt("u", "public.users", `{"id":3}`, `{"id":3}`, `{"id":3,"email":"d"}`),
		evt("d", "public.users", `{"id":4}`, `{"id":4}`, "null"),
	}
	h := newHarness(t, newFakeDeleter(), [][][]byte{batch})
	require.NoError(t, h.inv.Ready(t.Context()))

	stop := h.run(t)
	eventually(t, func() bool { return h.src.ackedCount() == 1 })
	stop()

	err := h.inv.Ready(t.Context())
	require.ErrorIs(t, err, ErrRequiresFullIdentity)
	var ie *IdentityError
	require.ErrorAs(t, err, &ie)
	assert.Equal(t, &IdentityError{Table: "public.users", Columns: []string{"email"}}, ie)
	assert.InDelta(t, 2, h.counter(t, "wake_cdcsource_template_requires_full_identity_total",
		map[string]string{"table": "public.users"}), 0)
	assert.Equal(t, 1, bytes.Count(h.log.Bytes(), []byte("template_requires_full_identity")), "logged once per table")
	assert.Equal(t, []string{
		"user:1", "user:email:a", "user:email:b", "user:2", "user:email:c", "user:3", "user:email:d", "user:4",
	}, h.del.deleted(), "processing continues with the images available")
}

func TestInvalidator_Ready_SourceHealth(t *testing.T) {
	t.Parallel()
	h := newHarness(t, newFakeDeleter(), nil)
	h.src.check = errors.New("broker down")

	assert.EqualError(t, h.inv.Ready(t.Context()), "broker down")
}

func TestInvalidator_Run(t *testing.T) {
	t.Parallel()

	t.Run("twice", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, newFakeDeleter(), nil)
		stop := h.run(t)
		defer stop()
		eventually(t, func() bool { return h.inv.running.Load() })

		require.ErrorIs(t, h.inv.Run(t.Context()), ErrInvalidConfig)
	})

	t.Run("source failure", func(t *testing.T) {
		t.Parallel()
		inv, err := New(failingSource{err: errors.New("boom")}, mustMapping(t, usersMapping), newFakeDeleter())
		require.NoError(t, err)

		err = inv.Run(t.Context())

		require.EqualError(t, err, "cdcsource: consume: boom")
	})

	t.Run("source returns", func(t *testing.T) {
		t.Parallel()
		inv, err := New(failingSource{}, mustMapping(t, usersMapping), newFakeDeleter())
		require.NoError(t, err)

		require.NoError(t, inv.Run(t.Context()))
	})
}
