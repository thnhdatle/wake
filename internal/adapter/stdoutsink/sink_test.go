// SPDX-License-Identifier: Apache-2.0

package stdoutsink_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/adapter/stdoutsink"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const waitTimeout = 20 * time.Second

var (
	epoch    = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	errWrite = errors.New("broken pipe")
)

// writer is a goroutine-safe buffer that can fail writes and flushes.
type writer struct {
	mu         sync.Mutex // protects every field
	buf        bytes.Buffer
	failWrites int // next writes that write half and fail
	failFlush  int // next flushes that fail
	flushes    int
	changed    chan struct{}
}

func newWriter() *writer { return &writer{changed: make(chan struct{})} }

func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.notifyLocked()
	if w.failWrites > 0 {
		w.failWrites--
		n := len(p) / 2
		w.buf.Write(p[:n])
		return n, errWrite
	}
	w.buf.Write(p) // bytes.Buffer.Write never fails
	return len(p), nil
}

func (w *writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer w.notifyLocked()
	w.flushes++
	if w.failFlush > 0 {
		w.failFlush--
		return errWrite
	}
	return nil
}

func (w *writer) notifyLocked() {
	close(w.changed)
	w.changed = make(chan struct{})
}

func (w *writer) flushCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushes
}

func (w *writer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// acker records acknowledged tokens.
type acker struct {
	mu      sync.Mutex // protects every field
	order   []uint64
	changed chan struct{}
}

func newAcker() *acker { return &acker{changed: make(chan struct{})} }

func (a *acker) Ack(tok pipeline.AckToken) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.order = append(a.order, tok.Tx)
	close(a.changed)
	a.changed = make(chan struct{})
	if tok.Tx == 99 {
		return errors.New("double ack")
	}
	return nil
}

func (a *acker) acked() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.order...)
}

func (a *acker) wait(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		a.mu.Lock()
		got, ch := len(a.order), a.changed
		a.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			require.FailNow(t, "acknowledgements missing", "got %d of %d", got, n)
		}
	}
}

func rec(a pipeline.Acker, n uint64) pipeline.Record {
	id := change.StreamID(lsn.LSN(0x100), n)
	return pipeline.Record{
		Kind: pipeline.RecordKindEvent, ID: id, Source: change.Source{Schema: "public", Table: "t"},
		Key: []byte(`{"id":1}`), Payload: fmt.Appendf(nil, `{"id":%q,"n":%d}`, id.String(), n),
		TsMs: epoch.UnixMilli() + int64(n),
		Ack:  pipeline.NewAckHandle(a, pipeline.AckToken{Tx: n}),
	}
}

func cfg() config.Sink {
	return config.Sink{Name: "out", Type: config.SinkStdout, MaxEvents: 10, MaxBytes: 1 << 20}
}

// recorder is an Observer and AckObserver.
type recorder struct {
	mu       sync.Mutex // protects every field
	sent     int
	acked    int
	retries  int
	latency  int
	inflight []int
	ackTs    []int64
}

func (r *recorder) ObserveSent(_ string, events, _ int) { r.mu.Lock(); r.sent += events; r.mu.Unlock() }

func (r *recorder) ObserveAcked(_ string, events int) { r.mu.Lock(); r.acked += events; r.mu.Unlock() }
func (r *recorder) ObserveRetry(_, _ string)          { r.mu.Lock(); r.retries++; r.mu.Unlock() }
func (r *recorder) ObserveLatency(string, time.Duration) {
	r.mu.Lock()
	r.latency++
	r.mu.Unlock()
}

func (r *recorder) SetInflight(_ string, n int) {
	r.mu.Lock()
	r.inflight = append(r.inflight, n)
	r.mu.Unlock()
}

func (r *recorder) ObserveAck(_ string, ts int64) {
	r.mu.Lock()
	r.ackTs = append(r.ackTs, ts)
	r.mu.Unlock()
}

func newSink(t *testing.T, c config.Sink, clk clock.Clock, opts ...stdoutsink.Option) *stdoutsink.Sink {
	t.Helper()
	opts = append([]stdoutsink.Option{stdoutsink.WithRand(rand.New(rand.NewPCG(1, 1)))}, opts...)
	s, err := stdoutsink.New(c, clk, testutil.DiscardLogger(), opts...)
	require.NoError(t, err)
	return s
}

func start(t *testing.T, s *stdoutsink.Sink) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

func waitTimer(t *testing.T, clk *clock.Fake) {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		ch := clk.WaiterChange()
		if clk.Waiters() == 1 {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			require.FailNow(t, "no timer")
		}
	}
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	log := testutil.DiscardLogger()
	cases := []struct {
		name   string
		mutate func(c *config.Sink)
		opts   []stdoutsink.Option
	}{
		{name: "no name", mutate: func(c *config.Sink) { c.Name = "" }},
		{name: "zero max events", mutate: func(c *config.Sink) { c.MaxEvents = 0 }},
		{name: "negative linger", mutate: func(c *config.Sink) { c.Linger = -1 }},
		{name: "nil writer", opts: []stdoutsink.Option{stdoutsink.WithWriter(nil)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := cfg()
			if tc.mutate != nil {
				tc.mutate(&c)
			}
			_, err := stdoutsink.New(c, clk, log, tc.opts...)
			require.ErrorIs(t, err, stdoutsink.ErrInvalidConfig)
		})
	}
	_, err := stdoutsink.New(cfg(), nil, log)
	require.ErrorIs(t, err, stdoutsink.ErrInvalidConfig)
	s, err := stdoutsink.New(cfg(), clk, log)
	require.NoError(t, err, "defaults to os.Stdout")
	assert.Equal(t, "out", s.Name())
	assert.True(t, s.Health().IsHealthy())
	assert.Equal(t, epoch, s.Health().Since)
}

func TestSink_Send_WritesJSONLinesAndAcks(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	w := newWriter()
	obs := &recorder{}
	s := newSink(t, cfg(), clk, stdoutsink.WithWriter(w), stdoutsink.WithObserver(obs), stdoutsink.WithAckObserver(obs))
	start(t, s)
	a := newAcker()
	for n := range uint64(25) {
		require.NoError(t, s.Send(context.Background(), rec(a, n)))
	}
	a.wait(t, 25)
	lines := strings.Split(strings.TrimSuffix(w.String(), "\n"), "\n")
	require.Len(t, lines, 25)
	for n, l := range lines {
		assert.JSONEq(t, string(rec(a, uint64(n)).Payload), l)
	}
	assert.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
		a.acked())
	obs.mu.Lock()
	defer obs.mu.Unlock()
	assert.Equal(t, 25, obs.sent)
	assert.Equal(t, 25, obs.acked)
	assert.Len(t, obs.ackTs, 25)
	assert.Equal(t, 0, obs.inflight[len(obs.inflight)-1])
}

func TestSink_Write_RetriesWithoutDuplicates(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	w := newWriter()
	w.failWrites, w.failFlush = 2, 1
	obs := &recorder{}
	s := newSink(t, cfg(), clk, stdoutsink.WithWriter(w), stdoutsink.WithObserver(obs))
	start(t, s)
	a := newAcker()
	require.NoError(t, s.Send(context.Background(), rec(a, 1)))
	for range 3 {
		waitTimer(t, clk)
		h := s.Health()
		assert.Equal(t, pipeline.SinkStateRetrying, h.State)
		assert.Contains(t, h.Reason, "broken pipe")
		assert.Empty(t, a.acked(), "nothing is acknowledged before the flush")
		clk.Advance(pipeline.DefaultBackoffMax)
	}
	a.wait(t, 1)
	assert.Equal(t, string(rec(a, 1).Payload)+"\n", w.String(), "a partial write resumes, no duplicate bytes")
	assert.True(t, s.Health().IsHealthy())
	obs.mu.Lock()
	defer obs.mu.Unlock()
	assert.Equal(t, 3, obs.retries)
}

func TestSink_Linger_CollectsBatch(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	w := newWriter()
	c := cfg()
	c.Linger = config.Duration(10 * time.Millisecond)
	c.MaxEvents = 3
	s := newSink(t, c, clk, stdoutsink.WithWriter(w))
	start(t, s)
	a := newAcker()
	require.NoError(t, s.Send(context.Background(), rec(a, 1)))
	waitTimer(t, clk)
	require.NoError(t, s.Send(context.Background(), rec(a, 2)))
	assert.Empty(t, w.String())
	clk.Advance(10 * time.Millisecond)
	a.wait(t, 2)
	assert.Equal(t, 1, w.flushCount(), "one batch")

	for n := range uint64(3) {
		require.NoError(t, s.Send(context.Background(), rec(a, 3+n)))
	}
	a.wait(t, 5)
	assert.Equal(t, 2, w.flushCount(), "a full batch leaves without lingering")
}

func TestSink_Send_EventTooLargeBlocks(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	w := newWriter()
	c := cfg()
	c.MaxEventBytes = 8
	s := newSink(t, c, clk, stdoutsink.WithWriter(w))
	start(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	a := newAcker()
	go func() { done <- s.Send(ctx, rec(a, 1)) }()
	deadline := time.After(waitTimeout)
	for !s.Health().IsFatal() {
		select {
		case <-deadline:
			require.FailNow(t, "not fatal")
		case <-time.After(time.Millisecond):
		}
	}
	h := s.Health()
	assert.Equal(t, pipeline.CodeEventTooLarge, h.Code)
	assert.Contains(t, h.Reason, "public.t")
	cancel()
	err := <-done
	require.ErrorIs(t, err, stdoutsink.ErrEventTooLarge)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, w.String())
	assert.Empty(t, a.acked())
	assert.True(t, s.Health().IsHealthy())
}

func TestSink_Send_BlocksWhileQueueFull(t *testing.T) {
	t.Parallel()
	s := newSink(t, cfg(), clock.NewFake(epoch), stdoutsink.WithWriter(newWriter()),
		stdoutsink.WithQueueCapacity(1))
	a := newAcker()
	require.NoError(t, s.Send(context.Background(), rec(a, 1)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.Send(ctx, rec(a, 2)), context.Canceled)
	start(t, s)
	require.NoError(t, s.Send(context.Background(), rec(a, 2)))
	a.wait(t, 2)
}

func TestSink_Ack_ErrorIsLogged(t *testing.T) {
	t.Parallel()
	w := newWriter()
	s := newSink(t, cfg(), clock.NewFake(epoch), stdoutsink.WithWriter(w))
	start(t, s)
	a := newAcker()
	require.NoError(t, s.Send(context.Background(), rec(a, 99)))
	require.NoError(t, s.Send(context.Background(), rec(a, 1)))
	a.wait(t, 2)
}

func TestSink_Run_Twice(t *testing.T) {
	t.Parallel()
	s := newSink(t, cfg(), clock.NewFake(epoch), stdoutsink.WithWriter(newWriter()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, s.Run(ctx))
	require.ErrorIs(t, s.Run(context.Background()), stdoutsink.ErrAlreadyRunning)
}
