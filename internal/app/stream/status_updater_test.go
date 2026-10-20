// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil/fakerepl"
)

// handoffConfirmed is a ConfirmedSource whose change signal is unbuffered:
// a completed send proves that Run received the signal, and a second send
// proves that Run finished handling the first.
type handoffConfirmed struct {
	mu      sync.Mutex
	l       lsn.LSN
	changed chan struct{}
}

func newHandoff(l lsn.LSN) *handoffConfirmed {
	return &handoffConfirmed{l: l, changed: make(chan struct{})}
}

func (h *handoffConfirmed) Confirmed() lsn.LSN {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.l
}

func (h *handoffConfirmed) ConfirmedChanged() <-chan struct{} { return h.changed }

// advance sets the confirmed LSN and waits until Run has fully handled it.
func (h *handoffConfirmed) advance(l lsn.LSN) {
	h.mu.Lock()
	h.l = l
	h.mu.Unlock()
	h.changed <- struct{}{}
	h.changed <- struct{}{}
}

type updaterRun struct {
	cancel context.CancelFunc
	errc   chan error
}

// startUpdater runs u until the test ends and waits until its ticker is
// registered on clk.
func startUpdater(t *testing.T, u *StatusUpdater, clk *clock.Fake) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	run := &updaterRun{cancel: cancel, errc: make(chan error, 1)}
	waiters := clk.WaiterChange()
	go func() { run.errc <- u.Run(ctx) }()
	for clk.Waiters() == 0 {
		<-waiters
		waiters = clk.WaiterChange()
	}
	t.Cleanup(func() { run.stop(t) })
}

func (r *updaterRun) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	require.NoError(t, <-r.errc)
}

func newUpdater(
	t *testing.T, src ConfirmedSource, opts ...StatusUpdaterOption,
) (*StatusUpdater, *fakerepl.Source, *clock.Fake) {
	t.Helper()
	repl := fakerepl.New(1)
	clk := clock.NewFake(epoch)
	u, err := NewStatusUpdater(repl, src, clk, opts...)
	require.NoError(t, err)
	return u, repl, clk
}

func TestCheckStatusInterval(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name              string
		interval, timeout time.Duration
		wantErr           bool
	}{
		{name: "timeout disabled", interval: time.Hour, timeout: 0},
		{name: "defaults", interval: 10 * time.Second, timeout: 60 * time.Second},
		{name: "just below half", interval: 29 * time.Second, timeout: 60 * time.Second},
		{name: "exactly half", interval: 30 * time.Second, timeout: 60 * time.Second, wantErr: true},
		{name: "above half", interval: 31 * time.Second, timeout: 60 * time.Second, wantErr: true},
		{name: "odd timeout", interval: 500 * time.Millisecond, timeout: 1001 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := CheckStatusInterval(tc.interval, tc.timeout)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrStatusInterval)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewStatusUpdater_Invalid(t *testing.T) {
	t.Parallel()
	repl, clk, conf := fakerepl.New(1), clock.NewFake(epoch), newHandoff(0)
	cases := []struct {
		name   string
		sender pipeline.StandbyStatusSender
		conf   ConfirmedSource
		clk    clock.Clock
		opts   []StatusUpdaterOption
	}{
		{name: "nil sender", conf: conf, clk: clk},
		{name: "nil confirmed", sender: repl, clk: clk},
		{name: "nil clock", sender: repl, conf: conf},
		{name: "zero interval", sender: repl, conf: conf, clk: clk, opts: []StatusUpdaterOption{WithStatusInterval(0)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewStatusUpdater(tc.sender, tc.conf, tc.clk, tc.opts...)
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

func TestStatusUpdater_Run_SendsEveryInterval(t *testing.T) {
	t.Parallel()
	conf := newHandoff(100)
	u, repl, clk := newUpdater(t, conf, WithStatusInterval(5*time.Second))
	assert.Equal(t, lsn.LSN(100), u.LastSent())
	startUpdater(t, u, clk)

	clk.Advance(4 * time.Second)
	assert.Empty(t, repl.Statuses())
	clk.Advance(time.Second)
	got, err := repl.WaitStatuses(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.StandbyStatus{Written: 100, Flushed: 100, ClientTime: epoch.Add(5 * time.Second)}, got[0])

	clk.Advance(5 * time.Second)
	got, err = repl.WaitStatuses(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, epoch.Add(10*time.Second), got[1].ClientTime, "unchanged LSN is still reported")
}

func TestStatusUpdater_Run_ConfirmedAdvance(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		minBytes uint64
		to       lsn.LSN
		wantSent bool
	}{
		{name: "below min confirm bytes", minBytes: 1 << 20, to: 100 + 1<<20 - 1},
		{name: "at min confirm bytes", minBytes: 1 << 20, to: 100 + 1<<20, wantSent: true},
		{name: "zero min sends any advance", minBytes: 0, to: 101, wantSent: true},
		{name: "no advance", minBytes: 0, to: 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conf := newHandoff(100)
			u, repl, clk := newUpdater(t, conf, WithMinConfirmBytes(tc.minBytes))
			startUpdater(t, u, clk)
			conf.advance(tc.to)
			if !tc.wantSent {
				assert.Empty(t, repl.Statuses())
				assert.Equal(t, lsn.LSN(100), u.LastSent())
				return
			}
			got := repl.Statuses()
			require.Len(t, got, 1)
			assert.Equal(t, tc.to, got[0].Flushed)
			assert.Equal(t, tc.to, u.LastSent())
		})
	}
}

func TestStatusUpdater_Run_DefaultMinConfirmBytes(t *testing.T) {
	t.Parallel()
	conf := newHandoff(0)
	u, repl, clk := newUpdater(t, conf)
	startUpdater(t, u, clk)
	conf.advance(DefaultMinConfirmBytes - 1)
	assert.Empty(t, repl.Statuses())
	conf.advance(DefaultMinConfirmBytes)
	assert.Len(t, repl.Statuses(), 1)
}

func TestStatusUpdater_Run_WithAckTracker(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100)
	u, repl, clk := newUpdater(t, tr, WithMinConfirmBytes(10))
	startUpdater(t, u, clk)
	tx := begin(t, tr, 200, 210)
	require.NoError(t, tr.Seal(tx))
	got, err := repl.WaitStatuses(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, lsn.LSN(210), got[0].Flushed)
}

func TestStatusUpdater_RequestReply_AnswersAtOnce(t *testing.T) {
	t.Parallel()
	u, repl, clk := newUpdater(t, newHandoff(100))
	startUpdater(t, u, clk)
	u.RequestReply()
	u.RequestReply() // coalesces, never blocks
	got, err := repl.WaitStatuses(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, lsn.LSN(100), got[0].Flushed)
	assert.Equal(t, epoch, got[0].ClientTime, "no clock advance needed")
}

// TestStatusUpdater_Run_SendsWhileReceiveBlocked models reading paused for
// backpressure: the reader is blocked (here in Receive, with nothing to
// read) and status updates still go out on time.
func TestStatusUpdater_Run_SendsWhileReceiveBlocked(t *testing.T) {
	t.Parallel()
	u, repl, clk := newUpdater(t, newHandoff(100), WithStatusInterval(time.Second))
	readerCtx, stopReader := context.WithCancel(context.Background())
	readerDone := make(chan error, 1)
	go func() {
		_, err := repl.Receive(readerCtx)
		readerDone <- err
	}()
	startUpdater(t, u, clk)
	for i := range 3 {
		clk.Advance(time.Second)
		_, err := repl.WaitStatuses(context.Background(), i+1)
		require.NoError(t, err)
	}
	stopReader()
	require.ErrorIs(t, <-readerDone, context.Canceled)
}

func TestStatusUpdater_ObserveReceived(t *testing.T) {
	t.Parallel()
	u, repl, _ := newUpdater(t, newHandoff(100))
	u.ObserveReceived(500)
	u.ObserveReceived(300) // never moves back
	require.NoError(t, u.Flush(context.Background()))
	u.ObserveReceived(50)
	got := repl.Statuses()
	require.Len(t, got, 1)
	assert.Equal(t, lsn.LSN(500), got[0].Written)
	assert.Equal(t, lsn.LSN(100), got[0].Flushed)

	u2, repl2, _ := newUpdater(t, newHandoff(100))
	u2.ObserveReceived(50)
	require.NoError(t, u2.Flush(context.Background()))
	assert.Equal(t, lsn.LSN(100), repl2.Statuses()[0].Written, "written is never behind flushed")
}

func TestStatusUpdater_Run_SendError(t *testing.T) {
	t.Parallel()
	u, repl, clk := newUpdater(t, newHandoff(100))
	repl.FailStatus(assert.AnError)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	waiters := clk.WaiterChange()
	go func() { errc <- u.Run(ctx) }()
	for clk.Waiters() == 0 {
		<-waiters
		waiters = clk.WaiterChange()
	}
	u.RequestReply()
	err := <-errc
	require.ErrorIs(t, err, assert.AnError)
	assert.Contains(t, err.Error(), "flushed 0/64")
	assert.Equal(t, lsn.LSN(100), u.LastSent())
}

// cancellingSender cancels the run's context and fails, as a send aborted
// by shutdown does.
type cancellingSender struct{ cancel context.CancelFunc }

func (c cancellingSender) SendStandbyStatus(_ context.Context, _ pipeline.StandbyStatus) error {
	c.cancel()
	return context.Canceled
}

func TestStatusUpdater_Run_ErrorAfterCancelIsNil(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := clock.NewFake(epoch)
	u, err := NewStatusUpdater(cancellingSender{cancel}, newHandoff(1), clk)
	require.NoError(t, err)
	u.RequestReply()
	require.NoError(t, u.Run(ctx))
}
