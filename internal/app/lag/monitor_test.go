// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/fakealert"
	"github.com/thnhdatle/wake/internal/testutil/fakecatalog"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
)

const (
	testSlot     = "wake_app"
	testWarn     = 8 << 20
	testCritical = 64 << 20
)

type monitorEnv struct {
	cat      *fakecatalog.Catalog
	acks     *fakeAcks
	clk      *clock.Fake
	notifier *fakealert.Notifier
	obs      *recObserver
	sink     *fakesink.Sink
	mon      *Monitor
}

func newMonitorEnv(t *testing.T, opts ...MonitorOption) *monitorEnv {
	t.Helper()
	e := &monitorEnv{
		cat: fakecatalog.New(), acks: &fakeAcks{}, clk: clock.NewFake(epoch),
		notifier: fakealert.New(), obs: newRecObserver(), sink: fakesink.New("kafka"),
	}
	e.setLag(0)
	all := append([]MonitorOption{
		WithInterval(5 * time.Second), WithWarnBytes(testWarn), WithCriticalBytes(testCritical),
		WithMaxAckAge(time.Minute), WithSinks(e.sink), WithNotifier(e.notifier), WithObserver(e.obs),
		WithLogger(testutil.DiscardLogger()),
	}, opts...)
	mon, err := NewMonitor(e.cat, testSlot, e.acks, e.clk, all...)
	require.NoError(t, err)
	e.mon = mon
	return e
}

func (e *monitorEnv) setLag(n int64) {
	e.cat.SetSlot(pipeline.SlotStatus{Exists: true, LagBytes: n, WALStatus: "reserved"}, nil)
}

type ruleState struct {
	Rule  string
	State pipeline.AlertState
}

func states(alerts []pipeline.Alert) []ruleState {
	out := make([]ruleState, 0, len(alerts))
	for _, a := range alerts {
		out = append(out, ruleState{a.Rule, a.State})
	}
	return out
}

func TestNewMonitor_Invalid(t *testing.T) {
	t.Parallel()
	cat, acks, clk := fakecatalog.New(), &fakeAcks{}, clock.NewFake(epoch)
	cases := []struct {
		name  string
		slots SlotReader
		slot  string
		acks  AckAges
		clk   clock.Clock
		opts  []MonitorOption
	}{
		{name: "nil slots", slot: testSlot, acks: acks, clk: clk},
		{name: "nil acks", slots: cat, slot: testSlot, clk: clk},
		{name: "nil clock", slots: cat, slot: testSlot, acks: acks},
		{name: "empty slot", slots: cat, acks: acks, clk: clk},
		{name: "zero interval", slots: cat, slot: testSlot, acks: acks, clk: clk, opts: []MonitorOption{WithInterval(0)}},
		{name: "zero warn", slots: cat, slot: testSlot, acks: acks, clk: clk, opts: []MonitorOption{WithWarnBytes(0)}},
		{
			name: "critical not above warn", slots: cat, slot: testSlot, acks: acks, clk: clk,
			opts: []MonitorOption{WithWarnBytes(10), WithCriticalBytes(10)},
		},
		{name: "zero max ack age", slots: cat, slot: testSlot, acks: acks, clk: clk, opts: []MonitorOption{WithMaxAckAge(0)}},
		{name: "nil notifier", slots: cat, slot: testSlot, acks: acks, clk: clk, opts: []MonitorOption{WithNotifier(nil)}},
		{name: "nil observer", slots: cat, slot: testSlot, acks: acks, clk: clk, opts: []MonitorOption{WithObserver(nil)}},
		{name: "nil logger", slots: cat, slot: testSlot, acks: acks, clk: clk, opts: []MonitorOption{WithLogger(nil)}},
		{name: "nil sink", slots: cat, slot: testSlot, acks: acks, clk: clk, opts: []MonitorOption{WithSinks(nil)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewMonitor(tc.slots, tc.slot, tc.acks, tc.clk, tc.opts...)
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

func TestNewMonitor_Defaults(t *testing.T) {
	t.Parallel()
	m, err := NewMonitor(fakecatalog.New(), testSlot, &fakeAcks{}, clock.NewFake(epoch))
	require.NoError(t, err)
	assert.Equal(t, DefaultInterval, m.interval)
	assert.Equal(t, []float64{DefaultWarnBytes, DefaultCriticalBytes, DefaultMaxAckAge.Seconds(), 0},
		[]float64{m.rules[0].threshold, m.rules[1].threshold, m.rules[2].threshold, m.rules[3].threshold})
	m.Poll(context.Background())
}

func TestMonitor_Poll_SlotLagTransitions(t *testing.T) {
	t.Parallel()
	warn := func(s pipeline.AlertState) ruleState { return ruleState{RuleSlotLagWarn, s} }
	crit := func(s pipeline.AlertState) ruleState { return ruleState{RuleSlotLagCritical, s} }
	const firing, resolved = pipeline.AlertStateFiring, pipeline.AlertStateResolved
	cases := []struct {
		name       string
		lags       []int64
		want       []ruleState
		wantActive map[string]bool
	}{
		{name: "below warn", lags: []int64{0, testWarn - 1, testWarn}, want: []ruleState{}},
		{
			name: "warn then critical then resolve",
			lags: []int64{testWarn + 1, testCritical + 1, testCritical + 2, testWarn + 1, 0},
			want: []ruleState{warn(firing), crit(firing), crit(resolved), warn(resolved)},
		},
		{
			name: "straight to critical and back", lags: []int64{testCritical + 1, 1},
			want: []ruleState{warn(firing), crit(firing), warn(resolved), crit(resolved)},
		},
		{
			name: "flapping notifies every change once",
			lags: []int64{testWarn + 1, testWarn, testWarn + 1, testWarn + 1, testWarn},
			want: []ruleState{warn(firing), warn(resolved), warn(firing), warn(resolved)},
		},
		{
			name: "stays firing", lags: []int64{testWarn + 1, testWarn + 2, testWarn + 3},
			want:       []ruleState{warn(firing)},
			wantActive: map[string]bool{RuleSlotLagWarn: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newMonitorEnv(t)
			for _, l := range tc.lags {
				e.setLag(l)
				e.mon.Poll(context.Background())
				e.clk.Advance(5 * time.Second)
			}
			assert.Equal(t, tc.want, states(e.notifier.Alerts()))
			for _, rule := range []string{RuleSlotLagWarn, RuleSlotLagCritical} {
				active, _ := e.obs.isActive(rule)
				assert.Equal(t, tc.wantActive[rule], active, rule)
			}
			assert.Equal(t, tc.lags, e.obs.lagBytes)
		})
	}
}

func TestMonitor_Poll_AlertFields(t *testing.T) {
	t.Parallel()
	e := newMonitorEnv(t)
	e.clk.Advance(time.Second)
	e.setLag(testWarn + 5)
	e.mon.Poll(context.Background())
	assert.Equal(t, []pipeline.Alert{{
		Rule: RuleSlotLagWarn, State: pipeline.AlertStateFiring, Value: testWarn + 5, Threshold: testWarn,
		Ts: epoch.Add(time.Second),
	}}, e.notifier.Alerts())
	assert.Equal(t, []string{"reserved"}, e.obs.walStatus)
}

func TestMonitor_Poll_NotifierFailureRetries(t *testing.T) {
	t.Parallel()
	buf := &syncBuffer{}
	e := newMonitorEnv(t, WithLogger(jsonLogger(buf)))
	ctx := context.Background()
	e.notifier.Fail(assert.AnError)
	e.setLag(testWarn + 1)
	e.mon.Poll(ctx)
	e.clk.Advance(5 * time.Second)
	e.mon.Poll(ctx)
	assert.Empty(t, e.notifier.Alerts())
	active, _ := e.obs.isActive(RuleSlotLagWarn)
	assert.True(t, active, "the gauge follows the evaluation, not the delivery")
	assert.Contains(t, buf.String(), `"operation":"notify"`)
	assert.Contains(t, buf.String(), `"attempt":2`)

	e.notifier.Fail(nil)
	e.clk.Advance(5 * time.Second)
	e.mon.Poll(ctx)
	e.clk.Advance(5 * time.Second)
	e.mon.Poll(ctx)
	alerts := e.notifier.Alerts()
	require.Len(t, alerts, 1)
	assert.Equal(t, pipeline.AlertStateFiring, alerts[0].State)
	assert.Equal(t, epoch, alerts[0].Ts, "the state change keeps its crossing time")
}

func TestMonitor_Poll_LatestPendingStateWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("change undone before delivery is dropped", func(t *testing.T) {
		t.Parallel()
		e := newMonitorEnv(t)
		e.notifier.Fail(assert.AnError)
		e.setLag(testWarn + 1)
		e.mon.Poll(ctx)
		e.setLag(0)
		e.mon.Poll(ctx)
		e.notifier.Fail(nil)
		e.mon.Poll(ctx)
		assert.Empty(t, e.notifier.Alerts())
	})
	t.Run("only the latest change is delivered", func(t *testing.T) {
		t.Parallel()
		e := newMonitorEnv(t)
		e.setLag(testWarn + 1)
		e.mon.Poll(ctx)
		e.notifier.Fail(assert.AnError)
		e.setLag(0)
		e.mon.Poll(ctx)
		e.setLag(testWarn + 1)
		e.mon.Poll(ctx)
		e.setLag(0)
		e.mon.Poll(ctx)
		e.notifier.Fail(nil)
		e.mon.Poll(ctx)
		assert.Equal(t, []ruleState{
			{RuleSlotLagWarn, pipeline.AlertStateFiring}, {RuleSlotLagWarn, pipeline.AlertStateResolved},
		}, states(e.notifier.Alerts()))
	})
}

func TestMonitor_Poll_MaxAckAge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := newMonitorEnv(t)
	e.mon.Poll(ctx)
	e.acks.set(epoch, true)
	e.clk.Advance(time.Minute)
	e.mon.Poll(ctx)
	e.clk.Advance(time.Second)
	e.mon.Poll(ctx)
	e.acks.set(time.Time{}, false)
	e.mon.Poll(ctx)
	assert.Equal(t, []time.Duration{0, time.Minute, 61 * time.Second, 0}, e.obs.ages)
	assert.Equal(t, []pipeline.Alert{
		{Rule: RuleMaxAckAge, State: pipeline.AlertStateFiring, Value: 61, Threshold: 60, Ts: epoch.Add(61 * time.Second)},
		{Rule: RuleMaxAckAge, State: pipeline.AlertStateResolved, Value: 0, Threshold: 60, Ts: epoch.Add(61 * time.Second)},
	}, e.notifier.Alerts())
}

func TestMonitor_Poll_SinkFatal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	buf := &syncBuffer{}
	other := fakesink.New("webhook")
	e := newMonitorEnv(t, WithSinks(other), WithLogger(jsonLogger(buf)))
	e.sink.SetHealth(pipeline.SinkHealth{State: pipeline.SinkStateRetrying})
	e.mon.Poll(ctx)
	e.sink.SetHealth(pipeline.SinkHealth{
		State: pipeline.SinkStateFatal, Code: pipeline.CodeEventTooLarge, Reason: "table public.t lsn 0/10",
	})
	e.mon.Poll(ctx)
	other.SetHealth(pipeline.SinkHealth{State: pipeline.SinkStateFatal, Code: pipeline.CodeSinkFatal, Reason: "401"})
	e.mon.Poll(ctx)
	e.sink.SetHealth(pipeline.SinkHealth{})
	other.SetHealth(pipeline.SinkHealth{})
	e.mon.Poll(ctx)
	assert.Equal(t, []pipeline.Alert{
		{Rule: RuleSinkFatal, State: pipeline.AlertStateFiring, Value: 1, Threshold: 0, Ts: epoch},
		{Rule: RuleSinkFatal, State: pipeline.AlertStateResolved, Value: 0, Threshold: 0, Ts: epoch},
	}, e.notifier.Alerts())
	logs := buf.String()
	assert.Contains(t, logs, `"error":"sink kafka: event_too_large: table public.t lsn 0/10"`)
	assert.Contains(t, logs, `"msg":"alert sink_fatal firing (threshold 0)",`+
		`"component":"lag","operation":"alert","status":"firing","count":1`)
	assert.Contains(t, logs, `"status":"resolved","count":0`)
}

func TestMonitor_Poll_SlotErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		status  pipeline.SlotStatus
		err     error
		wantErr error
	}{
		{name: "query error", err: assert.AnError, wantErr: assert.AnError},
		{name: "slot missing", status: pipeline.SlotStatus{}, wantErr: ErrSlotNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			buf := &syncBuffer{}
			e := newMonitorEnv(t, WithLogger(jsonLogger(buf)))
			e.setLag(testWarn + 1)
			e.mon.Poll(ctx)
			e.cat.SetSlot(tc.status, tc.err)
			e.acks.set(epoch, true)
			e.clk.Advance(2 * time.Minute)
			e.mon.Poll(ctx)
			errs := e.obs.errors()
			require.Len(t, errs, 1)
			require.ErrorIs(t, errs[0], tc.wantErr)
			assert.Contains(t, buf.String(), `"operation":"poll","error":`)
			active, _ := e.obs.isActive(RuleSlotLagWarn)
			assert.True(t, active, "slot rules keep their state")
			active, _ = e.obs.isActive(RuleMaxAckAge)
			assert.True(t, active, "other rules are still evaluated")
			assert.Equal(t, []ruleState{
				{RuleSlotLagWarn, pipeline.AlertStateFiring}, {RuleMaxAckAge, pipeline.AlertStateFiring},
			}, states(e.notifier.Alerts()))
		})
	}
}

func TestMonitor_Poll_CanceledContextIsQuiet(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := newChanNotifier(clock.NewFake(epoch))
	n.fail(context.Canceled)
	e := newMonitorEnv(t, WithNotifier(n))
	e.acks.set(epoch.Add(-time.Hour), true)
	e.mon.Poll(ctx)
	assert.Empty(t, e.obs.errors())
	assert.Equal(t, RuleMaxAckAge, n.next(t).alert.Rule)
	assert.NotNil(t, e.mon.rules[2].pending, "the change stays pending")
}

func TestMonitor_Run_PollsAtStartAndStopsOnCancel(t *testing.T) {
	t.Parallel()
	e := newMonitorEnv(t)
	r := start(e.mon.Run)
	waitWaiter(t, e.clk)
	assert.Equal(t, []string{testSlot}, e.cat.SlotCalls())
	for _, rule := range []string{RuleSlotLagWarn, RuleSlotLagCritical, RuleMaxAckAge, RuleSinkFatal} {
		active, set := e.obs.isActive(rule)
		assert.True(t, set, rule)
		assert.False(t, active, rule)
	}
	r.stop(t)
	assert.Equal(t, 0, e.clk.Waiters(), "the ticker is stopped")
}

func TestMonitor_Run_AlertWithinOneInterval(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		failFirst    bool
		wantAttempts int
	}{
		{name: "delivered at once", wantAttempts: 1},
		{name: "retried after a failed delivery", failFirst: true, wantAttempts: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const interval = 5 * time.Second
			clk := clock.NewFake(epoch)
			n := newChanNotifier(clk)
			cat := fakecatalog.New()
			cat.SetSlot(pipeline.SlotStatus{Exists: true, LagBytes: 1 << 20, WALStatus: "reserved"}, nil)
			mon, err := NewMonitor(cat, testSlot, &fakeAcks{}, clk,
				WithInterval(interval), WithWarnBytes(testWarn), WithNotifier(n))
			require.NoError(t, err)
			r := start(mon.Run)
			defer r.stop(t)
			waitWaiter(t, clk)

			clk.Advance(2 * time.Second)
			tCross := clk.Now()
			cat.SetSlot(pipeline.SlotStatus{Exists: true, LagBytes: testWarn + 1, WALStatus: "reserved"}, nil)
			if tc.failFirst {
				n.fail(assert.AnError)
			}
			clk.Advance(interval - 2*time.Second)
			var got attempt
			for i := range tc.wantAttempts {
				got = n.next(t)
				if i == 0 && tc.failFirst {
					require.ErrorIs(t, got.err, assert.AnError)
					n.fail(nil)
					clk.Advance(interval)
				}
			}
			require.NoError(t, got.err)
			assert.Equal(t, RuleSlotLagWarn, got.alert.Rule)
			assert.Equal(t, pipeline.AlertStateFiring, got.alert.State)
			assert.LessOrEqual(t, got.alert.Ts.Sub(tCross), interval)
			assert.LessOrEqual(t, got.at.Sub(tCross), 60*time.Second)
		})
	}
}

func TestMonitor_Run_PollErrorDoesNotStop(t *testing.T) {
	t.Parallel()
	n := newChanNotifier(clock.NewFake(epoch))
	e := newMonitorEnv(t, WithNotifier(n))
	e.cat.QueueSlot(fakecatalog.SlotResult{Err: errors.New("connection reset")})
	r := start(e.mon.Run)
	defer r.stop(t)
	waitWaiter(t, e.clk)
	require.Len(t, e.obs.errors(), 1)
	e.setLag(testCritical + 1)
	e.clk.Advance(5 * time.Second)
	assert.Equal(t, RuleSlotLagWarn, n.next(t).alert.Rule)
	assert.Equal(t, RuleSlotLagCritical, n.next(t).alert.Rule)
	assert.Equal(t, []string{testSlot, testSlot}, e.cat.SlotCalls())
}
