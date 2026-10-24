// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil/fakeemitter"
)

const hbInterval = time.Minute

func newHeartbeatEnv(t *testing.T, opts ...HeartbeatOption) (*Heartbeat, *fakeemitter.Emitter, *clock.Fake) {
	t.Helper()
	em, clk := fakeemitter.New(0x1000), clock.NewFake(epoch)
	hb, err := NewHeartbeat(em, clk, append([]HeartbeatOption{WithHeartbeatInterval(hbInterval)}, opts...)...)
	require.NoError(t, err)
	return hb, em, clk
}

func advance(t *testing.T, clk *clock.Fake, d time.Duration) {
	t.Helper()
	clk.Advance(d)
	waitWaiter(t, clk)
}

func tsJSON(t *testing.T, at time.Time) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]int64{"ts": at.UnixMilli()})
	require.NoError(t, err)
	return b
}

func TestNewHeartbeat_Invalid(t *testing.T) {
	t.Parallel()
	em, clk := fakeemitter.New(0), clock.NewFake(epoch)
	cases := []struct {
		name    string
		emitter pipeline.MessageEmitter
		clk     clock.Clock
		opts    []HeartbeatOption
	}{
		{name: "nil emitter", clk: clk},
		{name: "nil clock", emitter: em},
		{name: "zero interval", emitter: em, clk: clk, opts: []HeartbeatOption{WithHeartbeatInterval(0)}},
		{name: "nil logger", emitter: em, clk: clk, opts: []HeartbeatOption{WithHeartbeatLogger(nil)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewHeartbeat(tc.emitter, tc.clk, tc.opts...)
			require.ErrorIs(t, err, ErrInvalidOption)
		})
	}
}

func TestNewHeartbeat_DefaultInterval(t *testing.T) {
	t.Parallel()
	hb, err := NewHeartbeat(fakeemitter.New(0), clock.NewFake(epoch))
	require.NoError(t, err)
	assert.Equal(t, DefaultHeartbeatInterval, hb.interval)
}

func TestHeartbeat_Run_EmitsWhileIdle(t *testing.T) {
	t.Parallel()
	buf := &syncBuffer{}
	hb, em, clk := newHeartbeatEnv(t, WithHeartbeatLogger(jsonLogger(buf)))
	r := start(hb.Run)
	defer r.stop(t)
	waitWaiter(t, clk)
	advance(t, clk, hbInterval-time.Second)
	assert.Empty(t, em.Messages(), "not idle for a full interval yet")
	advance(t, clk, time.Second)
	advance(t, clk, hbInterval)
	assert.Equal(t, []fakeemitter.Message{
		{Prefix: HeartbeatPrefix, Content: tsJSON(t, epoch.Add(hbInterval)), LSN: lsn.LSN(0x1100)},
		{Prefix: HeartbeatPrefix, Content: tsJSON(t, epoch.Add(2*hbInterval)), LSN: lsn.LSN(0x1200)},
	}, em.Messages())
	assert.Contains(t, buf.String(), `"msg":"heartbeat emitted","component":"heartbeat","operation":"emit","lsn":"0/1100"`)
}

func TestHeartbeat_Run_NoHeartbeatWhileActive(t *testing.T) {
	t.Parallel()
	hb, em, clk := newHeartbeatEnv(t)
	r := start(hb.Run)
	defer r.stop(t)
	waitWaiter(t, clk)
	for range 5 {
		advance(t, clk, hbInterval/2)
		hb.Touch(clk.Now())
	}
	advance(t, clk, hbInterval/2)
	assert.Equal(t, 0, em.Attempts(), "activity within every interval")
	advance(t, clk, hbInterval/2)
	assert.Equal(t, 1, em.Attempts(), "idle for one interval since the last activity")
}

func TestHeartbeat_Run_RetriesAfterError(t *testing.T) {
	t.Parallel()
	buf := &syncBuffer{}
	hb, em, clk := newHeartbeatEnv(t, WithHeartbeatLogger(jsonLogger(buf)))
	em.Fail(assert.AnError)
	r := start(hb.Run)
	defer r.stop(t)
	waitWaiter(t, clk)
	advance(t, clk, hbInterval)
	assert.Equal(t, 1, em.Attempts())
	assert.Contains(t, buf.String(), `"component":"heartbeat","operation":"emit","error":"assert.AnError`)
	advance(t, clk, hbInterval-time.Second)
	assert.Equal(t, 1, em.Attempts(), "no retry before the next interval")
	em.Fail(nil)
	advance(t, clk, time.Second)
	assert.Len(t, em.Messages(), 1)
}

func TestHeartbeat_Touch_IgnoresEarlierTimes(t *testing.T) {
	t.Parallel()
	hb, _, _ := newHeartbeatEnv(t)
	cases := []struct {
		name string
		at   time.Time
		want time.Time
	}{
		{name: "later moves forward", at: epoch.Add(time.Minute), want: epoch.Add(time.Minute)},
		{name: "earlier is ignored", at: epoch, want: epoch.Add(time.Minute)},
		{name: "equal is kept", at: epoch.Add(time.Minute), want: epoch.Add(time.Minute)},
	}
	for _, tc := range cases {
		hb.Touch(tc.at)
		assert.Equal(t, tc.want.UnixNano(), hb.last.Load(), tc.name)
	}
}

func TestHeartbeat_Run_StopsOnCancel(t *testing.T) {
	t.Parallel()
	hb, em, clk := newHeartbeatEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clk.Advance(hbInterval)
	require.NoError(t, hb.Run(ctx))
	assert.Equal(t, 1, em.Attempts(), "the due heartbeat was attempted with the canceled context")
	assert.Empty(t, em.Messages())
}
