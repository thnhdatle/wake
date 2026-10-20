// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/platform/clock"
)

type latencyRec struct {
	mu   sync.Mutex
	sink []string
	lat  []time.Duration
}

func (l *latencyRec) ObserveEndToEndLatency(sink string, latency time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sink = append(l.sink, sink)
	l.lat = append(l.lat, latency)
}

func TestWALStatusLevel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		status string
		want   int
	}{
		{name: "reserved", status: "reserved", want: WALStatusLevelReserved},
		{name: "extended", status: "extended", want: WALStatusLevelExtended},
		{name: "unreserved", status: "unreserved", want: WALStatusLevelUnreserved},
		{name: "lost", status: "lost", want: WALStatusLevelLost},
		{name: "empty", status: "", want: WALStatusLevelUnknown},
		{name: "unknown", status: "gone", want: WALStatusLevelUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, WALStatusLevel(tc.status))
		})
	}
}

func TestNewLatencyRecorder_Invalid(t *testing.T) {
	t.Parallel()
	_, err := NewLatencyRecorder(nil, clock.NewFake(epoch))
	require.ErrorIs(t, err, ErrInvalidOption)
	_, err = NewLatencyRecorder(&latencyRec{}, nil)
	require.ErrorIs(t, err, ErrInvalidOption)
}

func TestLatencyRecorder_Record(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		tsMS int64
		want time.Duration
	}{
		{name: "past commit", tsMS: epoch.Add(-150 * time.Millisecond).UnixMilli(), want: 150 * time.Millisecond},
		{name: "same instant", tsMS: epoch.UnixMilli(), want: 0},
		{name: "clock skew clamps to zero", tsMS: epoch.Add(time.Second).UnixMilli(), want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obs := &latencyRec{}
			r, err := NewLatencyRecorder(obs, clock.NewFake(epoch))
			require.NoError(t, err)
			r.Record("kafka", tc.tsMS)
			assert.Equal(t, []string{"kafka"}, obs.sink)
			assert.Equal(t, []time.Duration{tc.want}, obs.lat)
		})
	}
}

func TestNopObserver(t *testing.T) {
	t.Parallel()
	var o Observer = nopObserver{}
	o.ObserveSlot(1, "reserved")
	o.ObserveOldestUnackedAge(time.Second)
	o.ObservePollError(assert.AnError)
	o.SetAlertActive(RuleSinkFatal, true)
}
