// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func keyed(n uint64, key string) pipeline.Record {
	return pipeline.Record{ID: change.StreamID(1, n), Key: []byte(key), Payload: []byte("0123456789")}
}

func barrier(n uint64) pipeline.Record {
	return pipeline.Record{ID: change.StreamID(1, n), Barrier: true, Payload: []byte("b")}
}

func ids(recs []pipeline.Record) []uint64 {
	out := make([]uint64, len(recs))
	for i := range recs {
		out[i] = recs[i].ID.N
	}
	return out
}

func TestLaneOf(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  string
		n    int
	}{
		{name: "single lane", key: "x", n: 1},
		{name: "zero lanes", key: "x", n: 0},
		{name: "eight lanes", key: `{"id":42}`, n: 8},
		{name: "empty key", key: "", n: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := pipeline.LaneOf([]byte(tc.key), tc.n)
			assert.Equal(t, got, pipeline.LaneOf([]byte(tc.key), tc.n), "deterministic")
			assert.GreaterOrEqual(t, got, 0)
			assert.Less(t, got, max(tc.n, 1))
		})
	}
}

func TestLaneOf_SpreadsKeys(t *testing.T) {
	t.Parallel()
	seen := map[int]bool{}
	for i := range 200 {
		seen[pipeline.LaneOf([]byte(`{"id":`+strconv.Itoa(i)+`}`), 8)] = true
	}
	assert.Len(t, seen, 8)
}

func TestNewLaneScheduler_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		lanes, capRec int
	}{
		{name: "zero lanes", lanes: 0, capRec: 1},
		{name: "zero capacity", lanes: 1, capRec: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pipeline.NewLaneScheduler(tc.lanes, tc.capRec)
			require.ErrorIs(t, err, pipeline.ErrInvalidLanes)
		})
	}
}

func TestLaneScheduler_Next_OneBatchPerLane(t *testing.T) {
	t.Parallel()
	s, err := pipeline.NewLaneScheduler(1, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, s.Lanes())
	require.NoError(t, s.Push(keyed(0, "a")))
	require.NoError(t, s.Push(keyed(1, "b")))
	b, ok := s.Next(pipeline.BatchLimits{MaxEvents: 1})
	require.True(t, ok)
	assert.Equal(t, []uint64{0}, ids(b.Records))
	assert.Equal(t, 1, s.Len())
	_, ok = s.Next(pipeline.BatchLimits{})
	assert.False(t, ok, "lane busy")
	require.NoError(t, s.Complete(b))
	b, ok = s.Next(pipeline.BatchLimits{})
	require.True(t, ok)
	assert.Equal(t, []uint64{1}, ids(b.Records))
	assert.Equal(t, 0, s.Len())
}

func TestLaneScheduler_Next_Limits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		limits pipeline.BatchLimits
		want   []uint64
	}{
		{name: "no limits takes all", limits: pipeline.BatchLimits{}, want: []uint64{0, 1, 2, 3}},
		{name: "max events", limits: pipeline.BatchLimits{MaxEvents: 3}, want: []uint64{0, 1, 2}},
		{name: "max bytes", limits: pipeline.BatchLimits{MaxBytes: 25}, want: []uint64{0, 1}},
		{name: "oversized first record still sent", limits: pipeline.BatchLimits{MaxBytes: 1}, want: []uint64{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := pipeline.NewLaneScheduler(1, 10)
			require.NoError(t, err)
			for i := range uint64(4) {
				require.NoError(t, s.Push(keyed(i, "k")))
			}
			b, ok := s.Next(tc.limits)
			require.True(t, ok)
			assert.Equal(t, 0, b.Lane)
			assert.Equal(t, tc.want, ids(b.Records))
		})
	}
}

func TestLaneScheduler_Push_Full(t *testing.T) {
	t.Parallel()
	s, err := pipeline.NewLaneScheduler(2, 2)
	require.NoError(t, err)
	require.NoError(t, s.Push(keyed(0, "a")))
	require.NoError(t, s.Push(barrier(1)))
	require.ErrorIs(t, s.Push(keyed(2, "a")), pipeline.ErrLanesFull)
	b, ok := s.Next(pipeline.BatchLimits{})
	require.True(t, ok)
	require.NoError(t, s.Push(keyed(2, "a")), "in-flight records do not count")
	require.NoError(t, s.Complete(b))
}

func TestLaneScheduler_Barrier_FlushesLanesAndGoesAlone(t *testing.T) {
	t.Parallel()
	s, err := pipeline.NewLaneScheduler(4, 100)
	require.NoError(t, err)
	require.NoError(t, s.Push(keyed(0, "a")))
	require.NoError(t, s.Push(keyed(1, "b")))
	require.NoError(t, s.Push(barrier(2)))
	require.NoError(t, s.Push(keyed(3, "a")))
	require.NoError(t, s.Push(barrier(4)))
	require.NoError(t, s.Push(barrier(5)))
	require.NoError(t, s.Push(keyed(6, "c")))

	var first []pipeline.LaneBatch
	for {
		b, ok := s.Next(pipeline.BatchLimits{})
		if !ok {
			break
		}
		first = append(first, b)
	}
	require.Len(t, first, 2, "only the records before the barrier")
	require.NoError(t, s.Complete(first[0]))
	_, ok := s.Next(pipeline.BatchLimits{})
	assert.False(t, ok, "barrier waits for every lane")
	require.NoError(t, s.Complete(first[1]))

	steps := [][]uint64{{2}, {3}, {4}, {5}, {6}}
	for _, want := range steps {
		b, ok := s.Next(pipeline.BatchLimits{})
		require.True(t, ok)
		assert.Equal(t, want, ids(b.Records))
		_, again := s.Next(pipeline.BatchLimits{})
		assert.False(t, again, "nothing passes an unfinished barrier or its predecessor")
		require.NoError(t, s.Complete(b))
	}
	assert.Equal(t, 0, s.Len())
}

func TestLaneScheduler_Complete_Unknown(t *testing.T) {
	t.Parallel()
	s, err := pipeline.NewLaneScheduler(2, 4)
	require.NoError(t, err)
	cases := []struct {
		name string
		b    pipeline.LaneBatch
	}{
		{name: "barrier not in flight", b: pipeline.LaneBatch{Lane: pipeline.BarrierLane}},
		{name: "idle lane", b: pipeline.LaneBatch{Lane: 1}},
		{name: "lane out of range", b: pipeline.LaneBatch{Lane: 2}},
		{name: "negative lane", b: pipeline.LaneBatch{Lane: -2}},
	}
	for _, tc := range cases {
		require.ErrorIs(t, s.Complete(tc.b), pipeline.ErrUnknownBatch, tc.name)
	}
}

func TestLaneScheduler_Property_OrderAndBarriers(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		lanes := rapid.IntRange(1, 5).Draw(t, "lanes")
		capacity := rapid.IntRange(1, 12).Draw(t, "capacity")
		limits := pipeline.BatchLimits{
			MaxEvents: rapid.IntRange(0, 4).Draw(t, "max_events"),
			MaxBytes:  rapid.IntRange(0, 40).Draw(t, "max_bytes"),
		}
		s, err := pipeline.NewLaneScheduler(lanes, capacity)
		require.NoError(t, err)
		m := newLaneModel()
		total := rapid.IntRange(0, 60).Draw(t, "total")
		var inflight []pipeline.LaneBatch
		for m.pushed < total || s.Len() > 0 || len(inflight) > 0 {
			switch op := rapid.IntRange(0, 2).Draw(t, "op"); {
			case op == 0 && m.pushed < total:
				rec := m.next(t)
				if err := s.Push(rec); err != nil {
					require.ErrorIs(t, err, pipeline.ErrLanesFull)
					require.Equal(t, capacity, s.Len())
					m.unpush()
				}
			case op == 1:
				if b, ok := s.Next(limits); ok {
					m.dispatch(t, b)
					inflight = append(inflight, b)
				}
			case len(inflight) > 0:
				i := rapid.IntRange(0, len(inflight)-1).Draw(t, "complete")
				require.NoError(t, s.Complete(inflight[i]))
				m.complete(inflight[i])
				inflight = append(inflight[:i], inflight[i+1:]...)
			}
		}
		require.Equal(t, total, m.completed, "every record dispatched and completed once")
	})
}

type laneModel struct {
	pushed, completed int
	keys              map[uint64]string
	lastPerKey        map[string]uint64
	dispatched        map[uint64]bool
	done              map[uint64]bool
	barrierOut        bool
}

func newLaneModel() *laneModel {
	return &laneModel{
		keys: map[uint64]string{}, lastPerKey: map[string]uint64{},
		dispatched: map[uint64]bool{}, done: map[uint64]bool{},
	}
}

func (m *laneModel) next(t *rapid.T) pipeline.Record {
	n := uint64(m.pushed) //nolint:gosec // G115: non-negative counter
	m.pushed++
	if rapid.IntRange(0, 5).Draw(t, "barrier") == 0 {
		m.keys[n] = ""
		return barrier(n)
	}
	key := "k" + strconv.Itoa(rapid.IntRange(0, 4).Draw(t, "key"))
	m.keys[n] = key
	return keyed(n, key)
}

func (m *laneModel) unpush() {
	m.pushed--
	delete(m.keys, uint64(m.pushed)) //nolint:gosec // G115: non-negative counter
}

func (m *laneModel) dispatch(t *rapid.T, b pipeline.LaneBatch) {
	require.False(t, m.barrierOut, "nothing is dispatched while a barrier is in flight")
	require.NotEmpty(t, b.Records)
	for _, r := range b.Records {
		n := r.ID.N
		require.False(t, m.dispatched[n], "dispatched twice")
		m.dispatched[n] = true
		for earlier := range n {
			isBarrier := m.keys[earlier] == ""
			switch {
			case r.Barrier:
				require.True(t, m.done[earlier], "barrier %d before earlier %d completed", n, earlier)
			case isBarrier:
				require.True(t, m.done[earlier], "record %d passed barrier %d", n, earlier)
			}
		}
		if r.Barrier {
			require.Equal(t, pipeline.BarrierLane, b.Lane)
			require.Len(t, b.Records, 1)
			m.barrierOut = true
			continue
		}
		key := m.keys[n]
		if last, ok := m.lastPerKey[key]; ok {
			require.Less(t, last, n, "key %s out of order", key)
		}
		m.lastPerKey[key] = n
	}
}

func (m *laneModel) complete(b pipeline.LaneBatch) {
	for _, r := range b.Records {
		m.done[r.ID.N] = true
		m.completed++
	}
	if b.Lane == pipeline.BarrierLane {
		m.barrierOut = false
	}
}
