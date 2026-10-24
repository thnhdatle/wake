// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
)

type ackModel struct {
	t       *rapid.T
	tracker *AckTracker
	router  *Router
	sinks   []*fakesink.Sink
	start   lsn.LSN
	lastEnd lsn.LSN
	txs     []*modelTx
	byID    map[change.ID]int
	nextN   uint64
	prev    lsn.LSN
}

type modelTx struct {
	commit, end lsn.LSN
	handle      TxHandle
	pending     int
	sealed      bool
	group       Group
	groupOpen   bool
	records     uint64
}

func (tx *modelTx) complete() bool { return tx.sealed && tx.pending == 0 }

func newAckModel(t *rapid.T) *ackModel {
	start := lsn.LSN(rapid.Uint64Range(1, 1000).Draw(t, "start"))
	tracker, err := NewAckTracker(start)
	require.NoError(t, err)
	nSinks := rapid.IntRange(1, 4).Draw(t, "sinks")
	routes := make([]SinkRoute, nSinks)
	sinks := make([]*fakesink.Sink, nSinks)
	for i := range nSinks {
		sinks[i] = fakesink.New("s" + strconv.Itoa(i))
		var tables []string
		for tb := range 4 {
			if rapid.Bool().Draw(t, "route") {
				tables = append(tables, "public.t"+strconv.Itoa(tb))
			}
		}
		routes[i] = SinkRoute{Sink: sinks[i], Tables: tables}
	}
	router, err := NewRouter(tracker, routes)
	require.NoError(t, err)
	return &ackModel{
		t: t, tracker: tracker, router: router, sinks: sinks, start: start, lastEnd: start,
		byID: map[change.ID]int{}, prev: start,
	}
}

func (m *ackModel) table() change.Source {
	return change.Source{DB: "app", Schema: "public", Table: "t" + strconv.Itoa(rapid.IntRange(0, 3).Draw(m.t, "table"))}
}

func (m *ackModel) begin(pseudo bool) *modelTx {
	commit := m.lastEnd + lsn.LSN(rapid.Uint64Range(0, 3).Draw(m.t, "gap"))
	end := commit + lsn.LSN(rapid.Uint64Range(1, 3).Draw(m.t, "len"))
	if pseudo {
		commit, end = m.start, m.start
	}
	h, err := m.tracker.Begin(context.Background(), commit, end, epoch)
	require.NoError(m.t, err)
	m.lastEnd = end
	tx := &modelTx{commit: commit, end: end, handle: h}
	m.txs = append(m.txs, tx)
	return tx
}

func (m *ackModel) route(i int, attached bool) {
	tx := m.txs[i]
	ev := change.Event{Source: m.table(), Op: change.OpCreate, Key: change.Row{{Name: "id", Value: []byte("1")}}}
	var (
		n   int
		err error
	)
	switch {
	case attached:
		m.nextN++
		ev.ID, ev.Op = change.SnapshotID(tx.commit, 1, m.nextN), change.OpRead
	default:
		ev.ID = change.StreamID(tx.commit, tx.records)
		tx.records++
	}
	if attached && tx.groupOpen {
		n, err = m.router.RouteToGroup(context.Background(), tx.group, pipeline.NewEventRecord(&ev))
	} else {
		n, err = m.router.Route(context.Background(), tx.handle, pipeline.NewEventRecord(&ev))
	}
	require.NoError(m.t, err)
	m.byID[ev.ID] = i
	tx.pending += n
}

func (m *ackModel) open() []int {
	var out []int
	for i, tx := range m.txs {
		if !tx.sealed {
			out = append(out, i)
		}
	}
	return out
}

func (m *ackModel) stepStreamTx() {
	tx := m.begin(false)
	for range rapid.IntRange(0, 3).Draw(m.t, "records") {
		m.route(len(m.txs)-1, false)
	}
	if rapid.IntRange(0, 3).Draw(m.t, "keep_open") > 0 {
		require.NoError(m.t, m.tracker.Seal(tx.handle))
		tx.sealed = true
	}
}

func (m *ackModel) stepAttach() {
	open := m.open()
	if len(open) == 0 {
		return
	}
	i := rapid.SampledFrom(open).Draw(m.t, "attach_to")
	tx := m.txs[i]
	if !tx.groupOpen && rapid.Bool().Draw(m.t, "new_group") {
		g, err := m.tracker.NewGroup(tx.handle, epoch)
		require.NoError(m.t, err)
		tx.group, tx.groupOpen = g, true
	}
	for range rapid.IntRange(1, 3).Draw(m.t, "attached") {
		m.route(i, true)
	}
}

func (m *ackModel) stepSeal() {
	open := m.open()
	if len(open) == 0 {
		return
	}
	tx := m.txs[rapid.SampledFrom(open).Draw(m.t, "seal")]
	if tx.groupOpen {
		require.NoError(m.t, m.tracker.SealGroup(tx.group))
		tx.groupOpen = false
		if rapid.Bool().Draw(m.t, "seal_group_only") {
			return
		}
	}
	require.NoError(m.t, m.tracker.Seal(tx.handle))
	tx.sealed = true
}

func (m *ackModel) stepAck() {
	s := m.sinks[rapid.IntRange(0, len(m.sinks)-1).Draw(m.t, "ack_sink")]
	pending := s.Pending()
	if len(pending) == 0 {
		return
	}
	i := rapid.IntRange(0, len(pending)-1).Draw(m.t, "ack_index")
	require.NoError(m.t, s.AckAt(i))
	m.txs[m.byID[pending[i].ID]].pending--
}

func (m *ackModel) check() {
	c := m.tracker.Confirmed()
	require.GreaterOrEqual(m.t, c, m.prev, "confirmed LSN is monotonic")
	m.prev = c
	want, inflight, anyPending := m.start, len(m.txs), false
	prefix := true
	for _, tx := range m.txs {
		anyPending = anyPending || tx.pending > 0
		if prefix && tx.complete() {
			want = tx.end
			inflight--
			continue
		}
		if prefix {
			require.LessOrEqual(m.t, c, tx.commit, "confirmed beyond the oldest unacknowledged commit")
		}
		prefix = false
	}
	require.Equal(m.t, want, c, "confirmed = end of the contiguous acknowledged prefix")
	require.Equal(m.t, inflight, m.tracker.InFlight())
	_, ok := m.tracker.OldestUnacked()
	require.Equal(m.t, anyPending, ok)
	for _, tx := range m.txs {
		require.Equal(m.t, tx.complete(), isClosed(m.tracker.Done(tx.handle)))
	}
}

func (m *ackModel) drain() {
	for _, tx := range m.txs {
		if tx.groupOpen {
			require.NoError(m.t, m.tracker.SealGroup(tx.group))
			tx.groupOpen = false
		}
		if !tx.sealed {
			require.NoError(m.t, m.tracker.Seal(tx.handle))
			tx.sealed = true
		}
		m.check()
	}
	for {
		var live []*fakesink.Sink
		for _, s := range m.sinks {
			if len(s.Pending()) > 0 {
				live = append(live, s)
			}
		}
		if len(live) == 0 {
			break
		}
		s := live[rapid.IntRange(0, len(live)-1).Draw(m.t, "drain_sink")]
		pending := s.Pending()
		i := rapid.IntRange(0, len(pending)-1).Draw(m.t, "drain_index")
		require.NoError(m.t, s.AckAt(i))
		m.txs[m.byID[pending[i].ID]].pending--
		m.check()
	}
}

func TestAckTracker_Property_ConfirmedLSNSafety(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		m := newAckModel(t)
		if rapid.Bool().Draw(t, "initial_snapshot") {
			m.begin(true)
		}
		m.check()
		for range rapid.IntRange(0, 60).Draw(t, "steps") {
			switch rapid.IntRange(0, 3).Draw(t, "op") {
			case 0:
				m.stepStreamTx()
			case 1:
				m.stepAttach()
			case 2:
				m.stepSeal()
			default:
				m.stepAck()
			}
			m.check()
		}
		m.drain()
		want := m.start
		if len(m.txs) > 0 {
			want = m.txs[len(m.txs)-1].end
		}
		require.Equal(t, want, m.tracker.Confirmed())
		require.Equal(t, 0, m.tracker.InFlight())
		_, ok := m.tracker.OldestUnacked()
		require.False(t, ok)
	})
}
