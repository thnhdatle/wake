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

// genTx draws one transaction's records in seq order: keyed and keyless
// events, `t` events, schema changes (before the event they precede) and
// forwarded messages.
func genTx(t *rapid.T, commit lsn.LSN) []pipeline.Record {
	var recs []pipeline.Record
	var k, msgs uint64
	for range rapid.IntRange(0, 6).Draw(t, "records") {
		s := change.Source{DB: "app", Schema: "public", Table: "t" + strconv.Itoa(rapid.IntRange(0, 2).Draw(t, "table"))}
		kind := rapid.IntRange(0, 9).Draw(t, "kind")
		if kind == 0 {
			// A schema change precedes the data event that takes index k.
			sc := change.SchemaChange{ID: change.SchemaID(commit, k), Source: s}
			recs = append(recs, pipeline.NewSchemaChangeRecord(&sc))
		}
		switch kind {
		case 1:
			m := change.Message{ID: change.MessageID(commit, msgs), Transactional: true}
			msgs++
			recs = append(recs, pipeline.NewMessageRecord(&m))
			continue // messages do not consume index_in_tx
		case 2:
			ev := change.Event{ID: change.StreamID(commit, k), Source: s, Op: change.OpTruncate}
			recs = append(recs, pipeline.NewEventRecord(&ev))
		case 3:
			ev := change.Event{ID: change.StreamID(commit, k), Source: s, Op: change.OpCreate}
			recs = append(recs, pipeline.NewEventRecord(&ev)) // keyless: partitioned by id
		default:
			key := strconv.Itoa(rapid.IntRange(0, 4).Draw(t, "key"))
			ev := change.Event{
				ID: change.StreamID(commit, k), Source: s, Op: change.OpUpdate,
				Key: change.Row{{Name: "id", Value: []byte(key)}},
			}
			recs = append(recs, pipeline.NewEventRecord(&ev))
		}
		k++
	}
	return recs
}

func partitionKey(r *pipeline.Record) string { return r.Source.QualifiedName() + "|" + string(r.Key) }

// routeWorkload builds random sinks, routes random transactions through a
// Router and returns, per sink, the records it received and the ids it
// should have received in routing order.
func routeWorkload(t *rapid.T) (received [][]pipeline.Record, expected [][]change.ID) {
	tr, err := NewAckTracker(1)
	require.NoError(t, err)
	nSinks := rapid.IntRange(1, 3).Draw(t, "sinks")
	sinks := make([]*fakesink.Sink, nSinks)
	filters := make([]pipeline.TableFilter, nSinks)
	routes := make([]SinkRoute, nSinks)
	for i := range nSinks {
		globs := rapid.SliceOfDistinct(rapid.SampledFrom([]string{"*", "public.t0", "public.t1", "public.t*"}),
			func(s string) string { return s }).Draw(t, "globs")
		sinks[i] = fakesink.New("s"+strconv.Itoa(i), fakesink.WithAutoAck())
		routes[i] = SinkRoute{Sink: sinks[i], Tables: globs}
		filters[i], err = pipeline.NewTableFilter(globs)
		require.NoError(t, err)
	}
	r, err := NewRouter(tr, routes)
	require.NoError(t, err)

	expected = make([][]change.ID, nSinks)
	for s := range expected {
		expected[s] = []change.ID{}
	}
	commit := lsn.LSN(10)
	for range rapid.IntRange(1, 8).Draw(t, "txs") {
		b := pipeline.TxBatch{CommitLSN: commit, EndLSN: commit + 1, Records: genTx(t, commit)}
		for i := range b.Records {
			for s := range nSinks {
				if wantsRecord(filters[s], &b.Records[i]) {
					expected[s] = append(expected[s], b.Records[i].ID)
				}
			}
		}
		_, err := r.Dispatch(context.Background(), &b, epoch)
		require.NoError(t, err)
		commit += 10
	}
	require.Equal(t, commit-9, tr.Confirmed(), "auto-acknowledged sinks confirm everything")
	received = make([][]pipeline.Record, nSinks)
	for s := range nSinks {
		received[s] = sinks[s].Received()
	}
	return received, expected
}

// wantsRecord is the routing rule stated independently of the Router.
func wantsRecord(f pipeline.TableFilter, rec *pipeline.Record) bool {
	if rec.Kind == pipeline.RecordKindMessage {
		return !f.IsEmpty()
	}
	return f.Match(rec.Table())
}

// TestRouter_Property_PerKeyOrder is the router per-key order
// property: for random
// workloads routed to sinks with random globs and then scheduled into random
// webhook lanes with random completion order, every sink receives its
// records in routing order, records of one (table, key) reach their lane in
// seq order, and barriers are ordered relative to everything.
func TestRouter_Property_PerKeyOrder(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		received, expected := routeWorkload(t)
		for s := range received {
			ids := make([]change.ID, 0, len(received[s]))
			for i := range received[s] {
				ids = append(ids, received[s][i].ID)
			}
			require.Equal(t, expected[s], ids, "sink %d receives its records in routing order", s)
			newLaneCheck(t, received[s]).run()
		}
	})
}

// laneCheck pushes a sink's records through a LaneScheduler with random
// lanes, limits and completion order and checks per-key seq order and
// barrier isolation.
type laneCheck struct {
	t          *rapid.T
	recs       []pipeline.Record
	lanes      *pipeline.LaneScheduler
	limits     pipeline.BatchLimits
	pos        map[change.ID]int
	lastSeq    map[string]change.Seq
	done       []bool
	pushed     int
	completed  int
	inflight   []pipeline.LaneBatch
	barrierOut bool
}

func newLaneCheck(t *rapid.T, recs []pipeline.Record) *laneCheck {
	lanes, err := pipeline.NewLaneScheduler(rapid.IntRange(1, 4).Draw(t, "lanes"), rapid.IntRange(1, 8).Draw(t, "cap"))
	require.NoError(t, err)
	c := &laneCheck{
		t: t, recs: recs, lanes: lanes,
		limits:  pipeline.BatchLimits{MaxEvents: rapid.IntRange(0, 3).Draw(t, "max_events")},
		pos:     map[change.ID]int{},
		lastSeq: map[string]change.Seq{},
		done:    make([]bool, len(recs)),
	}
	for i := range recs {
		c.pos[recs[i].ID] = i
	}
	return c
}

func (c *laneCheck) run() {
	for c.completed < len(c.recs) {
		switch op := rapid.IntRange(0, 2).Draw(c.t, "lane_op"); {
		case op == 0 && c.pushed < len(c.recs):
			if c.lanes.Push(c.recs[c.pushed]) == nil {
				c.pushed++
			}
		case op == 1:
			if b, ok := c.lanes.Next(c.limits); ok {
				c.dispatch(b)
			}
		case len(c.inflight) > 0:
			c.complete(rapid.IntRange(0, len(c.inflight)-1).Draw(c.t, "lane_complete"))
		}
	}
}

func (c *laneCheck) dispatch(b pipeline.LaneBatch) {
	require.False(c.t, c.barrierOut, "dispatch while a barrier is in flight")
	for _, rec := range b.Records {
		p := c.pos[rec.ID]
		for e := range p {
			if rec.Barrier || c.recs[e].Barrier {
				require.True(c.t, c.done[e], "record %d dispatched before %d completed", p, e)
			}
		}
		if rec.Barrier {
			c.barrierOut = true
			continue
		}
		pk := partitionKey(&rec)
		if last, ok := c.lastSeq[pk]; ok {
			require.True(c.t, last.Less(rec.ID.Seq()), "key %s out of seq order", pk)
		}
		c.lastSeq[pk] = rec.ID.Seq()
	}
	c.inflight = append(c.inflight, b)
}

func (c *laneCheck) complete(i int) {
	b := c.inflight[i]
	require.NoError(c.t, c.lanes.Complete(b))
	for _, rec := range b.Records {
		c.done[c.pos[rec.ID]] = true
		c.completed++
	}
	c.barrierOut = c.barrierOut && b.Lane != pipeline.BarrierLane
	c.inflight = append(c.inflight[:i], c.inflight[i+1:]...)
}
