// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
)

func src(schema, table string) change.Source {
	return change.Source{DB: "app", Schema: schema, Table: table}
}

// eventRec returns an update of the transaction committed at 110.
func eventRec(s change.Source, k uint64, key string) pipeline.Record {
	ev := change.Event{ID: change.StreamID(110, k), Source: s, Op: change.OpUpdate}
	if key != "" {
		ev.Key = change.Row{{Name: "id", Value: []byte(key)}}
	}
	return pipeline.NewEventRecord(&ev)
}

// routerFixture builds sinks "all" (*), "orders" (public.orders), "public"
// (public.*) and "none" ([]).
func routerFixture(t *testing.T, opts ...fakesink.Option) (*AckTracker, *Router, map[string]*fakesink.Sink) {
	t.Helper()
	tr := newTracker(t, 100)
	sinks := map[string]*fakesink.Sink{}
	specs := []struct {
		name   string
		tables []string
	}{
		{"all", []string{"*"}},
		{"orders", []string{"public.orders"}},
		{"public", []string{"public.*"}},
		{"none", []string{}},
	}
	routes := make([]SinkRoute, 0, len(specs))
	for _, rt := range specs {
		s := fakesink.New(rt.name, opts...)
		sinks[rt.name] = s
		routes = append(routes, SinkRoute{Sink: s, Tables: rt.tables})
	}
	r, err := NewRouter(tr, routes)
	require.NoError(t, err)
	return tr, r, sinks
}

func TestNewRouter_Invalid(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 0)
	cases := []struct {
		name    string
		tracker *AckTracker
		routes  []SinkRoute
		want    error
	}{
		{name: "nil tracker", routes: nil, want: ErrInvalidOption},
		{name: "nil sink", tracker: tr, routes: []SinkRoute{{}}, want: ErrInvalidOption},
		{name: "duplicate name", tracker: tr, want: ErrInvalidOption, routes: []SinkRoute{
			{Sink: fakesink.New("a"), Tables: []string{"*"}}, {Sink: fakesink.New("a")},
		}},
		{name: "bad glob", tracker: tr, want: pipeline.ErrBadTablePattern, routes: []SinkRoute{
			{Sink: fakesink.New("a"), Tables: []string{"public.["}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewRouter(tc.tracker, tc.routes)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestRouter_SinkNames(t *testing.T) {
	t.Parallel()
	_, r, _ := routerFixture(t)
	cases := []struct {
		name  string
		table string
		want  []string
	}{
		{name: "orders", table: "public.orders", want: []string{"all", "orders", "public"}},
		{name: "public table", table: "public.users", want: []string{"all", "public"}},
		{name: "other schema", table: "sales.orders", want: []string{"all"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, r.SinkNames(tc.table))
			assert.Equal(t, tc.want, r.SinkNames(tc.table), "cached")
		})
	}
}

func TestRouter_Route_ByKind(t *testing.T) {
	t.Parallel()
	sc := change.SchemaChange{ID: change.SchemaID(110, 0), Source: src("public", "orders"), SchemaVersion: 2}
	msg := change.Message{ID: change.MessageID(110, 0), Prefix: "app"}
	cases := []struct {
		name string
		rec  pipeline.Record
		want []string
	}{
		{name: "event", rec: eventRec(src("public", "orders"), 0, "1"), want: []string{"all", "orders", "public"}},
		{name: "event of other schema", rec: eventRec(src("sales", "x"), 0, "1"), want: []string{"all"}},
		{
			name: "schema change follows its table", rec: pipeline.NewSchemaChangeRecord(&sc),
			want: []string{"all", "orders", "public"},
		},
		{
			name: "message goes to sinks with tables", rec: pipeline.NewMessageRecord(&msg),
			want: []string{"all", "orders", "public"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr, r, sinks := routerFixture(t)
			tx := begin(t, tr, 110, 120)
			n, err := r.Route(context.Background(), tx, tc.rec)
			require.NoError(t, err)
			assert.Len(t, tc.want, n)
			var got []string
			for _, name := range []string{"all", "orders", "public", "none"} {
				if recs := sinks[name].Received(); len(recs) > 0 {
					require.Len(t, recs, 1)
					assert.Equal(t, tc.rec.Payload, recs[0].Payload)
					got = append(got, name)
				}
			}
			assert.Equal(t, tc.want, got)
			require.NoError(t, tr.Seal(tx))
			for _, name := range got {
				assert.Equal(t, lsn.LSN(100), tr.Confirmed())
				require.NoError(t, sinks[name].AckAll())
			}
			assert.Equal(t, lsn.LSN(120), tr.Confirmed(), "each sink acknowledged its own delivery")
		})
	}
}

func TestRouter_Dispatch(t *testing.T) {
	t.Parallel()
	tr, r, sinks := routerFixture(t)
	b1 := pipeline.TxBatch{CommitLSN: 110, EndLSN: 120, Records: []pipeline.Record{
		eventRec(src("public", "orders"), 0, "1"),
		eventRec(src("public", "orders"), 1, "2"),
	}}
	unrouted := pipeline.TxBatch{CommitLSN: 130, EndLSN: 140}
	tx1, err := r.Dispatch(context.Background(), &b1, epoch)
	require.NoError(t, err)
	tx2, err := r.Dispatch(context.Background(), &unrouted, epoch)
	require.NoError(t, err)
	assert.True(t, isClosed(tr.Done(tx2)))
	assert.False(t, isClosed(tr.Done(tx1)))
	assert.Equal(t, lsn.LSN(100), tr.Confirmed())
	for _, s := range sinks {
		require.NoError(t, s.AckAll())
	}
	assert.Equal(t, lsn.LSN(140), tr.Confirmed())
	assert.Len(t, sinks["orders"].Received(), 2)

	_, err = r.Dispatch(context.Background(), &pipeline.TxBatch{CommitLSN: 1, EndLSN: 2}, epoch)
	require.ErrorIs(t, err, ErrOutOfOrder)
}

func TestRouter_Route_Errors(t *testing.T) {
	t.Parallel()
	t.Run("send error", func(t *testing.T) {
		t.Parallel()
		tr, r, sinks := routerFixture(t)
		sinks["public"].FailSends(assert.AnError)
		_, err := r.Dispatch(context.Background(), &pipeline.TxBatch{CommitLSN: 110, EndLSN: 120, Records: []pipeline.Record{
			eventRec(src("public", "orders"), 0, "1"),
		}}, epoch)
		require.ErrorIs(t, err, assert.AnError)
		assert.Contains(t, err.Error(), `sink "public"`)
		assert.Equal(t, lsn.LSN(100), tr.Confirmed())
	})
	t.Run("sealed transaction", func(t *testing.T) {
		t.Parallel()
		tr, r, _ := routerFixture(t)
		tx := begin(t, tr, 110, 120)
		require.NoError(t, tr.Seal(tx))
		_, err := r.Route(context.Background(), tx, eventRec(src("public", "orders"), 0, "1"))
		require.ErrorIs(t, err, ErrUnknownTx)
		n, err := r.Route(context.Background(), tx, eventRec(src("x", "y"), 0, "1"))
		require.ErrorIs(t, err, ErrUnknownTx, "even with one target")
		assert.Zero(t, n)
	})
	t.Run("backpressure until ctx ends", func(t *testing.T) {
		t.Parallel()
		tr, r, sinks := routerFixture(t, fakesink.WithCapacity(1))
		tx := begin(t, tr, 110, 120)
		_, err := r.Route(context.Background(), tx, eventRec(src("sales", "x"), 0, "1"))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = r.Route(ctx, tx, eventRec(src("sales", "x"), 1, "1"))
		require.ErrorIs(t, err, context.Canceled)
		assert.Len(t, sinks["all"].Received(), 1)
	})
}

func TestRouter_RouteToGroup(t *testing.T) {
	t.Parallel()
	tr, r, sinks := routerFixture(t)
	high := begin(t, tr, 110, 120)
	g, err := tr.NewGroup(high, epoch)
	require.NoError(t, err)
	ev := change.Event{
		ID: change.SnapshotID(110, 1, 0), Source: src("public", "orders"), Op: change.OpRead,
		Key: change.Row{{Name: "id", Value: []byte("1")}},
	}
	n, err := r.RouteToGroup(context.Background(), g, pipeline.NewEventRecord(&ev))
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	require.NoError(t, tr.SealGroup(g))
	require.NoError(t, tr.Seal(high))
	done := tr.GroupDone(g)
	for _, name := range []string{"all", "orders"} {
		require.NoError(t, sinks[name].AckAll())
	}
	assert.False(t, isClosed(done))
	require.NoError(t, sinks["public"].AckAll())
	assert.True(t, isClosed(done))
	assert.Equal(t, lsn.LSN(120), tr.Confirmed())
	tok := sinks["all"].Received()[0].Ack.Token()
	assert.NotZero(t, tok.Group)
}
