// SPDX-License-Identifier: Apache-2.0

package fakechunk_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakechunk"
)

var (
	_ pipeline.ChunkReader  = (*fakechunk.Reader)(nil)
	_ pipeline.ChunkSession = (*fakechunk.Session)(nil)
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func seeded(ids ...int64) *fakechunk.Reader {
	r := fakechunk.New()
	for _, id := range ids {
		r.Put("public.t", id, `"v"`)
	}
	return r
}

func req(after string, limit int) pipeline.ChunkRequest {
	r := pipeline.ChunkRequest{Schema: "public", Table: "t", Limit: limit}
	if after != "" {
		r.After = json.RawMessage(after)
	}
	return r
}

func rowIDs(t *testing.T, c pipeline.Chunk) []string {
	t.Helper()
	ids := make([]string, 0, len(c.Rows))
	for _, row := range c.Rows {
		require.Len(t, row.Key, 1)
		ids = append(ids, string(row.Key[0].Value))
	}
	return ids
}

func TestRow_Shape(t *testing.T) {
	t.Parallel()
	row := fakechunk.Row(7, `{"a":1}`)
	require.Len(t, row.Key, 1)
	assert.Equal(t, "id", row.Key[0].Name)
	assert.Equal(t, "7", string(row.Key[0].Value))
	require.Len(t, row.After, 2)
	assert.Equal(t, "id", row.After[0].Name)
	assert.Equal(t, "7", string(row.After[0].Value))
	assert.Equal(t, "v", row.After[1].Name)
	assert.JSONEq(t, `{"a":1}`, string(row.After[1].Value))
}

func TestReader_ReadChunk_Pagination(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		ids   []int64
		limit int
		pages []struct {
			ids  []string
			last string
			done bool
		}
	}{
		{
			name: "remainder in last page", ids: []int64{5, 1, 3, 2, 4}, limit: 2,
			pages: []struct {
				ids  []string
				last string
				done bool
			}{
				{[]string{"1", "2"}, "[2]", false},
				{[]string{"3", "4"}, "[4]", false},
				{[]string{"5"}, "[5]", true},
			},
		},
		{
			name: "exact multiple is done on the last full page", ids: []int64{1, 2, 3, 4}, limit: 2,
			pages: []struct {
				ids  []string
				last string
				done bool
			}{
				{[]string{"1", "2"}, "[2]", false},
				{[]string{"3", "4"}, "[4]", true},
				{[]string{}, "[4]", true},
			},
		},
		{
			name: "limit above row count", ids: []int64{-3, 10}, limit: 100,
			pages: []struct {
				ids  []string
				last string
				done bool
			}{
				{[]string{"-3", "10"}, "[10]", true},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := seeded(tc.ids...)
			var after string
			for i, p := range tc.pages {
				c, err := r.ReadChunk(context.Background(), req(after, tc.limit))
				require.NoError(t, err, "page %d", i)
				assert.Equal(t, p.ids, rowIDs(t, c), "page %d", i)
				assert.Equal(t, p.last, string(c.Last), "page %d", i)
				assert.Equal(t, p.done, c.Done, "page %d", i)
				after = string(c.Last)
			}
			assert.Len(t, r.Reads(), len(tc.pages))
		})
	}
}

func TestReader_ReadChunk_RowContent(t *testing.T) {
	t.Parallel()
	r := fakechunk.New()
	r.Put("public.t", 1, `"a"`)
	r.Put("public.t", 1, `"b"`)
	c, err := r.ReadChunk(context.Background(), req("", 10))
	require.NoError(t, err)
	assert.Equal(t, []pipeline.ChunkRow{fakechunk.Row(1, `"b"`)}, c.Rows)
}

func TestReader_ReadChunk_EmptyTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(r *fakechunk.Reader)
		after string
	}{
		{"unknown table, no position", func(*fakechunk.Reader) {}, ""},
		{"unknown table, with position", func(*fakechunk.Reader) {}, "[42]"},
		{"deleted rows", func(r *fakechunk.Reader) {
			r.Put("public.t", 1, `"v"`)
			r.Delete("public.t", 1)
		}, "[3]"},
		{"position past the last row", func(r *fakechunk.Reader) { r.Put("public.t", 1, `"v"`) }, "[1]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := fakechunk.New()
			tc.setup(r)
			rq := req(tc.after, 5)
			c, err := r.ReadChunk(context.Background(), rq)
			require.NoError(t, err)
			assert.Empty(t, c.Rows)
			assert.True(t, c.Done)
			assert.Equal(t, rq.After, c.Last)
		})
	}
}

func TestReader_ReadChunk_BadPosition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		after string
	}{
		{"not json", "abc"},
		{"object", `{"id":1}`},
		{"string", `"1"`},
		{"empty array", "[]"},
		{"two keys", "[1,2]"},
		{"null", "null"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := seeded(1)
			c, err := r.ReadChunk(context.Background(), req(tc.after, 5))
			require.ErrorIs(t, err, fakechunk.ErrBadPosition)
			assert.Contains(t, err.Error(), tc.after)
			assert.Equal(t, pipeline.Chunk{}, c)
			assert.Len(t, r.Reads(), 1, "the failed read is recorded")
		})
	}
}

func TestReader_ReadChunk_Snapshot(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	xip := []uint64{5, 6}
	committed := []uint64{6}
	r.SetSnapshot(pipeline.TxSnapshot{XMin: 4, XMax: 9, XIP: xip}, committed)
	xip[0], committed[0] = 99, 99

	c, err := r.ReadChunk(context.Background(), req("", 5))
	require.NoError(t, err)
	assert.Equal(t, pipeline.TxSnapshot{XMin: 4, XMax: 9, XIP: []uint64{5, 6}}, c.Snapshot)
	assert.Equal(t, []uint64{6}, c.CommittedXIP)

	c.Snapshot.XIP[0], c.CommittedXIP[0] = 77, 77
	c2, err := r.ReadChunk(context.Background(), req("", 5))
	require.NoError(t, err)
	assert.Equal(t, []uint64{5, 6}, c2.Snapshot.XIP)
	assert.Equal(t, []uint64{6}, c2.CommittedXIP)
}

func TestReader_ReadChunk_NoSnapshotByDefault(t *testing.T) {
	t.Parallel()
	c, err := seeded(1).ReadChunk(context.Background(), req("", 5))
	require.NoError(t, err)
	assert.Zero(t, c.Snapshot.XMin)
	assert.Zero(t, c.Snapshot.XMax)
	assert.Empty(t, c.Snapshot.XIP)
	assert.Empty(t, c.CommittedXIP)
}

func TestReader_ReadChunk_CanceledContext(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	_, err := r.ReadChunk(canceled(), req("", 5))
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, r.Reads())
}

func TestReader_FailRead_ReadChunk(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	hooked := 0
	r.OnRead(func(int) { hooked++ })
	r.FailRead(assert.AnError)

	c, err := r.ReadChunk(context.Background(), req("", 5))
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, pipeline.Chunk{}, c)
	assert.Zero(t, hooked, "hook is not called on error")
	assert.Len(t, r.Reads(), 1)

	r.FailRead(nil)
	c, err = r.ReadChunk(context.Background(), req("", 5))
	require.NoError(t, err)
	assert.Len(t, c.Rows, 1)
	assert.Equal(t, 1, hooked)
}

func TestReader_OnRead_CountsEveryRead(t *testing.T) {
	t.Parallel()
	r := seeded(1, 2, 3)
	var got []int
	r.OnRead(func(n int) { got = append(got, n) })

	_, err := r.ReadChunk(context.Background(), req("", 1))
	require.NoError(t, err)
	sess, err := r.OpenExported(context.Background(), "snap")
	require.NoError(t, err)
	_, err = sess.ReadChunk(context.Background(), req("[1]", 1))
	require.NoError(t, err)
	_, err = r.ReadChunk(context.Background(), req("[2]", 1))
	require.NoError(t, err)

	assert.Equal(t, []int{1, 2, 3}, got)
}

func TestReader_OnRead_CanMutateWithoutDeadlock(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	r.OnRead(func(n int) {
		if n == 1 {
			r.Put("public.t", 2, `"late"`)
		}
	})
	c, err := r.ReadChunk(context.Background(), req("", 10))
	require.NoError(t, err)
	assert.Equal(t, []string{"1"}, rowIDs(t, c), "the hook runs after the rows were read")

	c, err = r.ReadChunk(context.Background(), req("", 10))
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, rowIDs(t, c))
}

func TestReader_Reads_RecordsRequestsInOrder(t *testing.T) {
	t.Parallel()
	r := seeded(1, 2)
	sess, err := r.OpenExported(context.Background(), "snap")
	require.NoError(t, err)
	first, second := req("", 1), req("[1]", 1)
	_, err = r.ReadChunk(context.Background(), first)
	require.NoError(t, err)
	_, err = sess.ReadChunk(context.Background(), second)
	require.NoError(t, err)

	got := r.Reads()
	assert.Equal(t, []pipeline.ChunkRequest{first, second}, got)
	got[0].Table = "mutated"
	assert.Equal(t, "t", r.Reads()[0].Table, "Reads returns a copy")
}

func TestReader_OpenExported_SeesRowsAsOfOpen(t *testing.T) {
	t.Parallel()
	r := seeded(1, 2)
	sess, err := r.OpenExported(context.Background(), "snap-1")
	require.NoError(t, err)
	r.Put("public.t", 3, `"new"`)
	r.Put("public.t", 1, `"changed"`)
	r.Delete("public.t", 2)
	r.Put("public.other", 1, `"x"`)

	c, err := sess.ReadChunk(context.Background(), req("", 10))
	require.NoError(t, err)
	assert.Equal(t, []pipeline.ChunkRow{fakechunk.Row(1, `"v"`), fakechunk.Row(2, `"v"`)}, c.Rows)
	assert.True(t, c.Done)
	assert.Equal(t, "[2]", string(c.Last))
	assert.Empty(t, c.CommittedXIP, "sessions do not report a snapshot")

	live, err := r.ReadChunk(context.Background(), req("", 10))
	require.NoError(t, err)
	assert.Equal(t, []pipeline.ChunkRow{fakechunk.Row(1, `"changed"`), fakechunk.Row(3, `"new"`)}, live.Rows)

	sess2, err := r.OpenExported(context.Background(), "snap-2")
	require.NoError(t, err)
	c2, err := sess2.ReadChunk(context.Background(), req("", 10))
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "3"}, rowIDs(t, c2))
	again, err := sess.ReadChunk(context.Background(), req("", 10))
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, rowIDs(t, again))
}

func TestReader_OpenExported_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(r *fakechunk.Reader)
		ctx   context.Context //nolint:containedctx // table input
		want  error
	}{
		{"canceled context", func(*fakechunk.Reader) {}, canceled(), context.Canceled},
		{"injected failure", func(r *fakechunk.Reader) { r.FailOpen(assert.AnError) }, context.Background(), assert.AnError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := seeded(1)
			tc.setup(r)
			sess, err := r.OpenExported(tc.ctx, "snap")
			require.ErrorIs(t, err, tc.want)
			assert.Nil(t, sess)
			opened, closed := r.Sessions()
			assert.Empty(t, opened)
			assert.Zero(t, closed)
		})
	}
}

func TestReader_FailOpen_NilRestores(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	r.FailOpen(assert.AnError)
	_, err := r.OpenExported(context.Background(), "a")
	require.ErrorIs(t, err, assert.AnError)
	r.FailOpen(nil)
	_, err = r.OpenExported(context.Background(), "b")
	require.NoError(t, err)
	opened, _ := r.Sessions()
	assert.Equal(t, []string{"b"}, opened)
}

func TestReader_Sessions_TracksOpenAndClose(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	s1, err := r.OpenExported(context.Background(), "a")
	require.NoError(t, err)
	_, err = r.OpenExported(context.Background(), "b")
	require.NoError(t, err)
	require.NoError(t, s1.Close(context.Background()))

	opened, closed := r.Sessions()
	assert.Equal(t, []string{"a", "b"}, opened)
	assert.Equal(t, 1, closed)
	opened[0] = "mutated"
	again, _ := r.Sessions()
	assert.Equal(t, []string{"a", "b"}, again, "Sessions returns a copy")
}

func TestSession_ReadChunk_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(r *fakechunk.Reader, s pipeline.ChunkSession)
		ctx   context.Context //nolint:containedctx // table input
		req   pipeline.ChunkRequest
		want  error
	}{
		{"canceled context", func(*fakechunk.Reader, pipeline.ChunkSession) {}, canceled(), req("", 5), context.Canceled},
		{"closed session", func(_ *fakechunk.Reader, s pipeline.ChunkSession) {
			require.NoError(t, s.Close(context.Background()))
		}, context.Background(), req("", 5), fakechunk.ErrClosed},
		{
			"injected failure",
			func(r *fakechunk.Reader, _ pipeline.ChunkSession) { r.FailRead(assert.AnError) },
			context.Background(), req("", 5), assert.AnError,
		},
		{
			"bad position",
			func(*fakechunk.Reader, pipeline.ChunkSession) {},
			context.Background(), req("[1,2]", 5), fakechunk.ErrBadPosition,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := seeded(1)
			hooked := 0
			r.OnRead(func(int) { hooked++ })
			sess, err := r.OpenExported(context.Background(), "snap")
			require.NoError(t, err)
			tc.setup(r, sess)
			c, err := sess.ReadChunk(tc.ctx, tc.req)
			require.ErrorIs(t, err, tc.want)
			assert.Equal(t, pipeline.Chunk{}, c)
			assert.Zero(t, hooked)
		})
	}
}

func TestSession_ReadChunk_ClosedSessionIsNotRecorded(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	sess, err := r.OpenExported(context.Background(), "snap")
	require.NoError(t, err)
	require.NoError(t, sess.Close(context.Background()))
	_, err = sess.ReadChunk(context.Background(), req("", 5))
	require.ErrorIs(t, err, fakechunk.ErrClosed)
	assert.Empty(t, r.Reads())
}

func TestSession_Close_Twice(t *testing.T) {
	t.Parallel()
	r := seeded(1)
	sess, err := r.OpenExported(context.Background(), "snap")
	require.NoError(t, err)
	require.NoError(t, sess.Close(context.Background()))
	require.ErrorIs(t, sess.Close(context.Background()), fakechunk.ErrClosed)
	_, closed := r.Sessions()
	assert.Equal(t, 1, closed)
}

func TestReader_Delete_UnknownTable(t *testing.T) {
	t.Parallel()
	r := fakechunk.New()
	r.Delete("public.none", 1)
	c, err := r.ReadChunk(context.Background(), pipeline.ChunkRequest{Schema: "public", Table: "none", Limit: 1})
	require.NoError(t, err)
	assert.True(t, c.Done)
	assert.Empty(t, c.Rows)
	assert.Nil(t, c.Last)
}
