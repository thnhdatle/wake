// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/watermark"
)

// feeder drives a Merger the way the stream does.
type feeder struct {
	t testing.TB
	m *watermark.Merger
}

func newFeeder(tb testing.TB) *feeder {
	tb.Helper()
	return &feeder{t: tb, m: watermark.NewMerger()}
}

// tx feeds a committed transaction that touched the given model keys.
func (f *feeder) tx(xid uint32, keys ...int) {
	f.t.Helper()
	require.NoError(f.t, f.m.Begin(xid))
	for _, k := range keys {
		require.NoError(f.t, f.m.Touch(keyBytes(k)))
	}
	require.NoError(f.t, f.m.Commit())
}

// move feeds a key-changing update from one key to another.
func (f *feeder) move(xid uint32, from, to int) {
	f.t.Helper()
	require.NoError(f.t, f.m.Begin(xid))
	require.NoError(f.t, f.m.Touch(keyBytes(to), keyBytes(from)))
	require.NoError(f.t, f.m.Commit())
}

func (f *feeder) truncate(xid uint32) {
	f.t.Helper()
	require.NoError(f.t, f.m.Begin(xid))
	require.NoError(f.t, f.m.TouchAll())
	require.NoError(f.t, f.m.Commit())
}

func (f *feeder) low(xid uint32, chunk uint64) {
	f.t.Helper()
	p := lowPayload(chunk)
	require.NoError(f.t, f.m.Begin(xid))
	require.NoError(f.t, f.m.Low(&p))
	require.NoError(f.t, f.m.Commit())
}

func (f *feeder) rows(chunk uint64, keys ...int) {
	f.t.Helper()
	b := make([][]byte, len(keys))
	for i, k := range keys {
		b[i] = keyBytes(k)
	}
	f.m.Rows(chunk, b)
}

func (f *feeder) high(xid uint32, chunk uint64, snap string, committed ...uint64) watermark.Outcome {
	f.t.Helper()
	s, err := watermark.ParseSnapshot(snap)
	require.NoError(f.t, err)
	p := highPayload(chunk, s, committed...)
	require.NoError(f.t, f.m.Begin(xid))
	out, err := f.m.High(&p)
	require.NoError(f.t, err)
	require.NoError(f.t, f.m.Commit())
	return out
}

func TestMerger_High_SingleChunk(t *testing.T) {
	t.Parallel()
	// Every case reads chunk 0 with rows for keys 1, 2, 3; LOW has xid 100.
	cases := []struct {
		name      string
		before    func(f *feeder) // stream before LOW
		window    func(f *feeder) // stream between LOW and HIGH
		snap      string
		committed []uint64
		want      watermark.Outcome
	}{
		{
			name: "quiet table keeps every row", snap: "101:101:",
			want: watermark.Outcome{Keep: []int{0, 1, 2}},
		},
		{
			name: "update in window drops the key", snap: "103:103:",
			window: func(f *feeder) { f.tx(102, 2) },
			want:   watermark.Outcome{Keep: []int{0, 2}, DroppedWindow: 1},
		},
		{
			name: "key-changing update in window drops old and new key", snap: "103:103:",
			window: func(f *feeder) { f.move(102, 2, 3) },
			want:   watermark.Outcome{Keep: []int{0}, DroppedWindow: 2},
		},
		{
			name: "key-changing update to a key outside the chunk drops the old key", snap: "103:103:",
			window: func(f *feeder) { f.move(102, 1, 9) },
			want:   watermark.Outcome{Keep: []int{1, 2}, DroppedWindow: 1},
		},
		{
			name: "insert in window outside the chunk is irrelevant", snap: "103:103:",
			window: func(f *feeder) { f.tx(102, 7) },
			want:   watermark.Outcome{Keep: []int{0, 1, 2}},
		},
		{
			name: "window commit invisible to the read is dropped", snap: "102:103:102", committed: []uint64{102},
			window: func(f *feeder) { f.tx(102, 3) },
			want:   watermark.Outcome{Keep: []int{0, 1}, DroppedWindow: 1},
		},
		{
			name: "commit before LOW visible to the read is kept", snap: "101:101:",
			before: func(f *feeder) { f.tx(99, 1) },
			want:   watermark.Outcome{Keep: []int{0, 1, 2}},
		},
		{
			name: "commit before LOW listed in xip is dropped", snap: "99:101:99", committed: []uint64{99},
			before: func(f *feeder) { f.tx(99, 1) },
			want:   watermark.Outcome{Keep: []int{1, 2}, DroppedInvisible: 1},
		},
		{
			// The transaction got its xid after LOW's and wrote its commit
			// record before LOW's, but left the proc array last: xmax is
			// LOW+1, so it is invisible without being listed in xip.
			name: "commit before LOW at or above xmax is dropped", snap: "101:101:",
			before: func(f *feeder) { f.tx(101, 2) },
			want:   watermark.Outcome{Keep: []int{0, 2}, DroppedInvisible: 1},
		},
		{
			name: "commit before LOW that touched nothing is ignored", snap: "99:101:99", committed: []uint64{99},
			before: func(f *feeder) { f.tx(99) },
			want:   watermark.Outcome{Keep: []int{0, 1, 2}},
		},
		{
			name: "xip transaction committing after HIGH is ignored", snap: "98:101:98",
			want: watermark.Outcome{Keep: []int{0, 1, 2}},
		},
		{
			name: "retry when a committed xip xid precedes the merger", snap: "98:101:98", committed: []uint64{98},
			want: watermark.Outcome{Retry: true, RetryXID: 98},
		},
		{
			name: "retry wins over drops", snap: "98:103:98", committed: []uint64{98},
			window: func(f *feeder) { f.tx(102, 1) },
			want:   watermark.Outcome{Retry: true, RetryXID: 98},
		},
		{
			// Postgres 15+ pgoutput skips transactions without published
			// changes: the xid began after LOW, so it cannot have committed
			// before the merger existed.
			name: "unread committed xid newer than the first LOW is ignored", snap: "100:105:103",
			committed: []uint64{103},
			want:      watermark.Outcome{Keep: []int{0, 1, 2}},
		},
		{
			name: "truncate in window drops every row", snap: "103:103:",
			window: func(f *feeder) { f.truncate(102) },
			want:   watermark.Outcome{DroppedWindow: 3},
		},
		{
			name: "invisible truncate before LOW drops every row", snap: "99:101:99", committed: []uint64{99},
			before: func(f *feeder) { f.truncate(99) },
			want:   watermark.Outcome{DroppedInvisible: 3},
		},
		{
			name: "visible truncate before LOW is ignored", snap: "101:101:",
			before: func(f *feeder) { f.truncate(99) },
			want:   watermark.Outcome{Keep: []int{0, 1, 2}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFeeder(t)
			if tc.before != nil {
				tc.before(f)
			}
			f.low(100, 0)
			f.rows(0, 1, 2, 3)
			if tc.window != nil {
				tc.window(f)
			}
			got := f.high(110, 0, tc.snap, tc.committed...)
			if tc.want.Keep == nil && !tc.want.Retry {
				tc.want.Keep = []int{}
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMerger_High_CarriesInvisibleAcrossChunks(t *testing.T) {
	t.Parallel()
	f := newFeeder(t)
	f.tx(99, 1) // commit record read, then stuck before the proc array update
	f.low(100, 0)
	f.rows(0, 1, 2)
	out := f.high(101, 0, "99:101:99", 99)
	assert.Equal(t, []int{1}, out.Keep)
	assert.Equal(t, 1, f.m.Retained(), "invisible transaction kept after HIGH")

	// Still invisible to the next chunk read: forgetting it at HIGH would
	// force a retry here; retaining it drops the key instead.
	f.low(102, 1)
	f.rows(1, 1, 3)
	out = f.high(103, 1, "99:103:99", 99)
	assert.False(t, out.Retry)
	assert.Equal(t, []int{1}, out.Keep)
	assert.Equal(t, 1, out.DroppedInvisible)

	// Visible at last: kept and forgotten.
	f.low(104, 2)
	f.rows(2, 1)
	out = f.high(105, 2, "105:105:")
	assert.Equal(t, []int{0}, out.Keep)
	assert.Zero(t, f.m.Retained())
}

func TestMerger_High_WindowCommitBecomesBeforeLow(t *testing.T) {
	t.Parallel()
	f := newFeeder(t)
	f.low(100, 0)
	f.rows(0, 1)
	f.tx(102, 5) // commits after the read's snapshot was taken
	out := f.high(103, 0, "102:102:")
	assert.Equal(t, []int{0}, out.Keep)
	assert.Equal(t, 1, f.m.Retained(), "commit after the snapshot is not yet proven visible")

	f.low(104, 1)
	f.rows(1, 5, 6)
	out = f.high(105, 1, "102:105:102", 102)
	assert.Equal(t, []int{1}, out.Keep)
	assert.Equal(t, 1, out.DroppedInvisible)
}

func TestMerger_Low_AbandonsOpenWindow(t *testing.T) {
	t.Parallel()
	f := newFeeder(t)
	f.low(100, 0)
	f.tx(101, 1)
	f.low(102, 1) // chunk 0 was given up before its HIGH
	f.rows(1, 1, 2)

	p := highPayload(0, watermark.Snapshot{Xmin: 103, Xmax: 103})
	require.NoError(t, f.m.Begin(103))
	_, err := f.m.High(&p)
	require.ErrorIs(t, err, watermark.ErrNoLow)
	require.NoError(t, f.m.Commit())

	out := f.high(104, 1, "101:103:101", 101)
	assert.Equal(t, []int{1}, out.Keep, "the abandoned window's commit is before the new LOW")
	assert.Equal(t, 1, out.DroppedInvisible)
}

func TestMerger_High_RetryThenRerun(t *testing.T) {
	t.Parallel()
	f := newFeeder(t)
	f.low(100, 0)
	f.rows(0, 1, 2)
	out := f.high(101, 0, "98:101:98", 98)
	require.True(t, out.Retry)
	assert.Equal(t, uint64(98), out.RetryXID)
	assert.Nil(t, out.Keep)

	// The re-run gets a new chunk counter; the old transaction is visible.
	f.tx(102, 2)
	f.low(103, 1)
	f.rows(1, 1, 2)
	out = f.high(104, 1, "104:104:")
	assert.False(t, out.Retry)
	assert.Equal(t, []int{0, 1}, out.Keep)
}

func TestMerger_High_WraparoundXIDs(t *testing.T) {
	t.Parallel()
	const epoch = uint64(7) << 32
	f := newFeeder(t)
	f.tx(0xFFFFFFFE, 1) // invisible, listed in xip of the next epoch's snapshot
	f.tx(0xFFFFFFFF, 2) // visible
	f.low(3, 0)         // xid wrapped to the next epoch (0..2 are special)
	f.rows(0, 1, 2, 3)
	f.tx(5, 3)
	out := f.high(6, 0, snapText(epoch|0xFFFFFFFE, 8<<32|6, epoch|0xFFFFFFFE), epoch|0xFFFFFFFE)
	assert.False(t, out.Retry)
	assert.Equal(t, []int{1}, out.Keep)
	assert.Equal(t, 1, out.DroppedInvisible)
	assert.Equal(t, 1, out.DroppedWindow)

	// An xid of the previous epoch that the merger never read forces a
	// retry: it precedes the first LOW.
	f.low(7, 1)
	f.rows(1, 4)
	out = f.high(8, 1, snapText(epoch|0xFFFFFFF0, 8<<32|8, epoch|0xFFFFFFF0), epoch|0xFFFFFFF0)
	assert.True(t, out.Retry)
	assert.Equal(t, epoch|0xFFFFFFF0, out.RetryXID)
}

func snapText(xmin, xmax uint64, xip ...uint64) string {
	return watermark.Snapshot{Xmin: xmin, Xmax: xmax, XIP: xip}.String()
}

func TestMerger_Calls_Errors(t *testing.T) {
	t.Parallel()
	low, high := lowPayload(0), highPayload(0, watermark.Snapshot{Xmin: 101, Xmax: 101})
	badHigh := highPayload(0, watermark.Snapshot{Xmin: 5, Xmax: 4})
	cases := []struct {
		name string
		call func(m *watermark.Merger) error
		want error
	}{
		{
			name: "touch outside transaction", call: func(m *watermark.Merger) error { return m.Touch(keyBytes(1)) },
			want: watermark.ErrNoTransaction,
		},
		{
			name: "truncate outside transaction", call: func(m *watermark.Merger) error { return m.TouchAll() },
			want: watermark.ErrNoTransaction,
		},
		{
			name: "commit outside transaction", call: func(m *watermark.Merger) error { return m.Commit() },
			want: watermark.ErrNoTransaction,
		},
		{
			name: "low outside transaction", call: func(m *watermark.Merger) error { return m.Low(&low) },
			want: watermark.ErrNoTransaction,
		},
		{
			name: "high outside transaction", call: func(m *watermark.Merger) error { _, err := m.High(&high); return err },
			want: watermark.ErrNoTransaction,
		},
		{
			name: "nested begin", call: func(m *watermark.Merger) error { _ = m.Begin(1); return m.Begin(2) },
			want: watermark.ErrOpenTransaction,
		},
		{
			name: "high payload to Low", call: func(m *watermark.Merger) error { _ = m.Begin(1); return m.Low(&high) },
			want: watermark.ErrWrongMark,
		},
		{name: "low payload to High", call: func(m *watermark.Merger) error {
			_ = m.Begin(1)
			_, err := m.High(&low)
			return err
		}, want: watermark.ErrWrongMark},
		{name: "invalid high", call: func(m *watermark.Merger) error {
			_ = m.Begin(1)
			_, err := m.High(&badHigh)
			return err
		}, want: watermark.ErrInvalidSnapshot},
		{name: "high without low", call: func(m *watermark.Merger) error {
			_ = m.Begin(1)
			m.Rows(0, nil)
			_, err := m.High(&high)
			return err
		}, want: watermark.ErrNoLow},
		{name: "high without rows", call: func(m *watermark.Merger) error {
			_ = m.Begin(1)
			_ = m.Low(&low)
			_, err := m.High(&high)
			return err
		}, want: watermark.ErrNoRows},
		{name: "rows of another chunk", call: func(m *watermark.Merger) error {
			_ = m.Begin(1)
			_ = m.Low(&low)
			m.Rows(7, nil)
			_, err := m.High(&high)
			return err
		}, want: watermark.ErrNoRows},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tc.call(watermark.NewMerger()), tc.want)
		})
	}
}

func TestMerger_High_ErrorKeepsState(t *testing.T) {
	t.Parallel()
	f := newFeeder(t)
	f.low(100, 0)
	p := highPayload(0, watermark.Snapshot{Xmin: 101, Xmax: 101})
	require.NoError(t, f.m.Begin(101))
	_, err := f.m.High(&p)
	require.ErrorIs(t, err, watermark.ErrNoRows)
	f.m.Rows(0, [][]byte{keyBytes(1)})
	out, err := f.m.High(&p)
	require.NoError(t, err)
	assert.Equal(t, []int{0}, out.Keep)
}
