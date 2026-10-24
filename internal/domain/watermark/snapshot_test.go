// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/watermark"
)

func TestParseSnapshot_Valid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want watermark.Snapshot
		text string
	}{
		{name: "empty xip", in: "10:20:", want: watermark.Snapshot{Xmin: 10, Xmax: 20}, text: "10:20:"},
		{name: "xmin equals xmax", in: "7:7:", want: watermark.Snapshot{Xmin: 7, Xmax: 7}, text: "7:7:"},
		{
			name: "xip list", in: "10:20:10,12,19",
			want: watermark.Snapshot{Xmin: 10, Xmax: 20, XIP: []uint64{10, 12, 19}}, text: "10:20:10,12,19",
		},
		{
			name: "duplicates dropped as Postgres does", in: "10:20:12,12,15",
			want: watermark.Snapshot{Xmin: 10, Xmax: 20, XIP: []uint64{12, 15}}, text: "10:20:12,15",
		},
		{
			name: "epoch 1", in: "4294967299:4294967310:4294967300",
			want: watermark.Snapshot{Xmin: 1<<32 | 3, Xmax: 1<<32 | 14, XIP: []uint64{1<<32 | 4}},
			text: "4294967299:4294967310:4294967300",
		},
		{
			name: "range of 2^31", in: "3:2147483651:",
			want: watermark.Snapshot{Xmin: 3, Xmax: 3 + 1<<31}, text: "3:2147483651:",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := watermark.ParseSnapshot(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.text, got.String())
		})
	}
}

func TestParseSnapshot_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ""},
		{name: "one colon", in: "10:20"},
		{name: "three fields of xip", in: "10:20:12:13"},
		{name: "missing xmin", in: ":20:"},
		{name: "missing xmax", in: "10::"},
		{name: "sign", in: "+10:20:"},
		{name: "negative", in: "-10:20:"},
		{name: "leading zero", in: "010:20:"},
		{name: "hex", in: "0x10:20:"},
		{name: "space", in: "10: 20:"},
		{name: "overflow", in: "10:18446744073709551616:"},
		{name: "zero xmin", in: "0:20:"},
		{name: "xmin low half zero", in: "4294967296:4294967300:"},
		{name: "xmin after xmax", in: "21:20:"},
		{name: "range beyond wraparound", in: "3:2147483652:"},
		{name: "xip below xmin", in: "10:20:9"},
		{name: "xip at xmax", in: "10:20:20"},
		{name: "xip unsorted", in: "10:20:15,12"},
		{name: "empty xip item", in: "10:20:12,,13"},
		{name: "trailing comma", in: "10:20:12,"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := watermark.ParseSnapshot(tc.in)
			require.ErrorIs(t, err, watermark.ErrInvalidSnapshot)
		})
	}
}

func TestSnapshot_Validate_StrictXIP(t *testing.T) {
	t.Parallel()
	err := watermark.Snapshot{Xmin: 10, Xmax: 20, XIP: []uint64{12, 12}}.Validate()
	require.ErrorIs(t, err, watermark.ErrInvalidSnapshot)
}

func TestSnapshot_IsZero_Values(t *testing.T) {
	t.Parallel()
	assert.True(t, watermark.Snapshot{}.IsZero())
	assert.False(t, watermark.Snapshot{Xmin: 3, Xmax: 3}.IsZero())
	assert.False(t, watermark.Snapshot{XIP: []uint64{1}}.IsZero())
}

func TestSnapshot_MarshalText_RoundTrip(t *testing.T) {
	t.Parallel()
	in := watermark.Snapshot{Xmin: 100, Xmax: 110, XIP: []uint64{100, 105}}
	b, err := in.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "100:110:100,105", string(b))
	var out watermark.Snapshot
	require.NoError(t, out.UnmarshalText(b))
	assert.Equal(t, in, out)

	_, err = watermark.Snapshot{Xmin: 5, Xmax: 4}.MarshalText()
	require.ErrorIs(t, err, watermark.ErrInvalidSnapshot)
	require.ErrorIs(t, out.UnmarshalText([]byte("x")), watermark.ErrInvalidSnapshot)
	assert.Equal(t, in, out, "failed unmarshal leaves the value unchanged")
}

func genSnapshot(t *rapid.T) watermark.Snapshot {
	xmin := rapid.Uint64Range(3, math.MaxUint64-1<<31).Draw(t, "xmin")
	if uint32(xmin) == 0 {
		xmin += 3
	}
	xmax := xmin + rapid.Uint64Range(0, 1000).Draw(t, "width")
	if uint32(xmax) == 0 {
		xmax++
	}
	s := watermark.Snapshot{Xmin: xmin, Xmax: xmax}
	for x := xmin; x < xmax && len(s.XIP) < 20; x += rapid.Uint64Range(1, 50).Draw(t, "gap") {
		if rapid.Bool().Draw(t, "in_xip") {
			s.XIP = append(s.XIP, x)
		}
	}
	return s
}

func TestSnapshot_String_ParseRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := genSnapshot(t)
		require.NoError(t, s.Validate())
		got, err := watermark.ParseSnapshot(s.String())
		require.NoError(t, err)
		assert.Equal(t, s.Xmin, got.Xmin)
		assert.Equal(t, s.Xmax, got.Xmax)
		assert.Len(t, got.XIP, len(s.XIP))
		assert.Equal(t, s.String(), got.String())
	})
}

func TestSnapshot_IsVisible_Cases(t *testing.T) {
	t.Parallel()
	const epoch = uint64(5) << 32
	snap := watermark.Snapshot{Xmin: epoch | 100, Xmax: epoch | 110, XIP: []uint64{epoch | 100, epoch | 104}}
	wrapped := watermark.Snapshot{Xmin: 6<<32 - 4, Xmax: 6<<32 | 5, XIP: []uint64{6<<32 - 2, 6<<32 | 3}}
	cases := []struct {
		name    string
		snap    watermark.Snapshot
		xid     uint32
		visible bool
		inXIP   bool
	}{
		{name: "below xmin", snap: snap, xid: 99, visible: true},
		{name: "listed in xip", snap: snap, xid: 104, inXIP: true},
		{name: "xmin listed", snap: snap, xid: 100, inXIP: true},
		{name: "completed in range", snap: snap, xid: 105, visible: true},
		{name: "at xmax", snap: snap, xid: 110},
		{name: "above xmax", snap: snap, xid: 200},
		{name: "old epoch tail", snap: wrapped, xid: 0xFFFFFFF0, visible: true},
		{name: "xip before wrap", snap: wrapped, xid: 0xFFFFFFFE, inXIP: true},
		{name: "xip after wrap", snap: wrapped, xid: 3, inXIP: true},
		{name: "completed after wrap", snap: wrapped, xid: 4, visible: true},
		{name: "running after wrap at xmax", snap: wrapped, xid: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.visible, tc.snap.IsVisible(tc.xid))
			assert.Equal(t, tc.inXIP, tc.snap.HasXIP(tc.xid))
		})
	}
}

func TestSnapshot_HasXIP_MatchesLowBits(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := genSnapshot(t)
		xid := rapid.Uint32().Draw(t, "xid")
		want := false
		for _, x := range s.XIP {
			want = want || watermark.MatchXID(xid, x)
		}
		assert.Equal(t, want, s.HasXIP(xid))
		if want {
			assert.False(t, s.IsVisible(xid))
		}
	})
}

func TestFullXID_Cases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		xid  uint32
		ref  uint64
		want uint64
	}{
		{name: "same", xid: 10, ref: 10, want: 10},
		{name: "slightly newer", xid: 15, ref: 3<<32 | 10, want: 3<<32 | 15},
		{name: "slightly older", xid: 5, ref: 3<<32 | 10, want: 3<<32 | 5},
		{name: "older across wrap", xid: 0xFFFFFFF0, ref: 3<<32 | 10, want: 2<<32 | 0xFFFFFFF0},
		{name: "newer across wrap", xid: 4, ref: 3<<32 | 0xFFFFFFF0, want: 4<<32 | 4},
		{name: "epoch 0 has no older epoch", xid: 0xFFFFFFF0, ref: 10, want: 0xFFFFFFF0},
		{name: "top of space has no newer epoch", xid: 5, ref: math.MaxUint64 - 2, want: 0xFFFFFFFF_00000005},
		{name: "exactly 2^31 newer is older", xid: 1<<31 | 10, ref: 1<<32 | 10, want: 10 | 1<<31},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, watermark.FullXID(tc.xid, tc.ref))
		})
	}
}

func TestFullXID_NearestWithLowBits(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		ref := rapid.Uint64Range(1<<32, math.MaxUint64-1<<32).Draw(t, "ref")
		delta := rapid.Int64Range(-(1<<31), 1<<31-1).Draw(t, "delta")
		var want uint64
		if delta >= 0 {
			want = ref + uint64(delta)
		} else {
			want = ref - uint64(-delta)
		}
		got := watermark.FullXID(uint32(want), ref)
		assert.Equal(t, want, got)
		assert.True(t, watermark.MatchXID(uint32(want), got))
	})
}
