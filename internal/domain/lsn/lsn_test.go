// SPDX-License-Identifier: Apache-2.0

package lsn_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

func TestParse_Valid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want lsn.LSN
	}{
		{name: "zero", in: "0/0", want: 0},
		{name: "low half", in: "0/16B3748", want: 0x16B3748},
		{name: "lower case", in: "1/a", want: 1<<32 | 0xA},
		{name: "max", in: "FFFFFFFF/FFFFFFFF", want: ^lsn.LSN(0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := lsn.Parse(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParse_Invalid(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"", "0", "/1", "1/", "G/0", "0/G", "123456789/0", "0/123456789", "1/2/3", "-1/0"} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			_, err := lsn.Parse(in)
			require.ErrorIs(t, err, lsn.ErrInvalid)
		})
	}
}

func TestLSN_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "0/0", lsn.LSN(0).String())
	assert.Equal(t, "0/16B3748", lsn.LSN(0x16B3748).String())
	assert.Equal(t, "1/A", lsn.LSN(1<<32|0xA).String())
	assert.Equal(t, "FFFFFFFF/FFFFFFFF", (^lsn.LSN(0)).String())
}

func TestLSN_TextRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		l := lsn.LSN(rapid.Uint64().Draw(t, "lsn"))
		b, err := l.MarshalText()
		require.NoError(t, err)
		var got lsn.LSN
		require.NoError(t, got.UnmarshalText(b))
		assert.Equal(t, l, got)
	})
}

func TestLSN_UnmarshalTextInvalid(t *testing.T) {
	t.Parallel()
	var l lsn.LSN
	require.ErrorIs(t, l.UnmarshalText([]byte("x")), lsn.ErrInvalid)
}

func TestCompare_MatchesNumericOrder(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		a := lsn.LSN(rapid.Uint64().Draw(t, "a"))
		b := lsn.LSN(rapid.Uint64().Draw(t, "b"))
		c := lsn.Compare(a, b)
		switch {
		case a < b:
			assert.Equal(t, -1, c)
		case a > b:
			assert.Equal(t, 1, c)
		default:
			assert.Equal(t, 0, c)
		}
		assert.Equal(t, -c, lsn.Compare(b, a))
	})
}
