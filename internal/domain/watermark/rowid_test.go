// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/watermark"
)

func TestRowN_Values(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                 string
		chunk, rows, i, want uint64
		err                  error
	}{
		{name: "first row", chunk: 0, rows: 10000, i: 0, want: 0},
		{name: "chunk 3 row 7", chunk: 3, rows: 10000, i: 7, want: 30007},
		{name: "last row", chunk: 2, rows: 5, i: 4, want: 14},
		{name: "largest n", chunk: math.MaxUint64 / 2, rows: 2, i: 1, want: math.MaxUint64},
		{name: "row beyond chunk", chunk: 0, rows: 5, i: 5, err: watermark.ErrRowIndex},
		{name: "zero chunk size", chunk: 0, rows: 0, i: 0, err: watermark.ErrRowIndex},
		{name: "overflow", chunk: math.MaxUint64 / 2, rows: 3, i: 0, err: watermark.ErrIDOverflow},
		{name: "overflow by the row", chunk: math.MaxUint64 / 10, rows: 10, i: 9, err: watermark.ErrIDOverflow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := watermark.RowN(tc.chunk, tc.rows, tc.i)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRowID_Format(t *testing.T) {
	t.Parallel()
	id, err := watermark.RowID(lsn.LSN(0x16B3748), 12, 3, 10000, 7)
	require.NoError(t, err)
	assert.Equal(t, "0/16B3748-r12-30007", id.String())
	_, err = watermark.RowID(1, 1, 0, 0, 0)
	require.ErrorIs(t, err, watermark.ErrRowIndex)
}

func TestRowN_UniqueAcrossChunks(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		rows := rapid.Uint64Range(1, 1<<20).Draw(t, "chunk_rows")
		c1, c2 := rapid.Uint64Range(0, 1<<20).Draw(t, "c1"), rapid.Uint64Range(0, 1<<20).Draw(t, "c2")
		i1, i2 := rapid.Uint64Range(0, rows-1).Draw(t, "i1"), rapid.Uint64Range(0, rows-1).Draw(t, "i2")
		n1, err := watermark.RowN(c1, rows, i1)
		require.NoError(t, err)
		n2, err := watermark.RowN(c2, rows, i2)
		require.NoError(t, err)
		assert.Equal(t, c1 == c2 && i1 == i2, n1 == n2)
	})
}
