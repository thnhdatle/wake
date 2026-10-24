// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"fmt"
	"math"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
)

func RowN(chunk, chunkRows, i uint64) (uint64, error) {
	if chunkRows == 0 || i >= chunkRows {
		return 0, fmt.Errorf("%w: row %d of chunk size %d", ErrRowIndex, i, chunkRows)
	}
	if chunk > (math.MaxUint64-i)/chunkRows {
		return 0, fmt.Errorf("%w: chunk %d × %d + %d", ErrIDOverflow, chunk, chunkRows, i)
	}
	return chunk*chunkRows + i, nil
}

func RowID(high lsn.LSN, run, chunk, chunkRows, i uint64) (change.ID, error) {
	n, err := RowN(chunk, chunkRows, i)
	if err != nil {
		return change.ID{}, err
	}
	return change.SnapshotID(high, run, n), nil
}
