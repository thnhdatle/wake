// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"fmt"
	"math"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// RowN returns the snapshot counter n = chunk × chunkRows + i of the row at
// position i of a chunk: unique within the run as long as chunk counters
// are and i < chunkRows.
func RowN(chunk, chunkRows, i uint64) (uint64, error) {
	if chunkRows == 0 || i >= chunkRows {
		return 0, fmt.Errorf("%w: row %d of chunk size %d", ErrRowIndex, i, chunkRows)
	}
	if chunk > (math.MaxUint64-i)/chunkRows {
		return 0, fmt.Errorf("%w: chunk %d × %d + %d", ErrIDOverflow, chunk, chunkRows, i)
	}
	return chunk*chunkRows + i, nil
}

// RowID returns the CDC event id <high>-r<run>-<n> of the row at position i of a
// chunk emitted at the HIGH watermark's commit LSN high.
func RowID(high lsn.LSN, run, chunk, chunkRows, i uint64) (change.ID, error) {
	n, err := RowN(chunk, chunkRows, i)
	if err != nil {
		return change.ID{}, err
	}
	return change.SnapshotID(high, run, n), nil
}
