// SPDX-License-Identifier: Apache-2.0

package watermark

import "errors"

// ErrInvalidSnapshot reports text that is not a valid pg_snapshot.
var ErrInvalidSnapshot = errors.New("watermark: invalid snapshot")

// ErrInvalidPayload reports a wake.watermark payload that is malformed or
// violates the payload rules.
var ErrInvalidPayload = errors.New("watermark: invalid payload")

// ErrInvalidTrigger reports a wake.snapshot payload that is malformed.
var ErrInvalidTrigger = errors.New("watermark: invalid snapshot trigger")

// ErrNoTransaction reports a Merger call that needs an open transaction
// (Touch, TouchAll, Commit, Low, High) made outside Begin and Commit.
var ErrNoTransaction = errors.New("watermark: no open transaction")

// ErrOpenTransaction reports Begin while another transaction is open.
var ErrOpenTransaction = errors.New("watermark: transaction already open")

// ErrWrongMark reports a LOW payload passed to High or a HIGH payload passed
// to Low.
var ErrWrongMark = errors.New("watermark: wrong mark")

// ErrNoLow reports a HIGH watermark whose chunk has no LOW read since the
// merger was created or since the previous HIGH.
var ErrNoLow = errors.New("watermark: high without matching low")

// ErrNoRows reports a HIGH watermark read before the chunk's rows were
// handed to the merger.
var ErrNoRows = errors.New("watermark: chunk rows missing")

// ErrChunkOrder reports a chunk registered out of increasing order.
var ErrChunkOrder = errors.New("watermark: chunk out of order")

// ErrUnknownChunk reports completion of a chunk that was never registered.
var ErrUnknownChunk = errors.New("watermark: unknown chunk")

// ErrRowIndex reports a row position outside the chunk size, or a zero
// chunk size.
var ErrRowIndex = errors.New("watermark: row index out of range")

// ErrIDOverflow reports a snapshot counter n that does not fit in 64 bits.
var ErrIDOverflow = errors.New("watermark: snapshot counter overflow")
