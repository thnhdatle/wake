// SPDX-License-Identifier: Apache-2.0

package watermark

import "errors"

var ErrInvalidSnapshot = errors.New("watermark: invalid snapshot")

var ErrInvalidPayload = errors.New("watermark: invalid payload")

var ErrInvalidTrigger = errors.New("watermark: invalid snapshot trigger")

var ErrNoTransaction = errors.New("watermark: no open transaction")

var ErrOpenTransaction = errors.New("watermark: transaction already open")

var ErrWrongMark = errors.New("watermark: wrong mark")

var ErrNoLow = errors.New("watermark: high without matching low")

var ErrNoRows = errors.New("watermark: chunk rows missing")

var ErrChunkOrder = errors.New("watermark: chunk out of order")

var ErrUnknownChunk = errors.New("watermark: unknown chunk")

var ErrRowIndex = errors.New("watermark: row index out of range")

var ErrIDOverflow = errors.New("watermark: snapshot counter overflow")
