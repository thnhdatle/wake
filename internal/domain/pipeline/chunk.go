// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"encoding/json"

	"github.com/thnhdatle/wake/internal/domain/change"
)

type ChunkRequest struct {
	Schema string
	Table  string
	After  json.RawMessage
	Limit  int
}

type Chunk struct {
	Rows         []ChunkRow
	Last         json.RawMessage
	Done         bool
	Snapshot     TxSnapshot
	CommittedXIP []uint64
}

type ChunkRow struct {
	Key   change.Row
	After change.Row
}

type TxSnapshot struct {
	XMin uint64
	XMax uint64
	XIP  []uint64
}
