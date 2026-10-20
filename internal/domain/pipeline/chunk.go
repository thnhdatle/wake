// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"encoding/json"

	"github.com/thnhdatle/wake/internal/domain/change"
)

// ChunkRequest asks for the next chunk of a table.
type ChunkRequest struct {
	// Schema and Table are the Postgres names (before tables.name_map).
	Schema string
	Table  string
	// After is the position of the last row already read: the JSON array of
	// key values in key order, or the adapter's ctid-range position for FULL
	// tables without a key. Nil starts at the beginning.
	After json.RawMessage
	// Limit is the chunk size (snapshot.chunk_rows of the run).
	Limit int
}

// Chunk is the result of one chunk read.
type Chunk struct {
	Rows []ChunkRow
	// Last is the position of the last row, to pass as the next After.
	Last json.RawMessage
	// Done is true when the table has no rows after Last.
	Done bool
	// Snapshot is the reading transaction's pg_current_snapshot()
	// (ChunkReader.ReadChunk only).
	Snapshot TxSnapshot
	// CommittedXIP holds the xids of Snapshot.XIP that pg_xact_status
	// reported committed (ChunkReader.ReadChunk only).
	CommittedXIP []uint64
}

// ChunkRow is one row read by a chunk.
type ChunkRow struct {
	// Key holds the key columns, After the row image, both in CDC event JSON form.
	Key   change.Row
	After change.Row
}

// TxSnapshot is a Postgres transaction snapshot (pg_snapshot).
type TxSnapshot struct {
	XMin uint64
	XMax uint64
	XIP  []uint64
}
