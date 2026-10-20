// SPDX-License-Identifier: Apache-2.0

package snapshot

// OtherTable is the table label of snapshot requests for uncaptured or
// unparsable tables (wake_snapshot_requests_total{table="_other"}).
const OtherTable = "_other"

// Metrics receives the snapshot counters; the adapter layer maps them to
// wake_snapshot_rows_total, wake_snapshot_requests_total and
// wake_snapshot_chunk_retries_total. Implementations must not block.
type Metrics interface {
	// RowsAcked counts snapshot rows whose `r` events every sink
	// acknowledged.
	RowsAcked(table string, n uint64)
	// Request counts a table snapshot request (queued table), OtherTable
	// for uncaptured ones.
	Request(table string)
	// ChunkRetry counts a chunk discarded and re-run.
	ChunkRetry(table string)
}

type nopMetrics struct{}

func (nopMetrics) RowsAcked(string, uint64) {}
func (nopMetrics) Request(string)           {}
func (nopMetrics) ChunkRetry(string)        {}
