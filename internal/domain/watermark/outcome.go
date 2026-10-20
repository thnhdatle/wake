// SPDX-License-Identifier: Apache-2.0

package watermark

// Outcome is the Merger's decision for one chunk, taken when its HIGH
// watermark is read.
type Outcome struct {
	// Chunk is the chunk counter of the HIGH watermark.
	Chunk uint64
	// Retry is true when the chunk must be discarded and re-run: an xid of
	// the HIGH snapshot's xip committed before the retention horizon, so the
	// keys it touched are unknown. Keep is nil then.
	Retry bool
	// RetryXID is the full xid that forced the retry.
	RetryXID uint64
	// Keep holds the positions (indices into the keys passed to Rows, in
	// increasing order) of the rows to emit as r events right after HIGH.
	Keep []int
	// DroppedWindow counts rows dropped because a stream event of the table
	// between LOW and HIGH had their key as key or before key.
	DroppedWindow int
	// DroppedInvisible counts the other dropped rows: keys touched by a
	// transaction whose commit was read before LOW but which the chunk read
	// did not see.
	DroppedInvisible int
}
