// SPDX-License-Identifier: Apache-2.0

// Package watermark holds the pure parts of Wake's watermark-based table
// snapshot: the wake.watermark and wake.snapshot
// logical message payloads, the pg_snapshot text form with 32-bit/64-bit xid
// matching, the chunk/stream merge with the visibility rule, per-table
// progress and snapshot row ids.
//
// Main types:
//   - Payload: LOW/HIGH watermark JSON, strict decoding, nonce filter;
//   - Trigger: the {"table":"<schema>.<table>"} snapshot request;
//   - Snapshot: pg_current_snapshot() (xmin:xmax:xip) with FullXID,
//     MatchXID and IsVisible for 32-bit pgoutput xids;
//   - Merger: decides, at each HIGH, which chunk rows to emit as r events or
//     whether to re-run the chunk;
//   - Progress: committed upper key over chunks acknowledged out of order;
//   - RowN, RowID: snapshot event ids n = c × chunk_rows + i.
//
// Usage on the stream side (one Merger per run, created before the first
// LOW is emitted; the snapshotter calls m.Rows(chunk, keys) before emitting
// each HIGH):
//
//	m := watermark.NewMerger()
//	// for each stream transaction, in WAL order:
//	_ = m.Begin(xid)
//	_ = m.Touch(change.AppendCanonicalKey(nil, ev.Key)) // per change of the table
//	if p, err := watermark.ParsePayload(content); err == nil && p.IsFor(nonce, run, table) {
//		if p.Mark == watermark.MarkLow {
//			_ = m.Low(&p)
//		} else if out, err := m.High(&p); err == nil && !out.Retry {
//			// emit rows out.Keep as r events with watermark.RowID(highLSN, run, p.Chunk, chunkRows, i)
//		}
//	}
//	_ = m.Commit()
package watermark
