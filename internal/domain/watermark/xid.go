// SPDX-License-Identifier: Apache-2.0

package watermark

import "math"

// Postgres prints snapshots with 64-bit FullTransactionIds (xid8: epoch in
// the high 32 bits, xid in the low 32 bits), while pgoutput Begin messages
// carry the 32-bit xid. Two facts make the 32-bit value sufficient:
//
//   - Postgres refuses to assign new xids before any live xid is 2^31 old
//     (wraparound protection), so every transaction that can be running,
//     committed-but-invisible or recently committed lies within 2^31 of the
//     current next xid. Within such a window the low 32 bits identify a full
//     xid uniquely: two full xids with equal low bits are 2^32 apart.
//   - A snapshot's xmax is at most the next xid, and xip lies in
//     [xmin, xmax) with xmax-xmin < 2^31 (ParseSnapshot rejects wider ones).
//
// FullXID therefore widens a 32-bit xid to the full xid nearest to a full
// reference, and MatchXID compares a 32-bit xid with a full one by low bits.

// halfXIDSpace is 2^31, the wraparound horizon of 32-bit xids.
const halfXIDSpace = 1 << 31

// FullXID returns the 64-bit xid whose low 32 bits equal xid and which lies
// in [ref-2^31, ref+2^31) around the full xid ref, the same widening
// Postgres applies with an epoch-aware reference. When the backward
// candidate would be negative (epoch 0) the forward one is returned, and
// when the forward candidate would overflow the backward one is.
func FullXID(xid uint32, ref uint64) uint64 {
	forward := xid - uint32(ref)      //nolint:gosec // G115: low 32 bits of ref are intended
	back := uint64(uint32(ref) - xid) //nolint:gosec // G115: low 32 bits of ref are intended
	if forward < halfXIDSpace && ref <= math.MaxUint64-uint64(forward) {
		return ref + uint64(forward)
	}
	if back <= ref {
		return ref - back
	}
	return ref + uint64(forward)
}

// MatchXID reports whether the 32-bit xid from a pgoutput Begin message
// names the full xid from a snapshot, by comparing low 32 bits.
func MatchXID(xid uint32, full uint64) bool {
	return uint32(full) == xid //nolint:gosec // G115: low 32 bits are the xid by definition
}

// precedes reports whether the 32-bit xid a was assigned before b, with
// Postgres' modulo-2^32 comparison (TransactionIdPrecedes); valid when both
// are within 2^31 of each other.
func precedes(a, b uint32) bool {
	return int32(a-b) < 0 //nolint:gosec // G115: modulo-2^32 distance is the intent
}
