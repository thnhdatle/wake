// SPDX-License-Identifier: Apache-2.0

package watermark

import "math"

const halfXIDSpace = 1 << 31

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

func MatchXID(xid uint32, full uint64) bool {
	return uint32(full) == xid //nolint:gosec // G115: low 32 bits are the xid by definition
}

func precedes(a, b uint32) bool {
	return int32(a-b) < 0 //nolint:gosec // G115: modulo-2^32 distance is the intent
}
