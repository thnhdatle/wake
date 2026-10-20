// SPDX-License-Identifier: Apache-2.0

package change

import "github.com/thnhdatle/wake/internal/domain/lsn"

// Seq is the CDC event sequence (lsn, k): a total order over all events of a
// source. A schema-change event sorts before the data event with the same
// (lsn, k).
type Seq struct {
	LSN    lsn.LSN
	K      uint64
	Schema bool
}

// CompareSeq returns -1, 0 or +1 when a sorts before, equal to or after b.
func CompareSeq(a, b Seq) int {
	if c := lsn.Compare(a.LSN, b.LSN); c != 0 {
		return c
	}
	switch {
	case a.K < b.K:
		return -1
	case a.K > b.K:
		return 1
	case a.Schema == b.Schema:
		return 0
	case a.Schema:
		return -1
	default:
		return 1
	}
}

// Less reports whether s sorts before o.
func (s Seq) Less(o Seq) bool { return CompareSeq(s, o) < 0 }
