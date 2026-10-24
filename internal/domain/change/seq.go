// SPDX-License-Identifier: Apache-2.0

package change

import "github.com/thnhdatle/wake/internal/domain/lsn"

// Seq is the total order of change events.
type Seq struct {
	LSN    lsn.LSN
	K      uint64
	Schema bool
}

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

func (s Seq) Less(o Seq) bool { return CompareSeq(s, o) < 0 }
