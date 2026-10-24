// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type txEntry struct {
	commit     lsn.LSN
	end        lsn.LSN
	readAt     time.Time
	pending    int
	ungrouped  int
	openGroups int
	sealed     bool
	complete   bool
	done       chan struct{}
}

type txRing struct {
	buf     []txEntry
	head    int
	n       int
	headSeq uint64
}

func newTxRing() txRing { return txRing{buf: make([]txEntry, 16), headSeq: 1} }

func (r *txRing) len() int { return r.n }

func (r *txRing) push(e txEntry) uint64 {
	if r.n == len(r.buf) {
		grown := make([]txEntry, 2*len(r.buf))
		for i := range r.n {
			grown[i] = r.buf[(r.head+i)&(len(r.buf)-1)]
		}
		r.buf, r.head = grown, 0
	}
	r.buf[(r.head+r.n)&(len(r.buf)-1)] = e
	r.n++
	return r.headSeq + uint64(r.n) - 1 //nolint:gosec // G115: n is non-negative
}

func (r *txRing) get(seq uint64) *txEntry {
	if seq < r.headSeq || seq-r.headSeq >= uint64(r.n) { //nolint:gosec // G115: n is non-negative
		return nil
	}
	return r.at(int(seq - r.headSeq)) //nolint:gosec // G115: bounded by n above
}

func (r *txRing) at(i int) *txEntry { return &r.buf[(r.head+i)&(len(r.buf)-1)] }

func (r *txRing) popFront() {
	r.buf[r.head] = txEntry{}
	r.head = (r.head + 1) & (len(r.buf) - 1)
	r.n--
	r.headSeq++
}
