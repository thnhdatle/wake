// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// txEntry is one registered transaction of the ack tracker.
type txEntry struct {
	commit lsn.LSN
	end    lsn.LSN
	readAt time.Time
	// pending counts deliveries not yet acknowledged; ungrouped counts those
	// outside any group (they age from readAt).
	pending    int
	ungrouped  int
	openGroups int
	sealed     bool
	// complete is sealed && pending == 0; the entry stays in the ring until
	// every earlier transaction is complete as well.
	complete bool
	// done is created on demand by AckTracker.Done and closed on completion.
	done chan struct{}
}

// txRing is a growable circular deque of transactions addressed by a
// monotonically increasing sequence number. Push, pop and lookup are O(1)
// (push amortized); capacity is a power of two and never exceeds the
// smallest power of two ≥ the tracker's max in-flight transactions.
type txRing struct {
	buf     []txEntry
	head    int
	n       int
	headSeq uint64 // sequence of buf[head]; the first transaction gets 1
}

func newTxRing() txRing { return txRing{buf: make([]txEntry, 16), headSeq: 1} }

func (r *txRing) len() int { return r.n }

// push appends e and returns its sequence number.
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

// get returns the entry of seq, or nil when it is not in the ring.
func (r *txRing) get(seq uint64) *txEntry {
	if seq < r.headSeq || seq-r.headSeq >= uint64(r.n) { //nolint:gosec // G115: n is non-negative
		return nil
	}
	return r.at(int(seq - r.headSeq)) //nolint:gosec // G115: bounded by n above
}

// at returns the i-th entry from the front (0 ≤ i < len).
func (r *txRing) at(i int) *txEntry { return &r.buf[(r.head+i)&(len(r.buf)-1)] }

// popFront removes the front entry.
func (r *txRing) popFront() {
	r.buf[r.head] = txEntry{}
	r.head = (r.head + 1) & (len(r.buf) - 1)
	r.n--
	r.headSeq++
}
