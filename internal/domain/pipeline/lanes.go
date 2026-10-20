// SPDX-License-Identifier: Apache-2.0

package pipeline

import "fmt"

// DefaultLanes is the default webhook.lanes.
const DefaultLanes = 8

// BarrierLane is LaneBatch.Lane of a barrier batch.
const BarrierLane = -1

// LaneOf hashes a partition key into one of n lanes (FNV-1a, 64 bit). It
// returns 0 for n <= 1.
func LaneOf(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, b := range key {
		h ^= uint64(b)
		h *= prime
	}
	return int(h % uint64(n)) //nolint:gosec // G115: n > 1, so the result fits in int
}

// BatchLimits bound one batch. A batch always holds at least one record,
// even when that record alone exceeds MaxBytes. Non-positive values mean no
// limit.
type BatchLimits struct {
	MaxEvents int
	MaxBytes  int
}

// LaneBatch is a batch handed out by LaneScheduler.Next.
type LaneBatch struct {
	// Lane is the lane index, or BarrierLane for a barrier sent alone.
	Lane    int
	Records []Record
}

// LaneScheduler orders records into parallel webhook lanes: keyed
// records hash into lanes by partition key, each lane has at most one batch
// in flight, so records of one key are delivered in push order. A barrier
// record (Record.Barrier) waits until every record pushed before it has
// completed (all lanes flush), is sent alone, and records pushed after it
// wait until it completes (lanes resume).
//
// It is pure logic and not safe for concurrent use; the adapter guards it
// with a mutex and drives timing (linger) and retries. Queued records are
// bounded by capacity: Push returns ErrLanesFull when full, and the adapter
// blocks its Send until a Complete frees space. Batches in flight are owned
// by the caller and bounded by lanes × BatchLimits.MaxEvents.
type LaneScheduler struct {
	lanes    []recordFIFO
	busy     []bool
	queued   int
	capacity int
	// blocked holds records pushed while a barrier is pending, in push
	// order; when non-empty its head is the pending barrier.
	blocked         recordFIFO
	barrierInFlight bool
	next            int // round-robin start for Next
}

// NewLaneScheduler returns a scheduler with n lanes holding at most
// capacity queued records.
func NewLaneScheduler(n, capacity int) (*LaneScheduler, error) {
	if n <= 0 || capacity <= 0 {
		return nil, fmt.Errorf("%w: lanes %d, capacity %d", ErrInvalidLanes, n, capacity)
	}
	return &LaneScheduler{
		lanes:    make([]recordFIFO, n),
		busy:     make([]bool, n),
		capacity: capacity,
	}, nil
}

// Lanes returns the number of lanes.
func (s *LaneScheduler) Lanes() int { return len(s.lanes) }

// Len returns the number of queued records (not in flight).
func (s *LaneScheduler) Len() int { return s.queued }

// Push queues rec behind every record pushed before it.
func (s *LaneScheduler) Push(rec Record) error {
	if s.queued >= s.capacity {
		return ErrLanesFull
	}
	s.queued++
	if s.barrierInFlight || s.blocked.len() > 0 || rec.Barrier {
		s.blocked.push(rec)
		return nil
	}
	s.lanes[LaneOf(rec.Key, len(s.lanes))].push(rec)
	return nil
}

// Next returns the next batch that may be sent now, or false when none can:
// every lane with queued records is busy, or a barrier is waiting for lanes
// to flush or is itself in flight.
func (s *LaneScheduler) Next(limits BatchLimits) (LaneBatch, bool) {
	if s.barrierInFlight {
		return LaneBatch{}, false
	}
	if s.blocked.len() > 0 && s.drained() {
		s.barrierInFlight = true
		s.queued--
		return LaneBatch{Lane: BarrierLane, Records: s.blocked.take(BatchLimits{MaxEvents: 1})}, true
	}
	n := len(s.lanes)
	for i := range n {
		lane := (s.next + i) % n
		if s.busy[lane] || s.lanes[lane].len() == 0 {
			continue
		}
		recs := s.lanes[lane].take(limits)
		s.busy[lane] = true
		s.queued -= len(recs)
		s.next = (lane + 1) % n
		return LaneBatch{Lane: lane, Records: recs}, true
	}
	return LaneBatch{}, false
}

// Complete releases the batch's lane (or the barrier) after the batch was
// delivered or dead-lettered. It returns ErrUnknownBatch when the batch is
// not in flight.
func (s *LaneScheduler) Complete(b LaneBatch) error {
	if b.Lane == BarrierLane {
		if !s.barrierInFlight {
			return fmt.Errorf("%w: barrier", ErrUnknownBatch)
		}
		s.barrierInFlight = false
		s.release()
		return nil
	}
	if b.Lane < 0 || b.Lane >= len(s.lanes) || !s.busy[b.Lane] {
		return fmt.Errorf("%w: lane %d", ErrUnknownBatch, b.Lane)
	}
	s.busy[b.Lane] = false
	return nil
}

// drained reports whether no keyed record is queued or in flight.
func (s *LaneScheduler) drained() bool {
	for i := range s.lanes {
		if s.busy[i] || s.lanes[i].len() > 0 {
			return false
		}
	}
	return true
}

// release moves the keyed records that follow a completed barrier into
// their lanes, up to the next barrier.
func (s *LaneScheduler) release() {
	for s.blocked.len() > 0 && !s.blocked.peek().Barrier {
		rec := s.blocked.pop()
		s.lanes[LaneOf(rec.Key, len(s.lanes))].push(rec)
	}
}

// recordFIFO is a queue of records reusing its backing array.
type recordFIFO struct {
	buf  []Record
	head int
}

func (q *recordFIFO) len() int { return len(q.buf) - q.head }

func (q *recordFIFO) push(r Record) { q.buf = append(q.buf, r) }

func (q *recordFIFO) peek() *Record { return &q.buf[q.head] }

func (q *recordFIFO) pop() Record {
	r := q.buf[q.head]
	q.buf[q.head] = Record{}
	q.head++
	q.compact()
	return r
}

// take removes up to limits records from the head (at least one).
func (q *recordFIFO) take(limits BatchLimits) []Record {
	n, size := 0, 0
	for q.head+n < len(q.buf) {
		if limits.MaxEvents > 0 && n >= limits.MaxEvents {
			break
		}
		sz := len(q.buf[q.head+n].Payload)
		if n > 0 && limits.MaxBytes > 0 && size+sz > limits.MaxBytes {
			break
		}
		size += sz
		n++
	}
	out := make([]Record, n)
	copy(out, q.buf[q.head:q.head+n])
	clear(q.buf[q.head : q.head+n])
	q.head += n
	q.compact()
	return out
}

func (q *recordFIFO) compact() {
	switch {
	case q.head == len(q.buf):
		q.buf, q.head = q.buf[:0], 0
	case q.head > cap(q.buf)/2:
		n := copy(q.buf, q.buf[q.head:])
		clear(q.buf[n:])
		q.buf, q.head = q.buf[:n], 0
	}
}
