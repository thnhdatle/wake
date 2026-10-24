// SPDX-License-Identifier: Apache-2.0

package pipeline

import "fmt"

const DefaultLanes = 8

const BarrierLane = -1

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

type BatchLimits struct {
	MaxEvents int
	MaxBytes  int
}

type LaneBatch struct {
	Lane    int
	Records []Record
}

// LaneScheduler queues records into lanes and hands out batches per lane.
type LaneScheduler struct {
	lanes           []recordFIFO
	busy            []bool
	queued          int
	capacity        int
	blocked         recordFIFO
	barrierInFlight bool
	next            int
}

// NewLaneScheduler creates a scheduler with n lanes and the given capacity.
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

func (s *LaneScheduler) Lanes() int { return len(s.lanes) }

func (s *LaneScheduler) Len() int { return s.queued }

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

func (s *LaneScheduler) drained() bool {
	for i := range s.lanes {
		if s.busy[i] || s.lanes[i].len() > 0 {
			return false
		}
	}
	return true
}

func (s *LaneScheduler) release() {
	for s.blocked.len() > 0 && !s.blocked.peek().Barrier {
		rec := s.blocked.pop()
		s.lanes[LaneOf(rec.Key, len(s.lanes))].push(rec)
	}
}

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
