// SPDX-License-Identifier: Apache-2.0

package watermark

import "fmt"

// Progress tracks the snapshot progress of one table: a chunk is
// complete when all its r events are acknowledged, and progress is the upper
// key of the latest chunk that is complete together with all chunks emitted
// before it. K is the position type (for example the key as JSON, or a ctid
// range position). The zero value has no progress; ResumeProgress starts
// from a persisted position. Progress is not safe for concurrent use.
type Progress[K any] struct {
	pending []pendingChunk[K]
	// last is the highest chunk registered; registered reports whether any
	// chunk was. done is the highest chunk committed; anyDone whether any was.
	last       uint64
	registered bool
	done       uint64
	anyDone    bool
	committed  K
	has        bool
}

type pendingChunk[K any] struct {
	chunk uint64
	upper K
	done  bool
}

// ResumeProgress returns a Progress whose committed position is upper, the
// position persisted before a restart.
func ResumeProgress[K any](upper K) *Progress[K] {
	return &Progress[K]{committed: upper, has: true}
}

// Register records an emitted chunk with its upper key. Chunks are
// registered in emission order with strictly increasing counters; gaps are
// allowed (a retried chunk consumes a counter and is never registered).
func (p *Progress[K]) Register(chunk uint64, upper K) error {
	if p.registered && chunk <= p.last {
		return fmt.Errorf("%w: chunk %d after %d", ErrChunkOrder, chunk, p.last)
	}
	p.pending = append(p.pending, pendingChunk[K]{chunk: chunk, upper: upper})
	p.last, p.registered = chunk, true
	return nil
}

// Complete marks a registered chunk complete; chunks may complete in any
// order. Completing a chunk at or below the latest committed one is a no-op
// (acknowledgements may repeat); completing a chunk that is neither pending
// nor committed fails with ErrUnknownChunk.
func (p *Progress[K]) Complete(chunk uint64) error {
	for i := range p.pending {
		if p.pending[i].chunk == chunk {
			p.pending[i].done = true
			p.advance()
			return nil
		}
	}
	if p.anyDone && chunk <= p.done {
		return nil
	}
	return fmt.Errorf("%w: chunk %d", ErrUnknownChunk, chunk)
}

// advance moves the committed position over the completed prefix.
func (p *Progress[K]) advance() {
	n := 0
	for n < len(p.pending) && p.pending[n].done {
		p.committed, p.has = p.pending[n].upper, true
		p.done, p.anyDone = p.pending[n].chunk, true
		n++
	}
	if n > 0 {
		clear(p.pending[:n])
		p.pending = p.pending[n:]
	}
}

// Committed returns the upper key of the latest chunk complete together
// with all earlier chunks, or the resumed position; ok is false when there
// is neither.
func (p *Progress[K]) Committed() (upper K, ok bool) {
	return p.committed, p.has
}

// Pending returns the number of registered chunks not yet committed.
func (p *Progress[K]) Pending() int { return len(p.pending) }
