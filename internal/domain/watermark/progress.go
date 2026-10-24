// SPDX-License-Identifier: Apache-2.0

package watermark

import "fmt"

type Progress[K any] struct {
	pending    []pendingChunk[K]
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

func ResumeProgress[K any](upper K) *Progress[K] {
	return &Progress[K]{committed: upper, has: true}
}

func (p *Progress[K]) Register(chunk uint64, upper K) error {
	if p.registered && chunk <= p.last {
		return fmt.Errorf("%w: chunk %d after %d", ErrChunkOrder, chunk, p.last)
	}
	p.pending = append(p.pending, pendingChunk[K]{chunk: chunk, upper: upper})
	p.last, p.registered = chunk, true
	return nil
}

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

func (p *Progress[K]) Committed() (upper K, ok bool) {
	return p.committed, p.has
}

func (p *Progress[K]) Pending() int { return len(p.pending) }
