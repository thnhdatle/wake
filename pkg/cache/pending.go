// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync"
	"sync/atomic"
)

// pendingSet holds keys whose L2 invalidation is deferred. It is bounded:
// beyond max keys it switches to overflow, which the maintenance goroutine
// resolves with an epoch increment. Every add gets a sequence number so a
// replay removes only keys (and an overflow) recorded before it started.
type pendingSet struct {
	max int

	mu          sync.Mutex // protects keys, seqN and overflowSeq
	keys        map[string]uint64
	seqN        uint64
	overflowSeq uint64 // seq of the latest add while overflowed; 0: none
	// active is true while keys is non-empty or overflowed; read lock-free
	// on the L2 read path.
	active atomic.Bool
}

func newPendingSet(maxKeys int) *pendingSet {
	return &pendingSet{max: maxKeys, keys: make(map[string]uint64)}
}

// add records key and reports whether this add overflowed the bound.
func (p *pendingSet) add(key string) (overflowed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seqN++
	p.active.Store(true)
	if p.overflowSeq != 0 {
		p.overflowSeq = p.seqN
		return false
	}
	if _, ok := p.keys[key]; ok || len(p.keys) < p.max {
		p.keys[key] = p.seqN
		return false
	}
	p.overflowSeq = p.seqN
	clear(p.keys)
	return true
}

// blocks reports whether L2 must not be read for key (its invalidation, or
// possibly every invalidation, is still pending).
func (p *pendingSet) blocks(key string) bool {
	if !p.active.Load() {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.keys[key]
	return ok || p.overflowSeq != 0
}

func (p *pendingSet) seq() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seqN
}

// snapshot returns the pending keys, whether the set overflowed and the
// sequence number the snapshot covers.
func (p *pendingSet) snapshot() (keys []string, overflow bool, seq uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	keys = make([]string, 0, len(p.keys))
	for k := range p.keys {
		keys = append(keys, k)
	}
	return keys, p.overflowSeq != 0, p.seqN
}

// removeUpTo drops key if it was last added at or before seq.
func (p *pendingSet) removeUpTo(key string, seq uint64) {
	if !p.active.Load() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.keys[key]; ok && s <= seq {
		delete(p.keys, key)
	}
	p.updateActiveLocked()
}

// resolveUpTo drops everything an epoch increment that started after seq
// covers.
func (p *pendingSet) resolveUpTo(seq uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.overflowSeq != 0 && p.overflowSeq <= seq {
		p.overflowSeq = 0
	}
	for k, s := range p.keys {
		if s <= seq {
			delete(p.keys, k)
		}
	}
	p.updateActiveLocked()
}

func (p *pendingSet) updateActiveLocked() {
	p.active.Store(len(p.keys) > 0 || p.overflowSeq != 0)
}

func (p *pendingSet) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.keys)
}
