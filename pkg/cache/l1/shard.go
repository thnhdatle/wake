// SPDX-License-Identifier: Apache-2.0

package l1

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

// shard is one LRU partition with its own lock and invalidation counter.
type shard[V any] struct {
	mu         sync.Mutex // protects items, the list links, bytes and rng
	items      map[string]*node[V]
	head       node[V] // sentinel: head.next is most, head.prev least recently used
	bytes      int64
	maxEntries int
	maxBytes   int64
	rng        *rand.Rand
	// inval counts invalidations; written under mu, read lock-free by Token.
	inval atomic.Uint64
	_     [64]byte // keeps neighboring shards on separate cache lines
}

type node[V any] struct {
	key       string
	value     V
	size      int64
	version   uint64
	storedAt  int64 // unix nanoseconds
	expiresAt int64 // unix nanoseconds; 0 never expires
	prev      *node[V]
	next      *node[V]
}

func (s *shard[V]) init(maxEntries int, maxBytes int64, rng *rand.Rand) {
	s.items = make(map[string]*node[V])
	s.head.next = &s.head
	s.head.prev = &s.head
	s.maxEntries = maxEntries
	s.maxBytes = maxBytes
	s.rng = rng
}

// lookupLocked returns the node of key and moves it to the front.
func (s *shard[V]) lookupLocked(key string) *node[V] {
	n := s.items[key]
	if n == nil {
		return nil
	}
	if s.head.next != n {
		s.unlinkLocked(n)
		s.pushFrontLocked(n)
	}
	return n
}

func (s *shard[V]) unlinkLocked(n *node[V]) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev, n.next = nil, nil
}

func (s *shard[V]) pushFrontLocked(n *node[V]) {
	n.prev = &s.head
	n.next = s.head.next
	s.head.next.prev = n
	s.head.next = n
}

func (s *shard[V]) removeLocked(n *node[V]) {
	s.unlinkLocked(n)
	delete(s.items, n.key)
	s.bytes -= n.size
}

// invalidateLocked counts one invalidation.
func (s *shard[V]) invalidateLocked() { s.inval.Add(1) }

// insertLocked stores n unless an entry with a higher version exists or n
// cannot fit; an existing entry with a version not above n's is removed
// either way.
func (s *shard[V]) insertLocked(n *node[V]) bool {
	if old := s.items[n.key]; old != nil {
		if old.version > n.version {
			return false
		}
		s.removeLocked(old)
	}
	if n.size > s.maxBytes {
		return false
	}
	s.items[n.key] = n
	s.pushFrontLocked(n)
	s.bytes += n.size
	for len(s.items) > s.maxEntries || s.bytes > s.maxBytes {
		s.removeLocked(s.head.prev)
	}
	return true
}

func (s *shard[V]) clearLocked() {
	clear(s.items)
	s.head.next = &s.head
	s.head.prev = &s.head
	s.bytes = 0
}
