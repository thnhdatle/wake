// SPDX-License-Identifier: Apache-2.0

package l1

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

type shard[V any] struct {
	mu         sync.Mutex
	items      map[string]*node[V]
	head       node[V]
	bytes      int64
	maxEntries int
	maxBytes   int64
	rng        *rand.Rand
	inval      atomic.Uint64
	_          [64]byte
}

type node[V any] struct {
	key       string
	value     V
	size      int64
	version   uint64
	storedAt  int64
	expiresAt int64
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

func (s *shard[V]) invalidateLocked() { s.inval.Add(1) }

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
