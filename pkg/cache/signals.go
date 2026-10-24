// SPDX-License-Identifier: Apache-2.0

package cache

import "sync"

type loadSignals struct {
	mu sync.Mutex
	m  map[string]*signalSlot
}

type signalSlot struct {
	ch   chan struct{}
	refs int
}

func (s *loadSignals) watch(key string) (ch <-chan struct{}, release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]*signalSlot)
	}
	slot := s.m[key]
	if slot == nil {
		slot = &signalSlot{ch: make(chan struct{})}
		s.m[key] = slot
	}
	slot.refs++
	return slot.ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		slot.refs--
		if slot.refs == 0 && s.m[key] == slot {
			delete(s.m, key)
		}
	}
}

func (s *loadSignals) notify(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot := s.m[key]; slot != nil {
		close(slot.ch)
		delete(s.m, key)
	}
}
