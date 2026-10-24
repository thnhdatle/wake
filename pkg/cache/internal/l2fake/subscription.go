// SPDX-License-Identifier: Apache-2.0

package l2fake

import (
	"context"
	"sync"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

type subscription struct {
	channel string
	owner   *Client

	mu        sync.Mutex
	queue     []item
	connected bool
	paused    bool
	waiting   bool
	closed    bool
	notify    chan struct{}
	changed   chan struct{}
}

type item struct {
	kind l2.EventKind
	msg  string
}

func newSubscription(channel string) *subscription {
	return &subscription{
		channel: channel,
		notify:  make(chan struct{}, 1),
		changed: make(chan struct{}),
	}
}

func (s *subscription) setConnected(up bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || up == s.connected {
		return
	}
	s.connected = up
	if up {
		s.queue = append(s.queue, item{kind: l2.EventConnected})
	} else {
		s.queue = append(s.queue, item{kind: l2.EventDisconnected})
	}
	s.signalLocked()
}

func (s *subscription) deliver(msg string, limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.connected {
		return
	}
	if len(s.queue) >= limit {
		s.queue = append(s.queue[:0], item{kind: l2.EventDisconnected}, item{kind: l2.EventConnected})
		s.signalLocked()
		return
	}
	s.queue = append(s.queue, item{msg: msg})
	s.signalLocked()
}

func (s *subscription) setPaused(p bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = p
	s.signalLocked()
}

func (s *subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.queue = nil
	s.signalLocked()
}

func (s *subscription) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *subscription) receive(ctx context.Context) (l2.Event, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return l2.Event{}, l2.ErrClosed
		}
		if !s.paused && len(s.queue) > 0 {
			it := s.queue[0]
			s.queue = s.queue[1:]
			s.waiting = false
			s.signalLocked()
			s.mu.Unlock()
			if it.kind != 0 {
				return l2.Event{Kind: it.kind}, nil
			}
			e, err := l2.DecodeMessage(it.msg)
			if err != nil {
				continue
			}
			return e, nil
		}
		if !s.waiting {
			s.waiting = true
			s.signalLocked()
		}
		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-ctx.Done():
			s.mu.Lock()
			s.waiting = false
			s.signalLocked()
			s.mu.Unlock()
			return l2.Event{}, ctx.Err()
		}
	}
}

func (s *subscription) waitIdle(ctx context.Context) error {
	for {
		s.mu.Lock()
		idle := s.closed || (s.waiting && len(s.queue) == 0)
		ch := s.changed
		s.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
