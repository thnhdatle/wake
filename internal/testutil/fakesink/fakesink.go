// SPDX-License-Identifier: Apache-2.0

package fakesink

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const DefaultCapacity = 1024

var ErrNoPending = errors.New("fakesink: no such pending record")

type Option func(*Sink)

func WithCapacity(n int) Option {
	return func(s *Sink) { s.capacity = max(n, 1) }
}

func WithAutoAck() Option {
	return func(s *Sink) { s.autoAck = true }
}

// Sink is an in-memory sink.
type Sink struct {
	name     string
	capacity int
	autoAck  bool

	mu       sync.Mutex
	received []pipeline.Record
	pending  []pipeline.Record
	health   pipeline.SinkHealth
	sendErr  error
	changed  chan struct{}
}

// New returns an empty healthy sink.
func New(name string, opts ...Option) *Sink {
	s := &Sink{name: name, capacity: DefaultCapacity, changed: make(chan struct{})}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *Sink) Name() string { return s.name }

func (s *Sink) Send(ctx context.Context, rec pipeline.Record) error {
	for {
		s.mu.Lock()
		if s.sendErr != nil {
			err := s.sendErr
			s.mu.Unlock()
			return err
		}
		if len(s.pending) < s.capacity {
			s.received = append(s.received, rec)
			if !s.autoAck {
				s.pending = append(s.pending, rec)
			}
			s.notifyLocked()
			s.mu.Unlock()
			if s.autoAck {
				return rec.Ack.Ack()
			}
			return nil
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return fmt.Errorf("fakesink: send: %w", ctx.Err())
		case <-ch:
		}
	}
}

func (s *Sink) Health() pipeline.SinkHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health
}

func (s *Sink) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (s *Sink) SetHealth(h pipeline.SinkHealth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.health = h
}

func (s *Sink) FailSends(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

func (s *Sink) Received() []pipeline.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.received)
}

func (s *Sink) Pending() []pipeline.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.pending)
}

func (s *Sink) AckNext(n int) (int, error) {
	s.mu.Lock()
	n = min(max(n, 0), len(s.pending))
	recs := slices.Clone(s.pending[:n])
	s.pending = slices.Delete(s.pending, 0, n)
	s.notifyLocked()
	s.mu.Unlock()
	return n, ackAll(recs)
}

func (s *Sink) AckAt(i int) error {
	s.mu.Lock()
	if i < 0 || i >= len(s.pending) {
		s.mu.Unlock()
		return fmt.Errorf("%w: index %d", ErrNoPending, i)
	}
	rec := s.pending[i]
	s.pending = slices.Delete(s.pending, i, i+1)
	s.notifyLocked()
	s.mu.Unlock()
	return rec.Ack.Ack()
}

func (s *Sink) AckAll() error {
	s.mu.Lock()
	n := len(s.pending)
	s.mu.Unlock()
	_, err := s.AckNext(n)
	return err
}

func (s *Sink) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

func (s *Sink) WaitReceived(ctx context.Context, n int) error {
	for {
		s.mu.Lock()
		got, ch := len(s.received), s.changed
		s.mu.Unlock()
		if got >= n {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("fakesink: wait for %d records, have %d: %w", n, got, ctx.Err())
		case <-ch:
		}
	}
}

func (s *Sink) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func ackAll(recs []pipeline.Record) error {
	var errs []error
	for i := range recs {
		if err := recs[i].Ack.Ack(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
