// SPDX-License-Identifier: Apache-2.0

package fakerepl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

var ErrClosed = errors.New("fakerepl: closed")

// Source is an in-memory replication source.
type Source struct {
	msgs   chan pipeline.ReplicationMessage
	closed chan struct{}
	once   sync.Once

	mu       sync.Mutex
	system   pipeline.SystemInfo
	slot     pipeline.SlotInfo
	slotErr  error
	created  []string
	dropped  []string
	started  []pipeline.StartOptions
	statuses []pipeline.StandbyStatus
	sendErr  error
	changed  chan struct{}
}

// New returns a source whose message queue holds up to capacity messages.
func New(capacity int) *Source {
	return &Source{
		msgs:    make(chan pipeline.ReplicationMessage, max(capacity, 1)),
		closed:  make(chan struct{}),
		changed: make(chan struct{}),
	}
}

func (s *Source) SetSystem(info pipeline.SystemInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.system = info
}

func (s *Source) SetSlot(info pipeline.SlotInfo, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slot, s.slotErr = info, err
}

func (s *Source) IdentifySystem(_ context.Context) (pipeline.SystemInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.system, nil
}

func (s *Source) CreateSlot(_ context.Context, name string) (pipeline.SlotInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.slotErr != nil {
		return pipeline.SlotInfo{}, s.slotErr
	}
	s.created = append(s.created, name)
	info := s.slot
	info.Name = name
	return info, nil
}

func (s *Source) DropSlot(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped = append(s.dropped, name)
	return nil
}

func (s *Source) Slots() (created, dropped []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.created), slices.Clone(s.dropped)
}

func (s *Source) StartReplication(_ context.Context, opts pipeline.StartOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = append(s.started, opts)
	return nil
}

func (s *Source) Started() []pipeline.StartOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.started)
}

func (s *Source) Push(ctx context.Context, msg pipeline.ReplicationMessage) error {
	select {
	case s.msgs <- msg:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("fakerepl: push: %w", ctx.Err())
	}
}

func (s *Source) Close() { s.once.Do(func() { close(s.closed) }) }

func (s *Source) Receive(ctx context.Context) (pipeline.ReplicationMessage, error) {
	select {
	case m := <-s.msgs:
		return m, nil
	default:
	}
	select {
	case m := <-s.msgs:
		return m, nil
	case <-s.closed:
		return pipeline.ReplicationMessage{}, ErrClosed
	case <-ctx.Done():
		return pipeline.ReplicationMessage{}, fmt.Errorf("fakerepl: receive: %w", ctx.Err())
	}
}

func (s *Source) SendStandbyStatus(_ context.Context, st pipeline.StandbyStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.statuses = append(s.statuses, st)
	close(s.changed)
	s.changed = make(chan struct{})
	return nil
}

func (s *Source) FailStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

func (s *Source) Statuses() []pipeline.StandbyStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.statuses)
}

func (s *Source) WaitStatuses(ctx context.Context, n int) ([]pipeline.StandbyStatus, error) {
	for {
		s.mu.Lock()
		got, ch := slices.Clone(s.statuses), s.changed
		s.mu.Unlock()
		if len(got) >= n {
			return got, nil
		}
		select {
		case <-ctx.Done():
			return got, fmt.Errorf("fakerepl: wait for %d statuses, have %d: %w", n, len(got), ctx.Err())
		case <-ch:
		}
	}
}
