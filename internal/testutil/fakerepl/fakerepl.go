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

// ErrClosed is returned by Receive once Close was called and every scripted
// message was received.
var ErrClosed = errors.New("fakerepl: closed")

// Source is the fake. It is safe for concurrent use; SendStandbyStatus may
// run while another goroutine is blocked in Receive, as the port requires.
type Source struct {
	msgs   chan pipeline.ReplicationMessage
	closed chan struct{}
	once   sync.Once

	mu       sync.Mutex // protects every field below
	system   pipeline.SystemInfo
	slot     pipeline.SlotInfo
	slotErr  error
	created  []string
	dropped  []string
	started  []pipeline.StartOptions
	statuses []pipeline.StandbyStatus
	sendErr  error
	changed  chan struct{} // closed and replaced on every status sent
}

// New returns a source whose scripted message queue holds up to capacity
// messages (Push blocks beyond that); values below 1 mean 1.
func New(capacity int) *Source {
	return &Source{
		msgs:    make(chan pipeline.ReplicationMessage, max(capacity, 1)),
		closed:  make(chan struct{}),
		changed: make(chan struct{}),
	}
}

// SetSystem sets the IdentifySystem result.
func (s *Source) SetSystem(info pipeline.SystemInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.system = info
}

// SetSlot sets the CreateSlot result (its Name is replaced by the requested
// name) and error.
func (s *Source) SetSlot(info pipeline.SlotInfo, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slot, s.slotErr = info, err
}

// IdentifySystem implements pipeline.SlotManager.
func (s *Source) IdentifySystem(_ context.Context) (pipeline.SystemInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.system, nil
}

// CreateSlot implements pipeline.SlotManager.
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

// DropSlot implements pipeline.SlotManager.
func (s *Source) DropSlot(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped = append(s.dropped, name)
	return nil
}

// Slots returns the names passed to CreateSlot and DropSlot.
func (s *Source) Slots() (created, dropped []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.created), slices.Clone(s.dropped)
}

// StartReplication implements pipeline.ReplicationSource.
func (s *Source) StartReplication(_ context.Context, opts pipeline.StartOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = append(s.started, opts)
	return nil
}

// Started returns the StartReplication calls.
func (s *Source) Started() []pipeline.StartOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.started)
}

// Push scripts the next message, blocking while the queue is full.
func (s *Source) Push(ctx context.Context, msg pipeline.ReplicationMessage) error {
	select {
	case s.msgs <- msg:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("fakerepl: push: %w", ctx.Err())
	}
}

// Close ends the script: Receive returns ErrClosed once the queue is empty.
func (s *Source) Close() { s.once.Do(func() { close(s.closed) }) }

// Receive implements pipeline.ReplicationSource.
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

// SendStandbyStatus implements pipeline.StandbyStatusSender.
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

// FailStatus makes SendStandbyStatus return err; nil restores it.
func (s *Source) FailStatus(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendErr = err
}

// Statuses returns the status updates sent, in order.
func (s *Source) Statuses() []pipeline.StandbyStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.statuses)
}

// WaitStatuses blocks until at least n status updates were sent and returns
// them.
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
