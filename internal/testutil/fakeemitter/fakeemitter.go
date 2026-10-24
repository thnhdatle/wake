// SPDX-License-Identifier: Apache-2.0

package fakeemitter

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

const Step = 0x100

type Message struct {
	Prefix  string
	Content []byte
	LSN     lsn.LSN
}

// Emitter is an in-memory message emitter.
type Emitter struct {
	mu       sync.Mutex
	next     lsn.LSN
	err      error
	messages []Message
	attempts int
	changed  chan struct{}
}

// New returns an emitter whose first message gets LSN start+Step.
func New(start lsn.LSN) *Emitter {
	return &Emitter{next: start, changed: make(chan struct{})}
}

func (e *Emitter) EmitMessage(ctx context.Context, prefix string, content []byte) (lsn.LSN, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempts++
	close(e.changed)
	e.changed = make(chan struct{})
	if err := ctx.Err(); err != nil {
		return lsn.InvalidLSN, err
	}
	if e.err != nil {
		return lsn.InvalidLSN, e.err
	}
	e.next += Step
	e.messages = append(e.messages, Message{Prefix: prefix, Content: slices.Clone(content), LSN: e.next})
	return e.next, nil
}

func (e *Emitter) Fail(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.err = err
}

func (e *Emitter) Messages() []Message {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.messages)
}

func (e *Emitter) Attempts() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts
}

func (e *Emitter) WaitAttempts(ctx context.Context, n int) (int, error) {
	for {
		e.mu.Lock()
		got, ch := e.attempts, e.changed
		e.mu.Unlock()
		if got >= n {
			return got, nil
		}
		select {
		case <-ctx.Done():
			return got, fmt.Errorf("fakeemitter: wait for %d attempts, have %d: %w", n, got, ctx.Err())
		case <-ch:
		}
	}
}
