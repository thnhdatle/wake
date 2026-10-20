// SPDX-License-Identifier: Apache-2.0

package fakeemitter

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// Step is the LSN distance between consecutive emitted messages.
const Step = 0x100

// Message is one emitted message.
type Message struct {
	Prefix  string
	Content []byte
	LSN     lsn.LSN
}

// Emitter is the fake. It is safe for concurrent use.
type Emitter struct {
	mu       sync.Mutex // protects every field below
	next     lsn.LSN
	err      error
	messages []Message
	attempts int
	changed  chan struct{} // closed and replaced on every attempt
}

// New returns an emitter whose first message gets LSN start+Step.
func New(start lsn.LSN) *Emitter {
	return &Emitter{next: start, changed: make(chan struct{})}
}

// EmitMessage counts the attempt; it returns the context's error when ctx
// is done, the injected error if any, and otherwise records the message
// (with a copy of content) and returns its LSN.
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

// Fail makes EmitMessage return err; nil restores it.
func (e *Emitter) Fail(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.err = err
}

// Messages returns the recorded messages in order.
func (e *Emitter) Messages() []Message {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.messages)
}

// Attempts returns the number of EmitMessage calls, failed ones included.
func (e *Emitter) Attempts() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts
}

// WaitAttempts waits until EmitMessage was called at least n times and
// returns the count, or the count so far and the context's error.
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
