// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"sync/atomic"
	"time"
)

const (
	breakerThreshold = 5
	probeInterval    = time.Second
)

// breaker is the L2 circuit breaker: it opens after breakerThreshold
// consecutive failures; while it is not closed no caller touches L2 and the
// maintenance goroutine probes every probeInterval (half-open).
type breaker struct {
	state    atomic.Int32
	failures atomic.Int32
}

func (b *breaker) allow() bool { return b.state.Load() == breakerClosed }

func (b *breaker) success() {
	if b.failures.Load() != 0 {
		b.failures.Store(0)
	}
}

// failure counts a failure and reports whether it opened the breaker.
func (b *breaker) failure() bool {
	if b.failures.Add(1) < breakerThreshold {
		return false
	}
	return b.state.CompareAndSwap(breakerClosed, breakerOpen)
}

func (b *breaker) halfOpen() bool { return b.state.CompareAndSwap(breakerOpen, breakerHalfOpen) }

func (b *breaker) reopen() { b.state.Store(breakerOpen) }

func (b *breaker) close() {
	b.failures.Store(0)
	b.state.Store(breakerClosed)
}
