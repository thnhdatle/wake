// SPDX-License-Identifier: Apache-2.0

package clock

import "time"

// Clock is the time source for timeouts, tickers and timestamps.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers ticks on C until stopped.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

// System is the real clock.
type System struct{}

// Now returns the current wall-clock time.
func (System) Now() time.Time { return time.Now() }

// After waits for d and then sends the current time.
func (System) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTicker returns a ticker with period d.
func (System) NewTicker(d time.Duration) Ticker { return systemTicker{time.NewTicker(d)} }

type systemTicker struct{ t *time.Ticker }

func (s systemTicker) C() <-chan time.Time   { return s.t.C }
func (s systemTicker) Stop()                 { s.t.Stop() }
func (s systemTicker) Reset(d time.Duration) { s.t.Reset(d) }
