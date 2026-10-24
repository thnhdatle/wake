// SPDX-License-Identifier: Apache-2.0

package clock

import "time"

// Clock is the time source for timeouts, tickers and timestamps.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

type System struct{}

func (System) Now() time.Time { return time.Now() }

func (System) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (System) NewTicker(d time.Duration) Ticker { return systemTicker{time.NewTicker(d)} }

type systemTicker struct{ t *time.Ticker }

func (s systemTicker) C() <-chan time.Time   { return s.t.C }
func (s systemTicker) Stop()                 { s.t.Stop() }
func (s systemTicker) Reset(d time.Duration) { s.t.Reset(d) }
