// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"sync"
	"time"
)

// Fake is a manually advanced clock. Timers and tickers fire only from
// Advance or Set. It is safe for concurrent use.
type Fake struct {
	mu      sync.Mutex // protects now, waiters and changed
	now     time.Time
	waiters []*fakeWaiter
	changed chan struct{} // closed and replaced whenever waiters change
}

type fakeWaiter struct {
	at     time.Time
	period time.Duration // 0 for one-shot timers
	ch     chan time.Time
}

// NewFake returns a fake clock reading start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start, changed: make(chan struct{})}
}

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After returns a channel that receives the fake time once it reaches
// Now()+d. A non-positive d fires immediately.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if d <= 0 {
		ch <- f.now
		return ch
	}
	f.addLocked(&fakeWaiter{at: f.now.Add(d), ch: ch})
	return ch
}

// NewTicker returns a ticker firing every d of fake time. Like time.Ticker,
// it drops ticks when the receiver is slow. It panics if d <= 0.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker period")
	}
	w := &fakeWaiter{period: d, ch: make(chan time.Time, 1)}
	f.mu.Lock()
	defer f.mu.Unlock()
	w.at = f.now.Add(d)
	f.addLocked(w)
	return &fakeTicker{f: f, w: w}
}

// Advance moves the clock forward by d and fires every due timer and tick.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.setLocked(f.now.Add(d))
	f.mu.Unlock()
}

// Set moves the clock to t (never backwards) and fires what is due.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	if t.After(f.now) {
		f.setLocked(t)
	}
	f.mu.Unlock()
}

// Waiters returns the number of pending timers and tickers.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// WaiterChange returns a channel closed the next time a timer or ticker is
// added or removed, so tests can wait for a goroutine to block on the clock
// without sleeping.
func (f *Fake) WaiterChange() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

func (f *Fake) addLocked(w *fakeWaiter) {
	f.waiters = append(f.waiters, w)
	f.notifyLocked()
}

func (f *Fake) removeLocked(w *fakeWaiter) {
	for i, x := range f.waiters {
		if x == w {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			f.notifyLocked()
			return
		}
	}
}

func (f *Fake) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *Fake) setLocked(t time.Time) {
	f.now = t
	kept := f.waiters[:0]
	removed := false
	for _, w := range f.waiters {
		if w.at.After(t) {
			kept = append(kept, w)
			continue
		}
		select {
		case w.ch <- t:
		default:
		}
		if w.period > 0 {
			for !w.at.After(t) {
				w.at = w.at.Add(w.period)
			}
			kept = append(kept, w)
			continue
		}
		removed = true
	}
	clear(f.waiters[len(kept):])
	f.waiters = kept
	if removed {
		f.notifyLocked()
	}
}

type fakeTicker struct {
	f *Fake
	w *fakeWaiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }

func (t *fakeTicker) Stop() {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.f.removeLocked(t.w)
}

func (t *fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("clock: non-positive ticker period")
	}
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	t.f.removeLocked(t.w)
	t.w.period = d
	t.w.at = t.f.now.Add(d)
	t.f.addLocked(t.w)
}
