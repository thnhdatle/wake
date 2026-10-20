// SPDX-License-Identifier: Apache-2.0

package pipeline

import "time"

// Default retry delays of sinks.
const (
	DefaultBackoffInitial = 100 * time.Millisecond
	DefaultBackoffMax     = 30 * time.Second
)

// Backoff computes capped, jittered exponential retry delays. Sinks retry
// every error without an attempt limit (backpressure instead of
// dropping, with lag.max_ack_age alerting as the time budget), so only the
// delay is bounded.
type Backoff struct {
	// Initial is the base delay of the first retry; non-positive means
	// DefaultBackoffInitial.
	Initial time.Duration
	// Max caps the delay; non-positive means DefaultBackoffMax.
	Max time.Duration
}

// Delay returns the wait before retry attempt (0 for the first retry). r is
// a uniform random number in [0, 1) supplied by the caller's seeded source;
// the delay is d/2 + r·d/2 with d = min(Max, Initial·2^attempt) ("equal
// jitter"), so it never drops below half the exponential step.
func (b Backoff) Delay(attempt int, r float64) time.Duration {
	initial, ceiling := b.Initial, b.Max
	if initial <= 0 {
		initial = DefaultBackoffInitial
	}
	if ceiling <= 0 {
		ceiling = DefaultBackoffMax
	}
	initial = min(initial, ceiling)
	d := initial
	for i := 0; i < attempt && d < ceiling; i++ {
		d *= 2
	}
	d = min(d, ceiling)
	r = min(max(r, 0), 1)
	half := d / 2
	return half + time.Duration(r*float64(d-half))
}
