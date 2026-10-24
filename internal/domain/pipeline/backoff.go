// SPDX-License-Identifier: Apache-2.0

package pipeline

import "time"

const (
	DefaultBackoffInitial = 100 * time.Millisecond
	DefaultBackoffMax     = 30 * time.Second
)

type Backoff struct {
	Initial time.Duration
	Max     time.Duration
}

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
