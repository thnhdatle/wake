// SPDX-License-Identifier: Apache-2.0

package cache

import "time"

// Clock is the time source for TTLs and waits.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
