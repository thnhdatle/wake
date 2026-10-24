// SPDX-License-Identifier: Apache-2.0

package l1

import (
	"fmt"
	"time"
)

// Clock is the time source for expiry.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Option configures New.
type Option func(*config) error

type config struct {
	shards     int
	clock      Clock
	jitter     time.Duration
	jitterSeed uint64
}

// WithShards sets the shard count, rounded up to a power of two.
func WithShards(n int) Option {
	return func(c *config) error {
		if n <= 0 || n > 1<<16 {
			return fmt.Errorf("shards %d: %w", n, ErrInvalidConfig)
		}
		c.shards = n
		return nil
	}
}

// WithClock sets the time source.
func WithClock(clk Clock) Option {
	return func(c *config) error {
		if clk == nil {
			return fmt.Errorf("nil clock: %w", ErrInvalidConfig)
		}
		c.clock = clk
		return nil
	}
}

// WithJitter shortens positive TTLs by a random amount to spread expirations.
func WithJitter(jitter time.Duration, seed uint64) Option {
	return func(c *config) error {
		if jitter < 0 {
			return fmt.Errorf("jitter %s: %w", jitter, ErrInvalidConfig)
		}
		c.jitter = jitter
		c.jitterSeed = seed
		return nil
	}
}
