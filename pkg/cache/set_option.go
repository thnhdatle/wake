// SPDX-License-Identifier: Apache-2.0

package cache

import "time"

// SetOption configures one Set call.
type SetOption func(*setConfig)

type setConfig struct {
	ttl time.Duration
}

// WithSetTTL overrides WithTTL for this value; the TTL is used as given,
// without jitter. A non-positive ttl makes Set fail.
func WithSetTTL(ttl time.Duration) SetOption {
	return func(c *setConfig) {
		c.ttl = ttl
		if ttl <= 0 {
			c.ttl = -1
		}
	}
}
