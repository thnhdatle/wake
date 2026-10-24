// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/redisl2"
)

// Option configures New.
type Option func(*config) error

type config struct {
	l1Entries   int
	l1Bytes     int64
	ttl         time.Duration
	jitter      time.Duration
	negTTL      time.Duration
	maxStale    time.Duration
	channel     string
	lockTimeout time.Duration
	degradedTTL time.Duration
	maxPending  int
	compressMin int
	registerer  prometheus.Registerer
	clock       Clock
	backend     func(l2.Config) (l2.Backend, error)
}

const (
	defaultL1Entries   = 10_000
	defaultL1Bytes     = 64 << 20
	defaultTTL         = 5 * time.Minute
	defaultLockTimeout = 5 * time.Second
	defaultDegradedTTL = 5 * time.Second
	defaultMaxPending  = 10_000
	genGrace           = time.Minute
)

func defaultConfig(namespace string) config {
	return config{
		l1Entries:   defaultL1Entries,
		l1Bytes:     defaultL1Bytes,
		ttl:         defaultTTL,
		channel:     l2.DefaultChannel(namespace),
		lockTimeout: defaultLockTimeout,
		degradedTTL: defaultDegradedTTL,
		maxPending:  defaultMaxPending,
		clock:       systemClock{},
	}
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), errInvalidConfig)
}

// WithL1 bounds the in-process tier by entries and encoded bytes.
func WithL1(maxEntries int, maxBytes int64) Option {
	return func(c *config) error {
		if maxEntries <= 0 || maxBytes <= 0 {
			return invalid("l1 bounds %d entries, %d bytes", maxEntries, maxBytes)
		}
		c.l1Entries, c.l1Bytes = maxEntries, maxBytes
		return nil
	}
}

// WithTTL sets the value TTL and its jitter.
func WithTTL(ttl, jitter time.Duration) Option {
	return func(c *config) error {
		if ttl <= 0 || jitter < 0 || jitter >= ttl {
			return invalid("ttl %s, jitter %s", ttl, jitter)
		}
		c.ttl, c.jitter = ttl, jitter
		return nil
	}
}

// WithNegativeTTL caches a loader's ErrNotFound for ttl.
func WithNegativeTTL(ttl time.Duration) Option {
	return func(c *config) error {
		if ttl <= 0 {
			return invalid("negative ttl %s", ttl)
		}
		c.negTTL = ttl
		return nil
	}
}

// WithStaleIfError lets GetOrLoad return an expired L1 value when the loader fails.
func WithStaleIfError(maxStale time.Duration) Option {
	return func(c *config) error {
		if maxStale <= 0 {
			return invalid("max stale %s", maxStale)
		}
		c.maxStale = maxStale
		return nil
	}
}

// WithInvalidationBus sets the L2 pub/sub channel.
func WithInvalidationBus(channel string) Option {
	return func(c *config) error {
		if channel == "" {
			return invalid("empty invalidation bus channel")
		}
		c.channel = channel
		return nil
	}
}

// WithLockTimeout sets the load lock TTL and the wait for the lock holder.
func WithLockTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return invalid("lock timeout %s", d)
		}
		c.lockTimeout = d
		return nil
	}
}

// WithDegradedL1TTL caps the age of L1 values while L2 or the bus is unreachable.
func WithDegradedL1TTL(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return invalid("degraded l1 ttl %s", d)
		}
		c.degradedTTL = d
		return nil
	}
}

// WithMaxPendingInvalidations bounds the keys whose invalidation is deferred while L2 is down.
func WithMaxPendingInvalidations(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return invalid("max pending invalidations %d", n)
		}
		c.maxPending = n
		return nil
	}
}

// WithCompression compresses L2 payloads of at least minBytes with zstd.
func WithCompression(minBytes int) Option {
	return func(c *config) error {
		if minBytes <= 0 {
			return invalid("compression threshold %d", minBytes)
		}
		c.compressMin = minBytes
		return nil
	}
}

// WithMetrics registers the wake_cache_* metrics on reg.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(c *config) error {
		if reg == nil {
			return invalid("nil metrics registerer")
		}
		c.registerer = reg
		return nil
	}
}

// WithClock sets the time source.
func WithClock(clk Clock) Option {
	return func(c *config) error {
		if clk == nil {
			return invalid("nil clock")
		}
		c.clock = clk
		return nil
	}
}

// WithL2 adds the shared Redis tier.
func WithL2(client redis.UniversalClient) Option {
	return func(c *config) error {
		if client == nil {
			return invalid("nil redis client")
		}
		c.backend = func(cfg l2.Config) (l2.Backend, error) {
			b, err := redisl2.New(client, cfg)
			if err != nil {
				return nil, fmt.Errorf("redis l2: %w", err)
			}
			return b, nil
		}
		return nil
	}
}

func withBackend(factory func(l2.Config) (l2.Backend, error)) Option {
	return func(c *config) error {
		if factory == nil {
			return invalid("nil l2 backend factory")
		}
		c.backend = factory
		return nil
	}
}
