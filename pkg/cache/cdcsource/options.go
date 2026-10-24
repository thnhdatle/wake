// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thnhdatle/wake/pkg/cache"
)

// Defaults of the options.
const (
	DefaultRetryMin      = 100 * time.Millisecond
	DefaultRetryMax      = 5 * time.Second
	DefaultDeleteTimeout = 30 * time.Second
	maxKeysPerDelete     = 256
)

// Option configures New.
type Option func(*options) error

type options struct {
	log           *slog.Logger
	reg           prometheus.Registerer
	onSnapshot    bool
	clock         cache.Clock
	rnd           *rand.Rand
	retryMin      time.Duration
	retryMax      time.Duration
	deleteTimeout time.Duration
}

func defaultOptions() options {
	return options{
		log:   slog.New(slog.DiscardHandler),
		clock: systemClock{},
		//nolint:gosec // G404: jitter for retry delays, not security sensitive
		rnd:           rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		retryMin:      DefaultRetryMin,
		retryMax:      DefaultRetryMax,
		deleteTimeout: DefaultDeleteTimeout,
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) error {
		if l == nil {
			return invalidConfig("nil logger")
		}
		o.log = l
		return nil
	}
}

// WithMetrics registers the wake_cdcsource_* metrics on reg.
func WithMetrics(reg prometheus.Registerer) Option {
	return func(o *options) error {
		o.reg = reg
		return nil
	}
}

// WithInvalidateOnSnapshot makes snapshot reads invalidate their keys.
func WithInvalidateOnSnapshot(enabled bool) Option {
	return func(o *options) error {
		o.onSnapshot = enabled
		return nil
	}
}

// WithClock sets the time source of retry waits.
func WithClock(c cache.Clock) Option {
	return func(o *options) error {
		if c == nil {
			return invalidConfig("nil clock")
		}
		o.clock = c
		return nil
	}
}

// WithRand sets the random source of the retry jitter.
func WithRand(r *rand.Rand) Option {
	return func(o *options) error {
		if r == nil {
			return invalidConfig("nil rand")
		}
		o.rnd = r
		return nil
	}
}

// WithRetry sets the first retry delay and the backoff cap of Delete retries.
func WithRetry(minDelay, maxDelay time.Duration) Option {
	return func(o *options) error {
		if minDelay <= 0 || maxDelay < minDelay {
			return invalidConfig("retry delays %s, %s: need 0 < min <= max", minDelay, maxDelay)
		}
		o.retryMin, o.retryMax = minDelay, maxDelay
		return nil
	}
}

// WithDeleteTimeout bounds each Delete call.
func WithDeleteTimeout(d time.Duration) Option {
	return func(o *options) error {
		if d <= 0 {
			return invalidConfig("delete timeout %s must be positive", d)
		}
		o.deleteTimeout = d
		return nil
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
