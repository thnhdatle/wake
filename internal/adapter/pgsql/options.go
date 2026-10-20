// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"fmt"
	"io"
	"log/slog"
	"time"
)

// Default timeouts.
const (
	// DefaultQueryTimeout bounds catalog queries, message emission and DDL.
	DefaultQueryTimeout = 30 * time.Second
	// DefaultChunkTimeout bounds one chunk read.
	DefaultChunkTimeout = 5 * time.Minute
	// MaxChunkRows bounds ChunkRequest.Limit.
	MaxChunkRows = 1_000_000
)

// Option configures the types of this package.
type Option func(*options)

type options struct {
	queryTimeout time.Duration
	chunkTimeout time.Duration
	logger       *slog.Logger
}

// WithQueryTimeout bounds every catalog query, message emission and
// publication change (default DefaultQueryTimeout).
func WithQueryTimeout(d time.Duration) Option { return func(o *options) { o.queryTimeout = d } }

// WithChunkTimeout bounds one chunk read (default DefaultChunkTimeout).
func WithChunkTimeout(d time.Duration) Option { return func(o *options) { o.chunkTimeout = d } }

// WithLogger sets the logger (default: discard).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

func newOptions(opts []Option) (options, error) {
	o := options{queryTimeout: DefaultQueryTimeout, chunkTimeout: DefaultChunkTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	if o.queryTimeout <= 0 || o.chunkTimeout <= 0 {
		return options{}, fmt.Errorf("%w: timeouts must be positive", ErrInvalidArgument)
	}
	if o.logger == nil {
		o.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return o, nil
}
