// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"fmt"
	"io"
	"log/slog"
	"time"
)

const (
	DefaultQueryTimeout = 30 * time.Second
	DefaultChunkTimeout = 5 * time.Minute
	MaxChunkRows        = 1_000_000
)

type Option func(*options)

type options struct {
	queryTimeout time.Duration
	chunkTimeout time.Duration
	logger       *slog.Logger
}

func WithQueryTimeout(d time.Duration) Option { return func(o *options) { o.queryTimeout = d } }

func WithChunkTimeout(d time.Duration) Option { return func(o *options) { o.chunkTimeout = d } }

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
