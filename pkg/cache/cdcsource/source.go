// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"
)

// SourceOption configures NewKafkaSource and NewNATSSource.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	log *slog.Logger
}

// WithSourceLogger sets the logger of a source.
func WithSourceLogger(l *slog.Logger) SourceOption {
	return func(o *sourceOptions) {
		if l != nil {
			o.log = l
		}
	}
}

func applySourceOptions(kind string, opts []SourceOption) sourceOptions {
	o := sourceOptions{log: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(&o)
	}
	o.log = o.log.With("component", "cdcsource", "provider", kind)
	return o
}

const (
	transportRetryMin = 200 * time.Millisecond
	transportRetryMax = 5 * time.Second
)

func retryDelay(attempt int, lo, hi time.Duration) time.Duration {
	d := hi
	if attempt < 32 {
		if exp := lo << attempt; exp > 0 && exp < d {
			d = exp
		}
	}
	half := d / 2
	return half + rand.N(d-half+1) //nolint:gosec // G404: retry jitter, not security sensitive
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func subjectNames(prefix string, m *Mapping) []string {
	names := make([]string, 0, len(m.names))
	for _, t := range m.names {
		names = append(names, prefix+"."+t)
	}
	return names
}
