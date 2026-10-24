// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/thnhdatle/wake/internal/platform/buildinfo"
)

// NewRegistry returns a registry with the Go runtime, process and build info collectors.
func NewRegistry(info buildinfo.Info) (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	build := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "wake_build_info",
		Help: "Build information; the value is always 1.",
		ConstLabels: prometheus.Labels{
			"version":    info.Version,
			"commit":     info.Commit,
			"go_version": info.GoVersion,
		},
	})
	build.Set(1)
	err := register(reg,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		build,
	)
	if err != nil {
		return nil, err
	}
	return reg, nil
}

func register(reg prometheus.Registerer, cs ...prometheus.Collector) error {
	if reg == nil {
		return ErrNilRegisterer
	}
	for _, c := range cs {
		if err := reg.Register(c); err != nil {
			return fmt.Errorf("register metrics collector: %w", err)
		}
	}
	return nil
}

type Option func(*options)

type options struct {
	tables *Capper
}

func WithTableCapper(c *Capper) Option {
	return func(o *options) { o.tables = c }
}

func newOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.tables == nil {
		o.tables = NewCapper(DefaultLabelLimit)
	}
	return o
}
