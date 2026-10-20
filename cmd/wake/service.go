// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/thnhdatle/wake/internal/app/lag"
	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/app/stream"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/platform/metrics"
)

// wiring are the inputs of wirePipeline.
type wiring struct {
	cfg     config.Config
	dsn     string
	cfgPath string
	reinit  bool
	log     *slog.Logger
	clk     clock.Clock
	reg     *prometheus.Registry
	hup     <-chan os.Signal
}

// collectors are the metrics adapters of the app layer.
type collectors struct {
	lag      *metrics.Lag
	reader   *metrics.Reader
	snapshot *metrics.Snapshot
	sinks    *metrics.SinkMetrics
}

func newCollectors(reg prometheus.Registerer) (*collectors, error) {
	var (
		c   collectors
		err error
	)
	tables := metrics.NewCapper(metrics.DefaultLabelLimit)
	if c.lag, err = metrics.NewLag(reg); err != nil {
		return nil, err
	}
	if c.reader, err = metrics.NewReader(reg, metrics.WithTableCapper(tables)); err != nil {
		return nil, err
	}
	if c.snapshot, err = metrics.NewSnapshot(reg, metrics.WithTableCapper(tables)); err != nil {
		return nil, err
	}
	if c.sinks, err = metrics.NewSinkMetrics(reg); err != nil {
		return nil, err
	}
	return &c, nil
}

// service is the wired pipeline of `wake run`.
type service struct {
	cfg       *config.Config
	log       *slog.Logger
	clk       clock.Clock
	src       *source
	repl      *replActivity
	sinks     []sinkEntry
	plan      *snapshot.Plan
	keeper    *snapshot.StateKeeper
	tracker   *stream.AckTracker
	status    *stream.StatusUpdater
	initial   *snapshot.Initial
	snap      *snapshot.Snapshotter
	reader    *stream.Reader
	monitor   *lag.Monitor
	heartbeat *lag.Heartbeat
	refresher *refresher
	// readerRunning is true while Reader.Run runs (readiness).
	readerRunning atomic.Bool
}

// close releases the source connections once run returned.
func (s *service) close(ctx context.Context) {
	if err := s.src.close(ctx); err != nil {
		s.log.Warn("close source connections", "component", "main", "error", err)
	}
}
