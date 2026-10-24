// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/thnhdatle/wake/internal/adapter/kafkasink"
	"github.com/thnhdatle/wake/internal/adapter/natssink"
	"github.com/thnhdatle/wake/internal/adapter/stdoutsink"
	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
	"github.com/thnhdatle/wake/internal/app/lag"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/platform/metrics"
)

var (
	_ kafkasink.Observer   = (*metrics.SinkMetrics)(nil)
	_ natssink.Observer    = (*metrics.SinkMetrics)(nil)
	_ webhooksink.Observer = (*metrics.SinkMetrics)(nil)
	_ stdoutsink.Observer  = (*metrics.SinkMetrics)(nil)
)

type latencyAcks struct {
	rec *lag.LatencyRecorder
}

func (l latencyAcks) ObserveAck(sink string, tsMs int64) { l.rec.Record(sink, tsMs) }

type sinkEntry struct {
	pipeline.Sink
	typ    config.SinkType
	tables []string
}

// buildSinks builds every configured sink.
func buildSinks(cfg *config.Config, clk clock.Clock, log *slog.Logger, m *metrics.SinkMetrics,
	acks latencyAcks,
) ([]sinkEntry, error) {
	out := make([]sinkEntry, 0, len(cfg.Sinks))
	for i := range cfg.Sinks {
		sc := cfg.Sinks[i]
		s, err := buildSink(&sc, clk, log, m, acks)
		if err != nil {
			return nil, &exitError{code: exitConfig, err: withCode(codeConfigInvalid,
				fmt.Errorf("sinks[%d] (%s): %w", i, sc.Name, err))}
		}
		out = append(out, sinkEntry{Sink: s, typ: sc.Type, tables: sc.Tables})
	}
	return out, nil
}

func buildSink(sc *config.Sink, clk clock.Clock, log *slog.Logger, m *metrics.SinkMetrics, acks latencyAcks,
) (pipeline.Sink, error) {
	switch sc.Type {
	case config.SinkKafka:
		return kafkasink.New(*sc, os.Getenv, clk, log, kafkasink.WithObserver(m), kafkasink.WithAckObserver(acks))
	case config.SinkNATS:
		return natssink.New(*sc, clk, log, natssink.WithObserver(m), natssink.WithAckObserver(acks))
	case config.SinkWebhook:
		return webhooksink.New(*sc, os.Getenv, clk, log, webhooksink.WithObserver(m),
			webhooksink.WithAckObserver(acks))
	case config.SinkStdout:
		return stdoutsink.New(*sc, clk, log, stdoutsink.WithObserver(m), stdoutsink.WithAckObserver(acks))
	default:
		return nil, fmt.Errorf("unknown sink type %q", sc.Type)
	}
}
