// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SinkMetrics exports per-sink delivery metrics.
type SinkMetrics struct {
	sent         *prometheus.CounterVec
	sentBytes    *prometheus.CounterVec
	acked        *prometheus.CounterVec
	retries      *prometheus.CounterVec
	deadLettered *prometheus.CounterVec
	inflight     *prometheus.GaugeVec
	sendDuration *prometheus.HistogramVec
}

const RetryReasonUnspecified = "unspecified"

// NewSinkMetrics creates the sink collectors and registers them on reg.
func NewSinkMetrics(reg prometheus.Registerer) (*SinkMetrics, error) {
	labels := []string{labelSink}
	m := &SinkMetrics{
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_sink_events_sent_total",
			Help: "Events handed to the sink's destination; events sent again after an error count again.",
		}, labels),
		sentBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_sink_bytes_sent_total",
			Help: "Bytes handed to the sink's destination; bytes sent again after an error count again.",
		}, labels),
		acked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_sink_events_acked_total",
			Help: "Events the destination acknowledged.",
		}, labels),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_sink_retries_total",
			Help: "Delivery attempts repeated after an error, by reason.",
		}, []string{labelSink, "reason"}),
		deadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wake_sink_dead_lettered_total",
			Help: "Events written to the sink's dead letter destination.",
		}, labels),
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "wake_sink_inflight",
			Help: "Events sent and not yet acknowledged.",
		}, labels),
		sendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wake_sink_send_duration_seconds",
			Help:    "Time one delivery attempt took.",
			Buckets: LatencyBuckets(),
		}, labels),
	}
	err := register(reg, m.sent, m.sentBytes, m.acked, m.retries, m.deadLettered, m.inflight, m.sendDuration)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *SinkMetrics) Sent(sink string, n int) { addCount(m.sent, sink, n) }

func (m *SinkMetrics) Acked(sink string, n int) { addCount(m.acked, sink, n) }

func (m *SinkMetrics) Retry(sink string, n int) {
	if n > 0 {
		m.retries.WithLabelValues(sink, RetryReasonUnspecified).Add(float64(n))
	}
}

func (m *SinkMetrics) DeadLettered(sink string, n int) { addCount(m.deadLettered, sink, n) }

func (m *SinkMetrics) Inflight(sink string, n int) {
	m.inflight.WithLabelValues(sink).Set(float64(n))
}

func (m *SinkMetrics) SendDuration(sink string, d time.Duration) {
	m.sendDuration.WithLabelValues(sink).Observe(d.Seconds())
}

func (m *SinkMetrics) ObserveSent(sink string, events, bytes int) {
	addCount(m.sent, sink, events)
	addCount(m.sentBytes, sink, bytes)
}

func (m *SinkMetrics) ObserveAcked(sink string, events int) { addCount(m.acked, sink, events) }

func (m *SinkMetrics) ObserveRetry(sink, reason string) {
	if reason == "" {
		reason = RetryReasonUnspecified
	}
	m.retries.WithLabelValues(sink, reason).Inc()
}

func (m *SinkMetrics) ObserveLatency(sink string, d time.Duration) { m.SendDuration(sink, d) }

func (m *SinkMetrics) SetInflight(sink string, events int) { m.Inflight(sink, events) }

func (m *SinkMetrics) ObserveDeadLettered(sink string, events int) { m.DeadLettered(sink, events) }

func addCount(c *prometheus.CounterVec, sink string, n int) {
	if n > 0 {
		c.WithLabelValues(sink).Add(float64(n))
	}
}
