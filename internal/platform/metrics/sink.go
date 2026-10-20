// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// SinkMetrics exports per-sink delivery metrics. Sinks are configured, so
// the sink label has bounded cardinality and is not capped; the retry reason
// comes from each sink's fixed set of reasons. Besides the plain methods it
// implements the observer method set the sink adapters share (ObserveSent,
// ObserveAcked, ObserveRetry, ObserveLatency, SetInflight and the webhook's
// ObserveDeadLettered), so one value serves every sink.
type SinkMetrics struct {
	sent         *prometheus.CounterVec
	sentBytes    *prometheus.CounterVec
	acked        *prometheus.CounterVec
	retries      *prometheus.CounterVec
	deadLettered *prometheus.CounterVec
	inflight     *prometheus.GaugeVec
	sendDuration *prometheus.HistogramVec
}

// RetryReasonUnspecified is the reason label of retries counted through
// Retry, which carries no reason.
const RetryReasonUnspecified = "unspecified"

// NewSinkMetrics creates the sink collectors and registers them on reg:
// wake_sink_events_sent_total{sink}, wake_sink_bytes_sent_total{sink},
// wake_sink_events_acked_total{sink}, wake_sink_retries_total{sink,reason},
// wake_sink_dead_lettered_total{sink}, wake_sink_inflight{sink} and
// wake_sink_send_duration_seconds{sink}.
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

// Sent counts n events handed to sink's destination; n <= 0 is ignored.
func (m *SinkMetrics) Sent(sink string, n int) { addCount(m.sent, sink, n) }

// Acked counts n events acknowledged by sink's destination.
func (m *SinkMetrics) Acked(sink string, n int) { addCount(m.acked, sink, n) }

// Retry counts n repeated delivery attempts of sink without a reason.
func (m *SinkMetrics) Retry(sink string, n int) {
	if n > 0 {
		m.retries.WithLabelValues(sink, RetryReasonUnspecified).Add(float64(n))
	}
}

// DeadLettered counts n events sink wrote to its dead letter destination.
func (m *SinkMetrics) DeadLettered(sink string, n int) { addCount(m.deadLettered, sink, n) }

// Inflight sets the number of events sink sent and has not seen acknowledged.
func (m *SinkMetrics) Inflight(sink string, n int) {
	m.inflight.WithLabelValues(sink).Set(float64(n))
}

// SendDuration records the duration of one delivery attempt of sink.
func (m *SinkMetrics) SendDuration(sink string, d time.Duration) {
	m.sendDuration.WithLabelValues(sink).Observe(d.Seconds())
}

// ObserveSent counts one delivery attempt of events events of bytes bytes
// (sink observer method set).
func (m *SinkMetrics) ObserveSent(sink string, events, bytes int) {
	addCount(m.sent, sink, events)
	addCount(m.sentBytes, sink, bytes)
}

// ObserveAcked counts events acknowledged by sink's destination (sink
// observer method set).
func (m *SinkMetrics) ObserveAcked(sink string, events int) { addCount(m.acked, sink, events) }

// ObserveRetry counts one failed attempt that sink will retry; reason is one
// of the sink's retry reason constants (sink observer method set).
func (m *SinkMetrics) ObserveRetry(sink, reason string) {
	if reason == "" {
		reason = RetryReasonUnspecified
	}
	m.retries.WithLabelValues(sink, reason).Inc()
}

// ObserveLatency records the duration of one delivery attempt (sink observer
// method set).
func (m *SinkMetrics) ObserveLatency(sink string, d time.Duration) { m.SendDuration(sink, d) }

// SetInflight sets the events sink accepted and has not seen acknowledged
// (sink observer method set).
func (m *SinkMetrics) SetInflight(sink string, events int) { m.Inflight(sink, events) }

// ObserveDeadLettered counts events written to the dead-letter directory and
// acknowledged without delivery (webhook observer method set).
func (m *SinkMetrics) ObserveDeadLettered(sink string, events int) { m.DeadLettered(sink, events) }

func addCount(c *prometheus.CounterVec, sink string, n int) {
	if n > 0 {
		c.WithLabelValues(sink).Add(float64(n))
	}
}
