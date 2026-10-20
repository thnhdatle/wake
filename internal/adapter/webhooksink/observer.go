// SPDX-License-Identifier: Apache-2.0

package webhooksink

import "time"

// Retry reasons passed to Observer.ObserveRetry; a bounded label set.
const (
	// RetryReasonServerError is a 5xx response.
	RetryReasonServerError = "server_error"
	// RetryReasonRateLimited is a 429 response.
	RetryReasonRateLimited = "rate_limited"
	// RetryReasonTimeout is a 408 response.
	RetryReasonTimeout = "timeout"
	// RetryReasonNetwork is a transport error: connection refused or reset,
	// TLS failure, client timeout.
	RetryReasonNetwork = "network"
	// RetryReasonPoison is any other non-2xx response.
	RetryReasonPoison = "poison"
	// RetryReasonDeadLetter is a failed dead-letter write.
	RetryReasonDeadLetter = "dead_letter"
	// RetryReasonInternal is a failure to build the request.
	RetryReasonInternal = "internal"
)

// Observer receives the sink's metrics (sent, acked, retries, latency,
// inflight; plus dead-lettered). The metrics adapter implements it;
// implementations must be safe for concurrent use and must not block.
type Observer interface {
	// ObserveSent counts one POST attempt of a batch of events with a body of
	// bytes bytes; retries are counted again.
	ObserveSent(sink string, events, bytes int)
	// ObserveAcked counts events acknowledged after a 2xx response.
	ObserveAcked(sink string, events int)
	// ObserveRetry counts one failed attempt that will be retried; reason is
	// one of the RetryReason constants.
	ObserveRetry(sink, reason string)
	// ObserveLatency records the duration of one POST attempt.
	ObserveLatency(sink string, d time.Duration)
	// SetInflight reports the number of events in batches being delivered.
	SetInflight(sink string, events int)
	// ObserveDeadLettered counts events written to the dead-letter directory
	// and acknowledged without delivery.
	ObserveDeadLettered(sink string, events int)
}

// AckObserver receives the ts_ms of every event acknowledged after delivery,
// for the end-to-end latency histogram. Dead-lettered events are not
// reported.
type AckObserver interface {
	ObserveAck(sink string, tsMs int64)
}

// AckObserverFunc adapts a function, such as lag.LatencyRecorder.Record, to
// AckObserver.
type AckObserverFunc func(sink string, tsMs int64)

// ObserveAck calls f.
func (f AckObserverFunc) ObserveAck(sink string, tsMs int64) { f(sink, tsMs) }

type nopObserver struct{}

func (nopObserver) ObserveSent(string, int, int)         {}
func (nopObserver) ObserveAcked(string, int)             {}
func (nopObserver) ObserveRetry(string, string)          {}
func (nopObserver) ObserveLatency(string, time.Duration) {}
func (nopObserver) SetInflight(string, int)              {}
func (nopObserver) ObserveDeadLettered(string, int)      {}
func (nopObserver) ObserveAck(string, int64)             {}
