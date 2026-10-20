// SPDX-License-Identifier: Apache-2.0

package stdoutsink

import "time"

// RetryReasonWrite is the reason passed to Observer.ObserveRetry after a
// failed write.
const RetryReasonWrite = "write"

// Observer receives the sink's metrics (sent, acked, retries, latency,
// inflight). Its methods match webhooksink.Observer's, so one metrics type
// serves both. Implementations must be safe for concurrent use and must not
// block.
type Observer interface {
	// ObserveSent counts one write attempt of a batch of events of bytes
	// bytes; retries are counted again.
	ObserveSent(sink string, events, bytes int)
	// ObserveAcked counts events acknowledged after write and flush.
	ObserveAcked(sink string, events int)
	// ObserveRetry counts one failed write that will be retried.
	ObserveRetry(sink, reason string)
	// ObserveLatency records the duration of one write attempt.
	ObserveLatency(sink string, d time.Duration)
	// SetInflight reports the number of events in the batch being written.
	SetInflight(sink string, events int)
}

// AckObserver receives the ts_ms of every acknowledged event, for the
// end-to-end latency histogram.
type AckObserver interface {
	ObserveAck(sink string, tsMs int64)
}

type nopObserver struct{}

func (nopObserver) ObserveSent(string, int, int)         {}
func (nopObserver) ObserveAcked(string, int)             {}
func (nopObserver) ObserveRetry(string, string)          {}
func (nopObserver) ObserveLatency(string, time.Duration) {}
func (nopObserver) SetInflight(string, int)              {}
func (nopObserver) ObserveAck(string, int64)             {}
