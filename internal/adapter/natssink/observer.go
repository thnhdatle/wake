// SPDX-License-Identifier: Apache-2.0

package natssink

import "time"

// Retry reasons passed to Observer.ObserveRetry; a bounded label set.
const (
	// RetryReasonNetwork is a disconnection, a missing acknowledgement or a
	// timeout.
	RetryReasonNetwork = "network"
	// RetryReasonServer is a retriable JetStream error (no responders,
	// 503).
	RetryReasonServer = "server"
	// RetryReasonUnknownStream is a missing stream.
	RetryReasonUnknownStream = "unknown_stream"
	// RetryReasonMessageTooLarge is a message above the server's
	// max_payload or the stream's max_msg_size.
	RetryReasonMessageTooLarge = "message_too_large"
	// RetryReasonEventTooLarge is an event above max_event_bytes.
	RetryReasonEventTooLarge = "event_too_large"
	// RetryReasonAuth is an authentication, authorization, permission or
	// certificate failure.
	RetryReasonAuth = "auth"
	// RetryReasonRejected is any other non-retriable JetStream error.
	RetryReasonRejected = "rejected"
	// RetryReasonOther is an unclassified error.
	RetryReasonOther = "other"
)

// Observer receives the sink's metrics (sent, acked, retries, latency,
// inflight). The metrics adapter implements it; implementations must be safe
// for concurrent use and must not block. The method set matches the other
// sinks' observers, so one adapter serves every sink.
type Observer interface {
	// ObserveSent counts one publish attempt of an event of bytes bytes
	// (payload and headers); events re-published after an error are
	// counted again.
	ObserveSent(sink string, events, bytes int)
	// ObserveAcked counts events acknowledged by a JetStream PubAck
	// (including duplicates the stream already held).
	ObserveAcked(sink string, events int)
	// ObserveRetry counts one failed attempt that will be retried; reason is
	// one of the RetryReason constants.
	ObserveRetry(sink, reason string)
	// ObserveLatency records the time from an event's last publish attempt
	// to its acknowledgement.
	ObserveLatency(sink string, d time.Duration)
	// SetInflight reports the events accepted by Send and not yet
	// acknowledged (queued or published).
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
