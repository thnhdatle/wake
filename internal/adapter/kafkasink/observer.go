// SPDX-License-Identifier: Apache-2.0

package kafkasink

import "time"

// Retry reasons passed to Observer.ObserveRetry; a bounded label set.
const (
	// RetryReasonNetwork is a connection, TLS transport or timeout error.
	RetryReasonNetwork = "network"
	// RetryReasonBroker is a retriable Kafka error code (leader change, not
	// enough replicas, request timed out, ...).
	RetryReasonBroker = "broker"
	// RetryReasonUnknownTopic is a missing topic.
	RetryReasonUnknownTopic = "unknown_topic"
	// RetryReasonMessageTooLarge is a record above the topic's limit.
	RetryReasonMessageTooLarge = "message_too_large"
	// RetryReasonEventTooLarge is an event above max_event_bytes.
	RetryReasonEventTooLarge = "event_too_large"
	// RetryReasonAuth is an authentication, authorization or certificate
	// failure.
	RetryReasonAuth = "auth"
	// RetryReasonRejected is any other non-retriable Kafka error code.
	RetryReasonRejected = "rejected"
	// RetryReasonOther is an unclassified error.
	RetryReasonOther = "other"
)

// Observer receives the sink's metrics (sent, acked, retries, latency,
// inflight). The metrics adapter implements it; implementations must be safe
// for concurrent use and must not block. The method set matches the other
// sinks' observers, so one adapter serves every sink.
type Observer interface {
	// ObserveSent counts one produce attempt of an event of bytes bytes
	// (key, value and headers); events re-produced after an error are
	// counted again.
	ObserveSent(sink string, events, bytes int)
	// ObserveAcked counts events acknowledged by Kafka (a `d` event once
	// its tombstone is, a `t` event once every partition write is).
	ObserveAcked(sink string, events int)
	// ObserveRetry counts one failed attempt that will be retried; reason is
	// one of the RetryReason constants.
	ObserveRetry(sink, reason string)
	// ObserveLatency records the time from an event's last produce attempt
	// to its acknowledgement.
	ObserveLatency(sink string, d time.Duration)
	// SetInflight reports the events accepted by Send and not yet
	// acknowledged (queued or produced).
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
