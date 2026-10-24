// SPDX-License-Identifier: Apache-2.0

package stdoutsink

import "time"

const RetryReasonWrite = "write"

// Observer receives delivery metrics from the sink.
type Observer interface {
	ObserveSent(sink string, events, bytes int)
	ObserveAcked(sink string, events int)
	ObserveRetry(sink, reason string)
	ObserveLatency(sink string, d time.Duration)
	SetInflight(sink string, events int)
}

// AckObserver is told when an event is acknowledged.
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
