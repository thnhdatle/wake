// SPDX-License-Identifier: Apache-2.0

package webhooksink

import "time"

const (
	RetryReasonServerError = "server_error"
	RetryReasonRateLimited = "rate_limited"
	RetryReasonTimeout     = "timeout"
	RetryReasonNetwork     = "network"
	RetryReasonPoison      = "poison"
	RetryReasonDeadLetter  = "dead_letter"
	RetryReasonInternal    = "internal"
)

// Observer receives delivery metrics from the sink.
type Observer interface {
	ObserveSent(sink string, events, bytes int)
	ObserveAcked(sink string, events int)
	ObserveRetry(sink, reason string)
	ObserveLatency(sink string, d time.Duration)
	SetInflight(sink string, events int)
	ObserveDeadLettered(sink string, events int)
}

// AckObserver is told when an event is acknowledged.
type AckObserver interface {
	ObserveAck(sink string, tsMs int64)
}

type AckObserverFunc func(sink string, tsMs int64)

func (f AckObserverFunc) ObserveAck(sink string, tsMs int64) { f(sink, tsMs) }

type nopObserver struct{}

func (nopObserver) ObserveSent(string, int, int)         {}
func (nopObserver) ObserveAcked(string, int)             {}
func (nopObserver) ObserveRetry(string, string)          {}
func (nopObserver) ObserveLatency(string, time.Duration) {}
func (nopObserver) SetInflight(string, int)              {}
func (nopObserver) ObserveDeadLettered(string, int)      {}
func (nopObserver) ObserveAck(string, int64)             {}
