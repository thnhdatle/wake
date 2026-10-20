// SPDX-License-Identifier: Apache-2.0

package pipeline

import "time"

// SinkState classifies a sink's delivery state.
type SinkState uint8

const (
	// SinkStateHealthy delivers without errors.
	SinkStateHealthy SinkState = iota
	// SinkStateRetrying is backing off after retriable errors.
	SinkStateRetrying
	// SinkStateFatal keeps retrying after a non-retriable error: /readyz
	// returns 503 and the fatal-sink alert fires until it clears.
	SinkStateFatal
)

// Error codes reported in SinkHealth.Code (docs/guides/errors.md).
const (
	// CodeEventTooLarge: an event exceeds the sink's max_event_bytes.
	CodeEventTooLarge = "event_too_large"
	// CodeSinkFatal: any other non-retriable sink error.
	CodeSinkFatal = "sink_fatal"
)

// String returns the state's lower-case name.
func (s SinkState) String() string {
	switch s {
	case SinkStateHealthy:
		return "healthy"
	case SinkStateRetrying:
		return "retrying"
	case SinkStateFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// SinkHealth is a sink's health as reported by Sink.Health.
type SinkHealth struct {
	State SinkState
	// Code is the error code of the current error, empty when healthy.
	Code string
	// Reason describes the current error (table, key, lsn for
	// event_too_large); it never contains secrets or row values.
	Reason string
	// Since is when the sink entered State.
	Since time.Time
}

// IsHealthy reports whether the sink delivers without errors.
func (h SinkHealth) IsHealthy() bool { return h.State == SinkStateHealthy }

// IsFatal reports whether the sink is blocked by a non-retriable error.
func (h SinkHealth) IsFatal() bool { return h.State == SinkStateFatal }
