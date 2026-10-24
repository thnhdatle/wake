// SPDX-License-Identifier: Apache-2.0

package pipeline

import "time"

type SinkState uint8

const (
	SinkStateHealthy SinkState = iota
	SinkStateRetrying
	SinkStateFatal
)

const (
	CodeEventTooLarge = "event_too_large"
	CodeSinkFatal     = "sink_fatal"
)

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

type SinkHealth struct {
	State  SinkState
	Code   string
	Reason string
	Since  time.Time
}

func (h SinkHealth) IsHealthy() bool { return h.State == SinkStateHealthy }

func (h SinkHealth) IsFatal() bool { return h.State == SinkStateFatal }
