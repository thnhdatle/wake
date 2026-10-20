// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"errors"
	"strconv"
)

// ErrInvalidConfig reports a sink configuration New cannot use.
var ErrInvalidConfig = errors.New("webhooksink: invalid configuration")

// ErrMissingSecret reports that the environment variable named by
// webhook.secret_env is unset or empty.
var ErrMissingSecret = errors.New("webhooksink: signing secret not set")

// ErrAlreadyRunning reports a second call to Run.
var ErrAlreadyRunning = errors.New("webhooksink: already running")

// ErrEventTooLarge reports an event above max_event_bytes (error code
// event_too_large). Send blocks with it until its context ends.
var ErrEventTooLarge = errors.New("webhooksink: event too large")

// ErrDeadLetter reports a failed write of a poison batch to
// webhook.dead_letter_dir (error code sink_fatal while it persists).
var ErrDeadLetter = errors.New("webhooksink: dead-letter write failed")

// ErrInvalidSignature reports a Wake-Signature header that does not match
// the body, is malformed, or lies outside the tolerance window.
var ErrInvalidSignature = errors.New("webhooksink: invalid signature")

// StatusError reports a non-2xx response of the receiver.
type StatusError struct {
	StatusCode int
}

// Error implements error.
func (e *StatusError) Error() string {
	return "webhooksink: receiver returned HTTP " + strconv.Itoa(e.StatusCode)
}
