// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"errors"
	"strconv"
)

var ErrInvalidConfig = errors.New("webhooksink: invalid configuration")

var ErrMissingSecret = errors.New("webhooksink: signing secret not set")

var ErrAlreadyRunning = errors.New("webhooksink: already running")

var ErrEventTooLarge = errors.New("webhooksink: event too large")

var ErrDeadLetter = errors.New("webhooksink: dead-letter write failed")

var ErrInvalidSignature = errors.New("webhooksink: invalid signature")

type StatusError struct {
	StatusCode int
}

func (e *StatusError) Error() string {
	return "webhooksink: receiver returned HTTP " + strconv.Itoa(e.StatusCode)
}
