// SPDX-License-Identifier: Apache-2.0

package stdoutsink

import "errors"

// ErrInvalidConfig reports a sink configuration New cannot use.
var ErrInvalidConfig = errors.New("stdoutsink: invalid configuration")

// ErrAlreadyRunning reports a second call to Run.
var ErrAlreadyRunning = errors.New("stdoutsink: already running")

// ErrEventTooLarge reports an event above a configured max_event_bytes
// (error code event_too_large). Send blocks with it until its context ends.
var ErrEventTooLarge = errors.New("stdoutsink: event too large")
