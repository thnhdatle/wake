// SPDX-License-Identifier: Apache-2.0

package natssink

import "errors"

// ErrInvalidConfig reports a sink configuration New cannot use.
var ErrInvalidConfig = errors.New("natssink: invalid configuration")

// ErrAlreadyRunning reports a second call to Run.
var ErrAlreadyRunning = errors.New("natssink: already running")

// ErrEventTooLarge reports an event above max_event_bytes, or above the
// server's max_payload when max_event_bytes is 0 (error code
// event_too_large).
var ErrEventTooLarge = errors.New("natssink: event too large")

// ErrMessageTooLarge reports a message above the server's max_payload while
// max_event_bytes is set higher (error code sink_fatal). Raising the
// server's max_payload resumes delivery.
var ErrMessageTooLarge = errors.New("natssink: message larger than the server's max_payload")

// ErrUnknownStream reports a missing stream while nats.create_stream is
// false (error code sink_fatal). Creating the stream resumes delivery.
var ErrUnknownStream = errors.New("natssink: unknown stream")

// ErrNotConnected reports that the connection to the server is not
// established yet or is being re-established (retriable).
var ErrNotConnected = errors.New("natssink: not connected")

// errEpochChanged reports that the connection was re-established while
// messages published on the previous connection are unresolved; the
// dispatcher waits for them before publishing more, so a message lost with
// the old connection is never overtaken.
var errEpochChanged = errors.New("natssink: connection changed")
