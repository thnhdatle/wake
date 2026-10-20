// SPDX-License-Identifier: Apache-2.0

package kafkasink

import "errors"

// ErrInvalidConfig reports a sink configuration New cannot use.
var ErrInvalidConfig = errors.New("kafkasink: invalid configuration")

// ErrMissingCredentials reports that an environment variable named by
// kafka.sasl.username_env or kafka.sasl.password_env is unset or empty.
var ErrMissingCredentials = errors.New("kafkasink: SASL credentials not set")

// ErrAlreadyRunning reports a second call to Run.
var ErrAlreadyRunning = errors.New("kafkasink: already running")

// ErrEventTooLarge reports an event above max_event_bytes (error code
// event_too_large). The sink blocks on it; only a configuration change on
// restart fixes it.
var ErrEventTooLarge = errors.New("kafkasink: event too large")

// ErrMessageTooLarge reports a record above the topic's max.message.bytes
// (error code sink_fatal). Raising the topic or broker limit resumes delivery.
var ErrMessageTooLarge = errors.New("kafkasink: record larger than the topic's max.message.bytes")

// ErrUnknownTopic reports a missing topic while kafka.auto_create_topics is
// false (error code sink_fatal). Creating the topic resumes delivery.
var ErrUnknownTopic = errors.New("kafkasink: unknown topic")

// errTopicNotReady reports a topic whose partitions are not in the metadata
// yet (retriable).
var errTopicNotReady = errors.New("kafkasink: topic not ready")
