// SPDX-License-Identifier: Apache-2.0

package kafkasink

import "errors"

var ErrInvalidConfig = errors.New("kafkasink: invalid configuration")

var ErrMissingCredentials = errors.New("kafkasink: SASL credentials not set")

var ErrAlreadyRunning = errors.New("kafkasink: already running")

var ErrEventTooLarge = errors.New("kafkasink: event too large")

var ErrMessageTooLarge = errors.New("kafkasink: record larger than the topic's max.message.bytes")

var ErrUnknownTopic = errors.New("kafkasink: unknown topic")

var errTopicNotReady = errors.New("kafkasink: topic not ready")
