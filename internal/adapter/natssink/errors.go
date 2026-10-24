// SPDX-License-Identifier: Apache-2.0

package natssink

import "errors"

var ErrInvalidConfig = errors.New("natssink: invalid configuration")

var ErrAlreadyRunning = errors.New("natssink: already running")

var ErrEventTooLarge = errors.New("natssink: event too large")

var ErrMessageTooLarge = errors.New("natssink: message larger than the server's max_payload")

var ErrUnknownStream = errors.New("natssink: unknown stream")

var ErrNotConnected = errors.New("natssink: not connected")

var errEpochChanged = errors.New("natssink: connection changed")
