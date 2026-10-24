// SPDX-License-Identifier: Apache-2.0

package stdoutsink

import "errors"

var ErrInvalidConfig = errors.New("stdoutsink: invalid configuration")

var ErrAlreadyRunning = errors.New("stdoutsink: already running")

var ErrEventTooLarge = errors.New("stdoutsink: event too large")
