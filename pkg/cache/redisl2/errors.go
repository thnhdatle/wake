// SPDX-License-Identifier: Apache-2.0

package redisl2

import "errors"

var (
	// ErrUnavailable wraps every failed Redis command.
	ErrUnavailable = errors.New("redisl2: redis unavailable")

	errInvalidConfig = errors.New("redisl2: invalid config")
)
