// SPDX-License-Identifier: Apache-2.0

package redisl2

import "errors"

var (
	// ErrUnavailable wraps every failed Redis command: network errors,
	// timeouts, context ends and server errors (LOADING, READONLY, OOM,
	// CLUSTERDOWN, script errors). The cache core treats it as an L2 failure.
	ErrUnavailable = errors.New("redisl2: redis unavailable")

	errInvalidConfig = errors.New("redisl2: invalid config")
)
