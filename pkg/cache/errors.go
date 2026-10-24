// SPDX-License-Identifier: Apache-2.0

package cache

import "errors"

var (
	// ErrNotFound is returned by a LoadFunc when the value does not exist.
	ErrNotFound = errors.New("cache: not found")
	// ErrDeferred is returned by Delete and Set while L2 is unreachable and the invalidation is
	// queued.
	ErrDeferred = errors.New("cache: invalidation deferred")

	errClosed         = errors.New("cache: closed")
	errInvalidConfig  = errors.New("cache: invalid config")
	errL2Unavailable  = errors.New("cache: l2 unavailable")
	errEpochUnstable  = errors.New("cache: epoch changed during write")
	errMalformedValue = errors.New("cache: malformed value")
)
