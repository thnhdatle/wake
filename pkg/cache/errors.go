// SPDX-License-Identifier: Apache-2.0

package cache

import "errors"

var (
	// ErrNotFound is returned by a LoadFunc when the value does not exist in
	// the primary database; with WithNegativeTTL the absence is cached.
	// GetOrLoad returns it for a cached or freshly loaded absence.
	ErrNotFound = errors.New("cache: not found")
	// ErrDeferred is returned (wrapped) by Delete and Set while L2 is
	// unreachable: the key was removed from this instance's L1 and the L2
	// invalidation is queued for replay on recovery, but other instances
	// have not been invalidated yet. Callers that need the invalidation to
	// be global (such as cdcsource) retry until Delete returns nil.
	ErrDeferred = errors.New("cache: invalidation deferred")

	errClosed         = errors.New("cache: closed")
	errInvalidConfig  = errors.New("cache: invalid config")
	errL2Unavailable  = errors.New("cache: l2 unavailable")
	errEpochUnstable  = errors.New("cache: epoch changed during write")
	errMalformedValue = errors.New("cache: malformed value")
)
