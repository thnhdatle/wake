// SPDX-License-Identifier: Apache-2.0

package l2

import "errors"

var (
	// ErrMalformed reports a stored value or bus message that does not follow
	// the shared encoding.
	ErrMalformed = errors.New("l2: malformed")
	// ErrClosed is returned by Receive after Close.
	ErrClosed = errors.New("l2: closed")
)
