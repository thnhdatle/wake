// SPDX-License-Identifier: Apache-2.0

package l2

import "errors"

var (
	ErrMalformed = errors.New("l2: malformed")
	ErrClosed    = errors.New("l2: closed")
)
