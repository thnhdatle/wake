// SPDX-License-Identifier: Apache-2.0

package lsn

import "errors"

// ErrInvalid reports text that is not a valid "X/X" LSN.
var ErrInvalid = errors.New("lsn: invalid")
