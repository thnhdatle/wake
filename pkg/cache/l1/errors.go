// SPDX-License-Identifier: Apache-2.0

package l1

import "errors"

// ErrInvalidConfig reports a bound or option outside its valid range.
var ErrInvalidConfig = errors.New("l1: invalid config")
