// SPDX-License-Identifier: Apache-2.0

package change

import "errors"

// ErrInvalidID reports text that does not match the event id grammar.
var ErrInvalidID = errors.New("change: invalid event id")

// ErrInvalidJSON reports a column value that is not valid JSON.
var ErrInvalidJSON = errors.New("change: invalid json value")
