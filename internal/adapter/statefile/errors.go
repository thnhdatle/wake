// SPDX-License-Identifier: Apache-2.0

package statefile

import "errors"

var (
	// ErrCorrupt reports a state file that is not valid state JSON. The file
	// is left untouched so an operator can inspect it.
	ErrCorrupt = errors.New("statefile: corrupt state file")

	// ErrUnsupportedVersion reports a state file written by a newer Wake.
	ErrUnsupportedVersion = errors.New("statefile: unsupported state format version")

	// ErrInvalidPath reports an empty state file path.
	ErrInvalidPath = errors.New("statefile: invalid path")
)
