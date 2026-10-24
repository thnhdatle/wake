// SPDX-License-Identifier: Apache-2.0

package statefile

import "errors"

var (
	ErrCorrupt = errors.New("statefile: corrupt state file")

	ErrUnsupportedVersion = errors.New("statefile: unsupported state format version")

	ErrInvalidPath = errors.New("statefile: invalid path")
)
