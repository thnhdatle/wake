// SPDX-License-Identifier: Apache-2.0

package spill

import "errors"

var (
	ErrInvalidName = errors.New("spill: invalid file name")

	ErrInvalidDir = errors.New("spill: invalid directory")

	ErrCorrupt = errors.New("spill: corrupt file")

	ErrRemoved = errors.New("spill: file removed")

	ErrRecordTooLarge = errors.New("spill: record too large")
)
