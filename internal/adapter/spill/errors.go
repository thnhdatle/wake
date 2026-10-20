// SPDX-License-Identifier: Apache-2.0

package spill

import "errors"

var (
	// ErrInvalidName reports a spill file name that is empty or contains a
	// path separator.
	ErrInvalidName = errors.New("spill: invalid file name")

	// ErrInvalidDir reports an empty spill directory.
	ErrInvalidDir = errors.New("spill: invalid directory")

	// ErrCorrupt reports a spill file whose frames are truncated or whose
	// length prefix exceeds the file.
	ErrCorrupt = errors.New("spill: corrupt file")

	// ErrRemoved reports a call on a removed file.
	ErrRemoved = errors.New("spill: file removed")

	// ErrRecordTooLarge reports a record longer than a frame can hold
	// (MaxRecordSize).
	ErrRecordTooLarge = errors.New("spill: record too large")
)
