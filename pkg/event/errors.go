// SPDX-License-Identifier: Apache-2.0

package event

import "errors"

var (
	// ErrInvalid is wrapped by every error for a payload that violates the CDC event contract.
	ErrInvalid = errors.New("event: invalid payload")

	// ErrTombstone is returned by the decoders for a nil or zero-length input.
	ErrTombstone = errors.New("event: tombstone (empty value)")

	// ErrValueType is wrapped by the Value accessors when the value cannot be converted.
	ErrValueType = errors.New("event: value has a different type")
)
