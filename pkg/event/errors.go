// SPDX-License-Identifier: Apache-2.0

package event

import "errors"

var (
	// ErrInvalid is wrapped by every error that reports a payload which does not
	// satisfy the CDC event contract: malformed JSON, a missing required field, a bad id or
	// LSN, or an op-specific rule violation.
	ErrInvalid = errors.New("event: invalid payload")

	// ErrTombstone is returned by the decoders for a nil or zero-length input,
	// which is how a Kafka tombstone record value looks. Consumers skip and count
	// such records.
	ErrTombstone = errors.New("event: tombstone (empty value)")

	// ErrValueType is wrapped by the typed accessors of Value when the value has
	// another JSON type than requested, is null, or does not fit the Go type.
	ErrValueType = errors.New("event: value has a different type")
)
