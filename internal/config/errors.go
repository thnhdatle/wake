// SPDX-License-Identifier: Apache-2.0

package config

import "errors"

var (
	// ErrInvalid is matched (errors.Is) by every *ValidationError.
	ErrInvalid = errors.New("config: invalid configuration")

	// ErrIdentifierInvalid reports a schema, table or prefix name outside
	// [A-Za-z0-9_-]+ (error code identifier_invalid).
	ErrIdentifierInvalid = errors.New("config: identifier invalid")

	// ErrTopicTooLong reports a topic name longer than MaxTopicLength.
	ErrTopicTooLong = errors.New("config: topic name too long")

	// ErrUnit reports an unparsable duration or byte size.
	ErrUnit = errors.New("config: invalid duration or size")
)

// ValidationError is one problem found by Validate. Path is the dotted key
// path (for example "sinks[0].kafka.brokers") so operators can find the key.
type ValidationError struct {
	Path    string
	Message string
}

// Error implements error.
func (e *ValidationError) Error() string { return e.Path + ": " + e.Message }

// Is makes every ValidationError match ErrInvalid.
func (e *ValidationError) Is(target error) bool { return target == ErrInvalid }
