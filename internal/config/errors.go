// SPDX-License-Identifier: Apache-2.0

package config

import "errors"

var (
	ErrInvalid = errors.New("config: invalid configuration")

	ErrIdentifierInvalid = errors.New("config: identifier invalid")

	ErrTopicTooLong = errors.New("config: topic name too long")

	ErrUnit = errors.New("config: invalid duration or size")
)

type ValidationError struct {
	Path    string
	Message string
}

func (e *ValidationError) Error() string { return e.Path + ": " + e.Message }

func (e *ValidationError) Is(target error) bool { return target == ErrInvalid }
