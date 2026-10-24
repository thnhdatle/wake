// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"errors"
	"fmt"
)

var ErrInvalidValue = errors.New("schema: invalid value")

var ErrInvalidState = errors.New("schema: invalid registry state")

const maxErrorText = 64

type ValueError struct {
	TypeOID uint32
	Text    string
	Reason  string
}

func newValueError(typeOID uint32, text []byte, reason string) *ValueError {
	if len(text) > maxErrorText {
		text = text[:maxErrorText]
	}
	return &ValueError{TypeOID: typeOID, Text: string(text), Reason: reason}
}

func (e *ValueError) Error() string {
	name, ok := TypeName(e.TypeOID)
	if !ok {
		name = FallbackTypeName(e.TypeOID)
	}
	return fmt.Sprintf("schema: invalid %s value %q: %s", name, e.Text, e.Reason)
}

func (e *ValueError) Unwrap() error { return ErrInvalidValue }
