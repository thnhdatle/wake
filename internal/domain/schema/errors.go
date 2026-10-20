// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"errors"
	"fmt"
)

// ErrInvalidValue reports a value whose text form does not match its type.
// Every ValueError matches it with errors.Is.
var ErrInvalidValue = errors.New("schema: invalid value")

// ErrInvalidState reports registry state that cannot be seeded: an empty
// table name, a duplicate table or an invalid replica identity.
var ErrInvalidState = errors.New("schema: invalid registry state")

// maxErrorText bounds the value text kept in a ValueError so that a huge
// malformed value is not retained or logged in full.
const maxErrorText = 64

// ValueError reports a value of a known type whose Postgres text output
// cannot be converted. Text holds at most the first 64 bytes of the value.
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

// Error implements error.
func (e *ValueError) Error() string {
	name, ok := TypeName(e.TypeOID)
	if !ok {
		name = FallbackTypeName(e.TypeOID)
	}
	return fmt.Sprintf("schema: invalid %s value %q: %s", name, e.Text, e.Reason)
}

// Unwrap returns ErrInvalidValue.
func (e *ValueError) Unwrap() error { return ErrInvalidValue }
