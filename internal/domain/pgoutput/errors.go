// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"errors"
	"fmt"
)

var (
	ErrMalformed           = errors.New("pgoutput: malformed message")
	ErrStreaming           = errors.New("pgoutput: streamed transactions are not supported")
	ErrUnknownRelation     = errors.New("pgoutput: unknown relation")
	ErrUnknownColumn       = errors.New("pgoutput: unknown column")
	ErrColumnCount         = errors.New("pgoutput: tuple column count does not match relation")
	ErrMissingOldTuple     = errors.New("pgoutput: old tuple required but not sent")
	ErrUnchangedKey        = errors.New("pgoutput: key column value not available")
	ErrUnexpectedUnchanged = errors.New("pgoutput: unexpected unchanged toast marker")
	ErrBinaryValue         = errors.New("pgoutput: binary column values are not supported")
	ErrNotRowMessage       = errors.New("pgoutput: message carries no row images")
)

type UnsupportedMessageError struct {
	Type byte
}

func (e *UnsupportedMessageError) Error() string {
	return fmt.Sprintf("pgoutput: unsupported message type %q", e.Type)
}

func (e *UnsupportedMessageError) Is(target error) bool {
	return target == ErrStreaming && isStreamType(e.Type)
}

func isStreamType(t byte) bool {
	return t == 'S' || t == 'E' || t == 'c' || t == 'A'
}
