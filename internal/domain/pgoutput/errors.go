// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"errors"
	"fmt"
)

var (
	// ErrMalformed reports bytes that are not a well-formed pgoutput
	// protocol version 1 message: truncated fields, lengths beyond the
	// message, unknown tuple column kinds or trailing bytes.
	ErrMalformed = errors.New("pgoutput: malformed message")
	// ErrStreaming reports a message of a streamed in-progress transaction
	// (Stream Start/Stop/Commit/Abort). Wake does not request streaming;
	// UnsupportedMessageError for those types matches it with errors.Is.
	ErrStreaming = errors.New("pgoutput: streamed transactions are not supported")
	// ErrUnknownRelation reports a row message for a relation OID that no
	// Relation message has described yet.
	ErrUnknownRelation = errors.New("pgoutput: unknown relation")
	// ErrUnknownColumn reports a key column override naming a column the
	// relation does not have.
	ErrUnknownColumn = errors.New("pgoutput: unknown column")
	// ErrColumnCount reports a tuple whose column count differs from the
	// cached relation's.
	ErrColumnCount = errors.New("pgoutput: tuple column count does not match relation")
	// ErrMissingOldTuple reports a Delete without old tuple, or an Update of
	// a REPLICA IDENTITY FULL table without the old row.
	ErrMissingOldTuple = errors.New("pgoutput: old tuple required but not sent")
	// ErrUnchangedKey reports a key column whose value Postgres did not send
	// (unchanged TOAST marker and no old key tuple).
	ErrUnchangedKey = errors.New("pgoutput: key column value not available")
	// ErrUnexpectedUnchanged reports an unchanged-TOAST marker where a value
	// is required: in an Insert, or under REPLICA IDENTITY FULL without an
	// old value to copy.
	ErrUnexpectedUnchanged = errors.New("pgoutput: unexpected unchanged toast marker")
	// ErrBinaryValue reports a column sent in binary format, which Wake
	// never requests.
	ErrBinaryValue = errors.New("pgoutput: binary column values are not supported")
	// ErrNotRowMessage reports RowImages called with a message other than
	// Insert, Update or Delete.
	ErrNotRowMessage = errors.New("pgoutput: message carries no row images")
)

// UnsupportedMessageError reports a message type outside protocol version 1
// without streaming: stream messages ('S', 'E', 'c', 'A', which also match
// ErrStreaming), two-phase commit messages of protocol version 3 and
// unknown types.
type UnsupportedMessageError struct {
	Type byte
}

// Error implements error.
func (e *UnsupportedMessageError) Error() string {
	return fmt.Sprintf("pgoutput: unsupported message type %q", e.Type)
}

// Is matches ErrStreaming for stream message types.
func (e *UnsupportedMessageError) Is(target error) bool {
	return target == ErrStreaming && isStreamType(e.Type)
}

func isStreamType(t byte) bool {
	return t == 'S' || t == 'E' || t == 'c' || t == 'A'
}
