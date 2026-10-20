// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"errors"
	"fmt"
)

// Error codes of docs/guides/errors.md produced by this package. The
// replication codes repeat those of the replication adapter, whose errors
// the pre-flight check reports.
const (
	CodePublicationPrivilege = "publication_privilege"
	CodePublicationMissing   = "publication_missing"
	CodePublicationConflict  = "publication_conflict"
	CodePartitionRoot        = "publication_partition_root"
	CodeWALLevel             = "wal_level_not_logical"
	CodeServerVersion        = "postgres_version_unsupported"
	CodeWALRetention         = "wal_retention_unlimited"
	CodeTableKeyless         = "table_keyless"
	CodeIdentifierInvalid    = "identifier_invalid"
	CodeReplicationPrivilege = "replication_privilege"
	CodeSlotInUse            = "slot_in_use"
	CodeSlotInvalidated      = "slot_invalidated"
	CodeNoFreeSlot           = "no_free_slot"
	CodeNoFreeWALSender      = "no_free_wal_sender"
)

var (
	// ErrInvalidArgument reports an invalid constructor argument, option or
	// request.
	ErrInvalidArgument = errors.New("pgsql: invalid argument")
	// ErrTableNotFound reports a chunk read of a table that does not exist
	// (or is not a table).
	ErrTableNotFound = errors.New("pgsql: table not found")
	// ErrBadPosition reports a ChunkRequest.After this adapter did not
	// produce for the table, e.g. after the table's key changed.
	ErrBadPosition = errors.New("pgsql: bad chunk position")
	// ErrSessionClosed reports a read on a closed ChunkSession or a second
	// Close.
	ErrSessionClosed = errors.New("pgsql: chunk session closed")
	// ErrPublicationPrivilege reports missing CREATE on the database, a
	// managed publication owned by another role or matched tables the role
	// does not own (code publication_privilege).
	ErrPublicationPrivilege = errors.New("pgsql: publication privilege missing")
	// ErrPublicationConflict reports a publication with a managed name that
	// Wake cannot manage, e.g. FOR ALL TABLES (code publication_conflict).
	ErrPublicationConflict = errors.New("pgsql: publication cannot be managed")
	// ErrPublicationMissing reports an existing-mode publication that does
	// not exist (code publication_missing).
	ErrPublicationMissing = errors.New("pgsql: publication missing")
	// ErrValue reports a column value that does not convert to CDC event JSON.
	ErrValue = errors.New("pgsql: column value")
)

// Error is a failure with an error code of docs/guides/errors.md. It matches
// its sentinel and the underlying cause with errors.Is and errors.As.
type Error struct {
	// Code is the error code, e.g. publication_privilege.
	Code string
	// Detail explains the failure and its remediation.
	Detail string
	kind   error
	cause  error
}

// Error implements error.
func (e *Error) Error() string {
	msg := fmt.Sprintf("%v (%s): %s", e.kind, e.Code, e.Detail)
	if e.cause != nil {
		msg += ": " + e.cause.Error()
	}
	return msg
}

// ErrorCode returns the error code.
func (e *Error) ErrorCode() string { return e.Code }

// Unwrap returns the sentinel and the cause.
func (e *Error) Unwrap() []error {
	if e.cause == nil {
		return []error{e.kind}
	}
	return []error{e.kind, e.cause}
}

func codedError(code string, kind error, detail string) *Error {
	return &Error{Code: code, Detail: detail, kind: kind}
}
