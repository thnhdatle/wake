// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"errors"
	"fmt"
)

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
	ErrInvalidArgument      = errors.New("pgsql: invalid argument")
	ErrTableNotFound        = errors.New("pgsql: table not found")
	ErrBadPosition          = errors.New("pgsql: bad chunk position")
	ErrSessionClosed        = errors.New("pgsql: chunk session closed")
	ErrPublicationPrivilege = errors.New("pgsql: publication privilege missing")
	ErrPublicationConflict  = errors.New("pgsql: publication cannot be managed")
	ErrPublicationMissing   = errors.New("pgsql: publication missing")
	ErrValue                = errors.New("pgsql: column value")
)

type Error struct {
	Code   string
	Detail string
	kind   error
	cause  error
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("%v (%s): %s", e.kind, e.Code, e.Detail)
	if e.cause != nil {
		msg += ": " + e.cause.Error()
	}
	return msg
}

func (e *Error) ErrorCode() string { return e.Code }

func (e *Error) Unwrap() []error {
	if e.cause == nil {
		return []error{e.kind}
	}
	return []error{e.kind, e.cause}
}

func codedError(code string, kind error, detail string) *Error {
	return &Error{Code: code, Detail: detail, kind: kind}
}
