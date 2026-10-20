// SPDX-License-Identifier: Apache-2.0

package pgrepl

import (
	"errors"
	"fmt"
)

// Error codes of docs/guides/errors.md produced by this package.
const (
	CodeReplicationPrivilege = "replication_privilege"
	CodeSlotInUse            = "slot_in_use"
	CodeSlotInvalidated      = "slot_invalidated"
	CodeNoFreeWALSender      = "no_free_wal_sender"
	CodeNoFreeSlot           = "no_free_slot"
)

var (
	// ErrReplicationPrivilege reports a role that may not open a replication
	// connection (code replication_privilege).
	ErrReplicationPrivilege = errors.New("pgrepl: replication privilege missing")
	// ErrSlotInUse reports a slot with another active consumer (code
	// slot_in_use).
	ErrSlotInUse = errors.New("pgrepl: replication slot in use")
	// ErrSlotInvalidated reports a slot whose WAL was removed (wal_status
	// lost, code slot_invalidated).
	ErrSlotInvalidated = errors.New("pgrepl: replication slot invalidated")
	// ErrNoFreeWALSender reports that max_wal_senders connections are in use
	// (code no_free_wal_sender).
	ErrNoFreeWALSender = errors.New("pgrepl: no free wal sender")
	// ErrNoFreeSlot reports that max_replication_slots slots exist (code
	// no_free_slot).
	ErrNoFreeSlot = errors.New("pgrepl: no free replication slot")
	// ErrSlotNotFound reports a slot that does not exist.
	ErrSlotNotFound = errors.New("pgrepl: replication slot not found")
	// ErrSlotExists reports CreateSlot for a slot that already exists.
	ErrSlotExists = errors.New("pgrepl: replication slot already exists")
	// ErrInvalidArgument reports an invalid constructor argument, option,
	// slot or publication name.
	ErrInvalidArgument = errors.New("pgrepl: invalid argument")
	// ErrNotStreaming reports Receive or SendStandbyStatus before
	// StartReplication.
	ErrNotStreaming = errors.New("pgrepl: replication not started")
	// ErrStreaming reports a command on a connection that is streaming.
	ErrStreaming = errors.New("pgrepl: connection is streaming")
	// ErrClosed reports a call after Close or after the connection broke
	// while streaming.
	ErrClosed = errors.New("pgrepl: connection closed")
	// ErrProtocol reports an unexpected replication protocol message.
	ErrProtocol = errors.New("pgrepl: protocol error")
)

// Error is a replication failure with an error code of docs/guides/errors.md.
// It matches its sentinel (ErrSlotInUse, ...) and the underlying cause with
// errors.Is and errors.As.
type Error struct {
	// Code is the error code, e.g. slot_in_use.
	Code string
	// Slot is the slot name, empty when not slot related.
	Slot string
	// PID is the active consumer's process id (slot_in_use), zero when
	// unknown.
	PID int32
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

// privilegeError explains how to grant the replication privilege per
// provider.
func privilegeError(role string, cause error) *Error {
	if role == "" {
		role = "<role>"
	}
	detail := fmt.Sprintf("role %q cannot open a replication connection (IDENTIFY_SYSTEM failed); fix per provider: "+
		"self-hosted: ALTER ROLE %s REPLICATION; "+
		"Amazon RDS/Aurora: GRANT rds_replication TO %s, set rds.logical_replication=1 in the parameter group "+
		"and reboot; Google Cloud SQL: set the flag cloudsql.logical_decoding=on and ALTER ROLE %s REPLICATION; "+
		"Azure Database for PostgreSQL: set wal_level=logical, restart the server and ALTER ROLE %s REPLICATION",
		role, role, role, role, role)
	return &Error{Code: CodeReplicationPrivilege, Detail: detail, kind: ErrReplicationPrivilege, cause: cause}
}

func slotInUseError(slot string, pid int32, cause error) *Error {
	detail := fmt.Sprintf("slot %q is used by another consumer", slot)
	if pid != 0 {
		detail = fmt.Sprintf("slot %q is used by pid %d; stop the other Wake instance (one consumer per slot)", slot, pid)
	}
	return &Error{Code: CodeSlotInUse, Slot: slot, PID: pid, Detail: detail, kind: ErrSlotInUse, cause: cause}
}

func slotInvalidatedError(slot string, cause error) *Error {
	detail := fmt.Sprintf("slot %q lost required WAL (wal_status lost, max_slot_wal_keep_size exceeded); "+
		"drop it with `wake slot drop --yes` and restart with --reinit-state, then rebuild consumers", slot)
	return &Error{Code: CodeSlotInvalidated, Slot: slot, Detail: detail, kind: ErrSlotInvalidated, cause: cause}
}

func noFreeWALSenderError(cause error) *Error {
	return &Error{
		Code: CodeNoFreeWALSender, Detail: "all max_wal_senders connections are in use; raise max_wal_senders " +
			"(restart required) or stop another replication client", kind: ErrNoFreeWALSender, cause: cause,
	}
}

func noFreeSlotError(slot string, cause error) *Error {
	return &Error{
		Code: CodeNoFreeSlot, Slot: slot, Detail: "all max_replication_slots slots exist; raise " +
			"max_replication_slots (restart required) or drop an unused slot", kind: ErrNoFreeSlot, cause: cause,
	}
}
