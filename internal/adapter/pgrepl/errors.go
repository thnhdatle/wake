// SPDX-License-Identifier: Apache-2.0

package pgrepl

import (
	"errors"
	"fmt"
)

const (
	CodeReplicationPrivilege = "replication_privilege"
	CodeSlotInUse            = "slot_in_use"
	CodeSlotInvalidated      = "slot_invalidated"
	CodeNoFreeWALSender      = "no_free_wal_sender"
	CodeNoFreeSlot           = "no_free_slot"
)

var (
	ErrReplicationPrivilege = errors.New("pgrepl: replication privilege missing")
	ErrSlotInUse            = errors.New("pgrepl: replication slot in use")
	ErrSlotInvalidated      = errors.New("pgrepl: replication slot invalidated")
	ErrNoFreeWALSender      = errors.New("pgrepl: no free wal sender")
	ErrNoFreeSlot           = errors.New("pgrepl: no free replication slot")
	ErrSlotNotFound         = errors.New("pgrepl: replication slot not found")
	ErrSlotExists           = errors.New("pgrepl: replication slot already exists")
	ErrInvalidArgument      = errors.New("pgrepl: invalid argument")
	ErrNotStreaming         = errors.New("pgrepl: replication not started")
	ErrStreaming            = errors.New("pgrepl: connection is streaming")
	ErrClosed               = errors.New("pgrepl: connection closed")
	ErrProtocol             = errors.New("pgrepl: protocol error")
)

type Error struct {
	Code   string
	Slot   string
	PID    int32
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
