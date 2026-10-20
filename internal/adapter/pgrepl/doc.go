// SPDX-License-Identifier: Apache-2.0

// Package pgrepl implements the replication connection of Wake with
// github.com/jackc/pglogrepl over one pgconn connection opened
// with replication=database. Conn implements pipeline.SlotManager
// (IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT … LOGICAL pgoutput
// EXPORT_SNAPSHOT, DROP_REPLICATION_SLOT) and pipeline.ReplicationSource
// (START_REPLICATION with proto_version, publication_names and messages,
// Receive, SendStandbyStatus).
//
// # Session settings
//
// The connection sets DateStyle=ISO, IntervalStyle=postgres,
// client_encoding=UTF8, TimeZone=UTC and extra_float_digits=1 as startup
// parameters, so that pgoutput renders column values in the text forms
// schema.AppendJSON expects. Options cannot override them.
//
// # Exported snapshot
//
// CreateSlot exports a snapshot that stays importable until the connection
// runs its next command. Conn therefore runs no command of its own between
// CreateSlot and StartReplication: the caller imports the snapshot in every
// worker session and only then calls StartReplication. Calling
// IdentifySystem, CreateSlot or DropSlot in between ends the snapshot.
//
// # Concurrency
//
// Conn starts no goroutines. After StartReplication one goroutine calls
// Receive while others may call SendStandbyStatus at any time, including
// while Receive is blocked (status updates continue while reading
// is paused). Receive reads through pgconn.ReceiveMessage; a canceled or
// expired context interrupts it by setting a read deadline only (a custom
// pgconn context watcher handler, never a cancel request, which would end
// the walsender), and pgconn keeps a partially read message, so the
// connection stays usable after a context error. SendStandbyStatus encodes
// the update itself and writes it to the socket under its own mutex with a
// write deadline; a failed write closes the connection because a partial
// write corrupts the protocol stream. Close must not run concurrently with
// Receive.
//
// # Errors
//
// Failures that operators must act on are *Error values carrying the codes
// of docs/guides/errors.md: replication_privilege (with the fix per
// provider), slot_in_use (naming the active PID), slot_invalidated,
// no_free_wal_sender and no_free_slot. They match the sentinels of
// errors.go with errors.Is.
//
//	conn, err := pgrepl.New(dsn)
//	if err != nil { ... }
//	defer conn.Close(ctx)
//	slot, err := conn.CreateSlot(ctx, "wake_app")  // import slot.SnapshotName, then:
//	err = conn.StartReplication(ctx, pipeline.StartOptions{Slot: "wake_app", Start: slot.ConsistentPoint,
//		ProtoVersion: 1, Publications: []string{"wake_app"}, Messages: true})
//	msg, err := conn.Receive(ctx)
package pgrepl
