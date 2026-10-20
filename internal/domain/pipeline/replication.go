// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// ReplicationMessageKind tells which part of a ReplicationMessage is set.
type ReplicationMessageKind uint8

const (
	// ReplicationMessageXLogData carries WAL data (a pgoutput message).
	ReplicationMessageXLogData ReplicationMessageKind = iota + 1
	// ReplicationMessageKeepalive is a primary keepalive.
	ReplicationMessageKeepalive
)

// ReplicationMessage is one CopyData message of the replication stream.
type ReplicationMessage struct {
	Kind      ReplicationMessageKind
	XLogData  XLogData
	Keepalive Keepalive
}

// XLogData is a WAL data message.
type XLogData struct {
	// WALStart is the WAL position of the start of Data.
	WALStart lsn.LSN
	// ServerWALEnd is the server's current end of WAL.
	ServerWALEnd lsn.LSN
	// ServerTime is the server's clock when it sent the message.
	ServerTime time.Time
	// Data is one pgoutput message.
	Data []byte
}

// Keepalive is a primary keepalive message.
type Keepalive struct {
	// ServerWALEnd is the server's current end of WAL.
	ServerWALEnd lsn.LSN
	// ServerTime is the server's clock when it sent the message.
	ServerTime time.Time
	// ReplyRequested asks for a standby status update at once.
	ReplyRequested bool
}

// StandbyStatus is a standby status update.
type StandbyStatus struct {
	// Written is the last WAL position received.
	Written lsn.LSN
	// Flushed is the confirmed LSN; the adapter also reports it as applied.
	Flushed lsn.LSN
	// ClientTime is the sender's clock.
	ClientTime time.Time
	// ReplyRequested asks the server to answer at once.
	ReplyRequested bool
}

// StartOptions are the START_REPLICATION parameters.
type StartOptions struct {
	Slot         string
	Start        lsn.LSN
	ProtoVersion int
	Publications []string
	// Messages sets the pgoutput option messages 'true'.
	Messages bool
}

// SystemInfo is the IDENTIFY_SYSTEM result.
type SystemInfo struct {
	// SystemID is the Postgres system identifier in decimal.
	SystemID string
	Timeline int32
	XLogPos  lsn.LSN
	DBName   string
}

// SlotInfo is the CREATE_REPLICATION_SLOT result.
type SlotInfo struct {
	Name string
	// ConsistentPoint is the slot's consistent point (initial snapshot lsn).
	ConsistentPoint lsn.LSN
	// SnapshotName is the exported snapshot for SET TRANSACTION SNAPSHOT.
	SnapshotName string
}
