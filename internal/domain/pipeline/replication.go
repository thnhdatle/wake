// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type ReplicationMessageKind uint8

const (
	ReplicationMessageXLogData ReplicationMessageKind = iota + 1
	ReplicationMessageKeepalive
)

type ReplicationMessage struct {
	Kind      ReplicationMessageKind
	XLogData  XLogData
	Keepalive Keepalive
}

type XLogData struct {
	WALStart     lsn.LSN
	ServerWALEnd lsn.LSN
	ServerTime   time.Time
	Data         []byte
}

type Keepalive struct {
	ServerWALEnd   lsn.LSN
	ServerTime     time.Time
	ReplyRequested bool
}

type StandbyStatus struct {
	Written        lsn.LSN
	Flushed        lsn.LSN
	ClientTime     time.Time
	ReplyRequested bool
}

type StartOptions struct {
	Slot         string
	Start        lsn.LSN
	ProtoVersion int
	Publications []string
	Messages     bool
}

type SystemInfo struct {
	SystemID string
	Timeline int32
	XLogPos  lsn.LSN
	DBName   string
}

type SlotInfo struct {
	Name            string
	ConsistentPoint lsn.LSN
	SnapshotName    string
}
