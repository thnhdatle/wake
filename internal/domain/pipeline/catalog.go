// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type ServerSettings struct {
	ServerVersionNum    int
	WALLevel            string
	WALSenderTimeout    time.Duration
	MaxReplicationSlots int
	MaxWALSenders       int
	MaxSlotWALKeepSize  int64
}

type SlotStatus struct {
	Exists         bool
	Plugin         string
	ConfirmedFlush lsn.LSN
	WALStatus      string
	ActivePID      int32
	LagBytes       int64
}

type TableInfo struct {
	Schema string
	Table  string
	OID    uint32
	HasKey bool
}
