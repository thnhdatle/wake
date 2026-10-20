// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// ServerSettings are the server settings read at startup.
type ServerSettings struct {
	// ServerVersionNum is server_version_num, e.g. 170002.
	ServerVersionNum int
	WALLevel         string
	// WALSenderTimeout is wal_sender_timeout; zero disables the timeout.
	WALSenderTimeout    time.Duration
	MaxReplicationSlots int
	MaxWALSenders       int
	// MaxSlotWALKeepSize is max_slot_wal_keep_size in bytes; -1 is unlimited.
	MaxSlotWALKeepSize int64
}

// SlotStatus is a slot's row of pg_replication_slots.
type SlotStatus struct {
	// Exists is false when the slot does not exist; other fields are zero.
	Exists         bool
	Plugin         string
	ConfirmedFlush lsn.LSN
	// WALStatus is reserved, extended, unreserved or lost.
	WALStatus string
	// ActivePID is the pid of the consumer, zero when inactive.
	ActivePID int32
	// LagBytes is pg_current_wal_lsn() - confirmed_flush_lsn.
	LagBytes int64
}

// TableInfo describes one published table.
type TableInfo struct {
	// Schema and Table are the Postgres names (before tables.name_map).
	Schema string
	Table  string
	OID    uint32
	// HasKey is false for keyless tables.
	HasKey bool
}
