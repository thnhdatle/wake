// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// SlotManager runs the slot commands of the replication connection
// (adapter pgrepl). It is used at startup and by `wake slot drop`.
type SlotManager interface {
	// IdentifySystem runs IDENTIFY_SYSTEM. Its failure is the replication
	// privilege check.
	IdentifySystem(ctx context.Context) (SystemInfo, error)
	// CreateSlot creates the pgoutput slot with EXPORT_SNAPSHOT. The exported
	// snapshot stays importable until the connection runs its next command,
	// so StartReplication must wait until every snapshot worker imported it.
	CreateSlot(ctx context.Context, name string) (SlotInfo, error)
	// DropSlot drops the slot; an absent slot is not an error.
	DropSlot(ctx context.Context, name string) error
}

// StandbyStatusSender sends standby status updates. Implementations
// must allow a call while another goroutine is blocked in
// ReplicationSource.Receive, so that status updates continue while reading is
// paused for backpressure.
type StandbyStatusSender interface {
	// SendStandbyStatus reports the written and flushed positions to the
	// server. The flushed position is the confirmed LSN.
	SendStandbyStatus(ctx context.Context, status StandbyStatus) error
}

// ReplicationSource is the streaming side of the replication connection
// (adapter pgrepl). One goroutine calls Receive; SendStandbyStatus may be
// called concurrently from another.
type ReplicationSource interface {
	StandbyStatusSender
	// StartReplication issues START_REPLICATION with the pgoutput options.
	StartReplication(ctx context.Context, opts StartOptions) error
	// Receive blocks until the next XLogData or primary keepalive arrives.
	// XLogData.Data is only valid until the next call to Receive; callers copy
	// what they keep. A context deadline returns the context's error and
	// leaves the connection usable.
	Receive(ctx context.Context) (ReplicationMessage, error)
}

// Catalog answers the SQL-connection questions of startup, `wake check` and
// the lag monitor (adapter pgsql).
type Catalog interface {
	// ServerSettings reads the settings checked at startup.
	ServerSettings(ctx context.Context) (ServerSettings, error)
	// Slot reads the slot's row of pg_replication_slots; SlotStatus.Exists is
	// false when there is none.
	Slot(ctx context.Context, name string) (SlotStatus, error)
	// PublicationTables lists the tables of the publications.
	PublicationTables(ctx context.Context, publications []string) ([]TableInfo, error)
	// CurrentTime returns Postgres clock_timestamp(), read just before slot
	// creation as the initial snapshot's ts_ms.
	CurrentTime(ctx context.Context) (time.Time, error)
}

// ChunkReader reads table chunks by keyset pagination (adapter pgsql).
type ChunkReader interface {
	// ReadChunk reads one chunk in its own REPEATABLE READ transaction and
	// reports that transaction's snapshot and the committed xids of its xip
	// list (watermark procedure).
	ReadChunk(ctx context.Context, req ChunkRequest) (Chunk, error)
	// OpenExported opens a REPEATABLE READ transaction that imports the
	// exported snapshot of the initial snapshot.
	OpenExported(ctx context.Context, snapshotName string) (ChunkSession, error)
}

// ChunkSession reads chunks inside one transaction at an imported snapshot.
type ChunkSession interface {
	// ReadChunk reads one chunk; Chunk.Snapshot and CommittedXIP are empty.
	ReadChunk(ctx context.Context, req ChunkRequest) (Chunk, error)
	// Close ends the transaction.
	Close(ctx context.Context) error
}

// MessageEmitter writes transactional logical messages (watermarks,
// heartbeats) with pg_logical_emit_message (adapter pgsql).
type MessageEmitter interface {
	// EmitMessage emits a transactional message and returns its LSN.
	EmitMessage(ctx context.Context, prefix string, content []byte) (lsn.LSN, error)
}

// Sink delivers records to one destination asynchronously (adapters
// kafkasink, natssink, webhooksink, stdoutsink).
//
// Send hands a record to the sink's bounded buffer and blocks while the
// buffer is full (backpressure); it may be called from several goroutines
// and keeps the call order per partition key. Once the destination holds the
// record (for a Kafka `d` event: also its tombstone; for a `t` event: every
// partition write), the sink calls rec.Ack.Ack() exactly once. Retriable and
// non-retriable errors are retried without limit; Health reports
// them. Payload and key bytes are shared between sinks and are read-only.
type Sink interface {
	// Name returns the configured sink name.
	Name() string
	// Send enqueues rec; it returns the context's error when ctx ends first.
	Send(ctx context.Context, rec Record) error
	// Health reports the sink's state for /readyz and the fatal-sink alert.
	Health() SinkHealth
	// Run delivers and acknowledges records until ctx ends.
	Run(ctx context.Context) error
}

// Acker receives a sink's acknowledgement of one delivery. The stream ack
// tracker implements it; sinks reach it through Record.Ack.
type Acker interface {
	// Ack acknowledges the delivery identified by tok. Acknowledging a
	// delivery twice is an error.
	Ack(tok AckToken) error
}

// StateStore loads and saves the state file (adapter statefile).
type StateStore interface {
	// Load returns ErrStateNotFound when no state exists yet.
	Load(ctx context.Context) (State, error)
	// Save replaces the state atomically (temp file, fsync, rename).
	Save(ctx context.Context, s State) error
}

// SpillStore holds the records of transactions larger than
// stream.max_tx_bytes on disk (adapter spill).
type SpillStore interface {
	// Create starts a new spill file; name identifies the transaction.
	Create(ctx context.Context, name string) (SpillFile, error)
	// RemoveAll deletes the spill files of a crashed run (at start).
	RemoveAll(ctx context.Context) error
}

// SpillFile is one spilled transaction.
type SpillFile interface {
	// Append writes one encoded record.
	Append(ctx context.Context, rec []byte) error
	// Replay calls fn for every record in append order. The slice passed to
	// fn is only valid during the call.
	Replay(ctx context.Context, fn func(rec []byte) error) error
	// Size returns the bytes appended so far.
	Size() int64
	// Remove closes and deletes the file.
	Remove() error
}

// AlertNotifier delivers alert state changes (adapter alerthook).
type AlertNotifier interface {
	// Notify posts one alert state change.
	Notify(ctx context.Context, a Alert) error
}
