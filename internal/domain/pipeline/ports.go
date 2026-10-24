// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type SlotManager interface {
	IdentifySystem(ctx context.Context) (SystemInfo, error)
	CreateSlot(ctx context.Context, name string) (SlotInfo, error)
	DropSlot(ctx context.Context, name string) error
}

type StandbyStatusSender interface {
	SendStandbyStatus(ctx context.Context, status StandbyStatus) error
}

// ReplicationSource streams changes from the source database.
type ReplicationSource interface {
	StandbyStatusSender
	StartReplication(ctx context.Context, opts StartOptions) error
	Receive(ctx context.Context) (ReplicationMessage, error)
}

type Catalog interface {
	ServerSettings(ctx context.Context) (ServerSettings, error)
	Slot(ctx context.Context, name string) (SlotStatus, error)
	PublicationTables(ctx context.Context, publications []string) ([]TableInfo, error)
	CurrentTime(ctx context.Context) (time.Time, error)
}

type ChunkReader interface {
	ReadChunk(ctx context.Context, req ChunkRequest) (Chunk, error)
	OpenExported(ctx context.Context, snapshotName string) (ChunkSession, error)
}

type ChunkSession interface {
	ReadChunk(ctx context.Context, req ChunkRequest) (Chunk, error)
	Close(ctx context.Context) error
}

type MessageEmitter interface {
	EmitMessage(ctx context.Context, prefix string, content []byte) (lsn.LSN, error)
}

// Sink delivers records to a destination.
type Sink interface {
	Name() string
	Send(ctx context.Context, rec Record) error
	Health() SinkHealth
	Run(ctx context.Context) error
}

type Acker interface {
	Ack(tok AckToken) error
}

// StateStore loads and saves the pipeline state.
type StateStore interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, s State) error
}

type SpillStore interface {
	Create(ctx context.Context, name string) (SpillFile, error)
	RemoveAll(ctx context.Context) error
}

type SpillFile interface {
	Append(ctx context.Context, rec []byte) error
	Replay(ctx context.Context, fn func(rec []byte) error) error
	Size() int64
	Remove() error
}

type AlertNotifier interface {
	Notify(ctx context.Context, a Alert) error
}
