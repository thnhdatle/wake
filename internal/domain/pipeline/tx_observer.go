// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type ObservedTx struct {
	XID       uint32
	CommitLSN lsn.LSN
	TsMs      int64
}

type TxObserver interface {
	ObserveBegin(tx ObservedTx)
	ObserveChange(table string, key, beforeKey []byte)
	ObserveTruncate(table string)
	ObserveMessage(prefix string, content []byte)
	ObserveCommit(ctx context.Context) (hold bool, err error)
	ObserveHeld(ctx context.Context, tx HeldTx) error
}

type HeldTx interface {
	NewGroup(readAt time.Time) (TxGroup, error)
	Seal() error
}

type TxGroup interface {
	Route(ctx context.Context, rec Record) (int, error)
	Seal() error
	Done() <-chan struct{}
}
