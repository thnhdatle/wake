// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// ObservedTx describes a stream transaction at its Begin message.
type ObservedTx struct {
	// XID is the 32-bit Postgres xid of the pgoutput Begin message.
	XID uint32
	// CommitLSN is the transaction's commit LSN (Begin final LSN).
	CommitLSN lsn.LSN
	// TsMs is the commit time in Unix milliseconds.
	TsMs int64
}

// TxObserver sees every stream transaction in WAL order, as the stream
// reader assembles it: the snapshotter implements it to feed its
// watermark merger and to receive wake.watermark and wake.snapshot messages.
// All methods are called from the reader goroutine, one transaction at a
// time: ObserveBegin, then ObserveChange, ObserveTruncate and
// ObserveMessage in WAL order, then ObserveCommit. Transactions the reader
// skips because they end at or before its start LSN are not observed.
//
// Hold-open: when ObserveCommit returns hold, the reader registers the
// transaction with the ack tracker, routes its own records and passes it to
// ObserveHeld instead of sealing it. The observer then owns the transaction:
// it may route further records (a chunk's `r` events) into it and must seal
// it. The reader dispatches no later transaction before ObserveHeld returns,
// so records routed during the call precede every later stream event on
// every sink (per-key order).
type TxObserver interface {
	// ObserveBegin opens a transaction.
	ObserveBegin(tx ObservedTx)
	// ObserveChange reports a captured row change. table is the Postgres
	// "<schema>.<table>"; key is the canonical key JSON; beforeKey is the
	// canonical JSON of the before replica-identity key when it differs from
	// key (key-changing update), else nil. Changes of keyless tables are not
	// reported. The slices are only valid during the call.
	ObserveChange(table string, key, beforeKey []byte)
	// ObserveTruncate reports a TRUNCATE of a captured table.
	ObserveTruncate(table string)
	// ObserveMessage reports a transactional wake.watermark or wake.snapshot
	// logical message; content is only valid during the call.
	ObserveMessage(prefix string, content []byte)
	// ObserveCommit closes the transaction. hold asks the reader to hand the
	// registered, unsealed transaction to ObserveHeld; an error stops the
	// reader.
	ObserveCommit(ctx context.Context) (hold bool, err error)
	// ObserveHeld receives a transaction for which ObserveCommit returned
	// hold, after its own records were routed. The observer must seal it;
	// an error stops the reader.
	ObserveHeld(ctx context.Context, tx HeldTx) error
}

// HeldTx is a transaction registered with the ack tracker and left open, so
// that the snapshotter can attach `r` events to it: the initial-snapshot
// pseudo-transaction at the consistent point or a HIGH watermark
// transaction. The stream package implements it.
type HeldTx interface {
	// NewGroup opens a completion group (one snapshot chunk) whose
	// deliveries age from readAt.
	NewGroup(readAt time.Time) (TxGroup, error)
	// Seal closes the transaction to further groups and deliveries; it is
	// confirmed once every delivery is acknowledged.
	Seal() error
}

// TxGroup is a completion group of a HeldTx: the `r` events of one chunk.
type TxGroup interface {
	// Route hands rec to the sinks that receive its table as part of the
	// group and returns the number of sinks; it blocks while a sink's buffer
	// is full.
	Route(ctx context.Context, rec Record) (int, error)
	// Seal closes the group; it completes once its deliveries are
	// acknowledged.
	Seal() error
	// Done returns a channel closed once the group is sealed and every
	// delivery acknowledged: the chunk is complete.
	Done() <-chan struct{}
}
