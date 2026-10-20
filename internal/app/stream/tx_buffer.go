// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// txBuffer collects the records of the transaction being assembled. It
// keeps them in memory up to maxBytes (recordSize accounting) and moves them
// to a spill file once a record would exceed the bound: the
// buffered records are written first, every later record is appended, and
// the file is replayed in order when the transaction is dispatched.
type txBuffer struct {
	maxBytes int64
	store    pipeline.SpillStore

	records  []pipeline.Record
	memBytes int64
	file     pipeline.SpillFile
	// commit and xid identify the transaction; its spill file name
	// (spillName) is built only when it spills.
	commit  lsn.LSN
	xid     uint32
	name    string
	scratch []byte
}

// reset prepares the buffer for a new transaction, the one committed at
// commit with xid xid (zero for none).
func (b *txBuffer) reset(commit lsn.LSN, xid uint32) {
	b.records, b.memBytes, b.file, b.name, b.commit, b.xid = nil, 0, nil, "", commit, xid
}

// spillName is the spill file name of the transaction committed at commit
// with xid xid: "<commit LSN as 16 hex digits>-<xid>".
func spillName(commit lsn.LSN, xid uint32) string {
	return fmt.Sprintf("%016X-%d", uint64(commit), xid)
}

// add buffers rec, spilling when the memory bound would be exceeded. It
// reports whether this call started spilling.
func (b *txBuffer) add(ctx context.Context, rec *pipeline.Record) (started bool, err error) {
	size := recordSize(rec)
	if b.file == nil && b.memBytes+size > b.maxBytes {
		if err := b.spill(ctx); err != nil {
			return false, err
		}
		started = true
	}
	if b.file != nil {
		return started, b.appendFile(ctx, rec)
	}
	b.records = append(b.records, *rec)
	b.memBytes += size
	return false, nil
}

func (b *txBuffer) spill(ctx context.Context) error {
	b.name = spillName(b.commit, b.xid)
	f, err := b.store.Create(ctx, b.name)
	if err != nil {
		return fmt.Errorf("stream: create spill file %s: %w", b.name, err)
	}
	b.file = f
	for i := range b.records {
		if err := b.appendFile(ctx, &b.records[i]); err != nil {
			return err
		}
	}
	b.records, b.memBytes = nil, 0
	return nil
}

func (b *txBuffer) appendFile(ctx context.Context, rec *pipeline.Record) error {
	b.scratch = appendSpillRecord(b.scratch[:0], rec)
	if err := b.file.Append(ctx, b.scratch); err != nil {
		return fmt.Errorf("stream: append to spill file %s: %w", b.name, err)
	}
	return nil
}

// take hands the buffered records over to an assembled transaction and
// empties the buffer.
func (b *txBuffer) take(tx *assembledTx) {
	tx.records, tx.file = b.records, b.file
	tx.bytes = b.memBytes
	if b.file != nil {
		tx.bytes = b.file.Size()
	}
	b.reset(0, 0)
}

// discard drops the buffered records and removes the spill file.
func (b *txBuffer) discard() error {
	f := b.file
	b.reset(0, 0)
	if f == nil {
		return nil
	}
	if err := f.Remove(); err != nil {
		return fmt.Errorf("stream: remove spill file: %w", err)
	}
	return nil
}

// assembledTx is a transaction complete at Commit (or a forwarded
// non-transactional message), waiting to be dispatched.
type assembledTx struct {
	commit lsn.LSN
	end    lsn.LSN
	readAt time.Time
	// hold asks to hand the registered transaction to the observer.
	hold bool
	// records are in memory unless file is set.
	records []pipeline.Record
	file    pipeline.SpillFile
	// bytes counts the buffered size (memory or spill file).
	bytes int64
}

// route routes every record in order as part of h, replaying the spill
// file. In-memory records are routed without a callback, so the common
// case allocates nothing here.
func (tx *assembledTx) route(ctx context.Context, router *Router, h TxHandle) error {
	if tx.file == nil {
		for i := range tx.records {
			if _, err := router.Route(ctx, h, tx.records[i]); err != nil {
				return err
			}
		}
		return nil
	}
	err := tx.file.Replay(ctx, func(data []byte) error {
		rec, err := decodeSpillRecord(data)
		if err != nil {
			return err
		}
		_, err = router.Route(ctx, h, rec)
		return err
	})
	if err != nil {
		return fmt.Errorf("stream: replay spill file of %s: %w", tx.commit, err)
	}
	return nil
}

// release removes the spill file, if any.
func (tx *assembledTx) release() error {
	if tx.file == nil {
		return nil
	}
	f := tx.file
	tx.file = nil
	if err := f.Remove(); err != nil {
		return fmt.Errorf("stream: remove spill file of %s: %w", tx.commit, err)
	}
	return nil
}
