// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

type txBuffer struct {
	maxBytes int64
	store    pipeline.SpillStore

	records  []pipeline.Record
	memBytes int64
	file     pipeline.SpillFile
	commit   lsn.LSN
	xid      uint32
	name     string
	scratch  []byte
}

func (b *txBuffer) reset(commit lsn.LSN, xid uint32) {
	b.records, b.memBytes, b.file, b.name, b.commit, b.xid = nil, 0, nil, "", commit, xid
}

func spillName(commit lsn.LSN, xid uint32) string {
	return fmt.Sprintf("%016X-%d", uint64(commit), xid)
}

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

func (b *txBuffer) take(tx *assembledTx) {
	tx.records, tx.file = b.records, b.file
	tx.bytes = b.memBytes
	if b.file != nil {
		tx.bytes = b.file.Size()
	}
	b.reset(0, 0)
}

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

type assembledTx struct {
	commit  lsn.LSN
	end     lsn.LSN
	readAt  time.Time
	hold    bool
	records []pipeline.Record
	file    pipeline.SpillFile
	bytes   int64
}

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
