// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/watermark"
)

// maxPendingChunks bounds the emitted chunks of one table whose `r` events
// are not all acknowledged yet; at the bound the copy waits for the oldest.
const maxPendingChunks = 16

// chunkProgress tracks one table's chunks from emission to acknowledgement
// and persists the progress: the position is the upper key of the
// latest chunk complete together with all earlier ones (watermark.Progress)
// and rows counts the rows of those chunks.
type chunkProgress struct {
	keeper   *StateKeeper
	metrics  Metrics
	run      uint64
	table    string
	progress *watermark.Progress[json.RawMessage]
	pending  []pendingChunk
	rows     uint64
}

type pendingChunk struct {
	chunk    uint64
	rows     uint64
	done     <-chan struct{}
	complete bool
}

func newChunkProgress(k *StateKeeper, m Metrics, run uint64, tp *pipeline.TableProgress) *chunkProgress {
	p := &chunkProgress{keeper: k, metrics: m, run: run, table: tp.Table, rows: tp.Rows}
	if tp.Position != nil {
		p.progress = watermark.ResumeProgress(tp.Position)
	} else {
		p.progress = &watermark.Progress[json.RawMessage]{}
	}
	return p
}

// add registers an emitted chunk whose group completes on done, then
// persists whatever completed meanwhile; it waits for the oldest chunk while
// maxPendingChunks are pending.
func (p *chunkProgress) add(ctx context.Context, chunk uint64, upper json.RawMessage, rows uint64,
	done <-chan struct{},
) error {
	if err := p.progress.Register(chunk, upper); err != nil {
		return fmt.Errorf("snapshot: %s: %w", p.table, err)
	}
	p.pending = append(p.pending, pendingChunk{chunk: chunk, rows: rows, done: done})
	if len(p.pending) >= maxPendingChunks {
		if err := p.wait(ctx, &p.pending[0]); err != nil {
			return err
		}
	}
	return p.poll(ctx)
}

func (p *chunkProgress) wait(ctx context.Context, c *pendingChunk) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("snapshot: wait for chunk %d of %s: %w", c.chunk, p.table, ctx.Err())
	}
}

// poll completes the acknowledged chunks and persists the progress when it
// advanced.
func (p *chunkProgress) poll(ctx context.Context) error {
	before := p.progress.Pending()
	for i := range p.pending {
		c := &p.pending[i]
		if c.complete {
			continue
		}
		select {
		case <-c.done:
			c.complete = true
			if err := p.progress.Complete(c.chunk); err != nil {
				return fmt.Errorf("snapshot: %s: %w", p.table, err)
			}
		default:
		}
	}
	n := before - p.progress.Pending()
	if n == 0 {
		return nil
	}
	var rows uint64
	for _, c := range p.pending[:n] {
		rows += c.rows
	}
	clear(p.pending[:n])
	p.pending = p.pending[n:]
	p.rows += rows
	p.metrics.RowsAcked(p.table, rows)
	pos, _ := p.progress.Committed()
	return p.keeper.setProgress(ctx, p.run, p.table, pos, p.rows, false)
}

// finish waits until every chunk is acknowledged, persisting the progress
// as chunks complete, and marks the table done.
func (p *chunkProgress) finish(ctx context.Context) error {
	for len(p.pending) > 0 {
		if err := p.wait(ctx, &p.pending[0]); err != nil {
			return err
		}
		if err := p.poll(ctx); err != nil {
			return err
		}
	}
	pos, _ := p.progress.Committed()
	return p.keeper.setProgress(ctx, p.run, p.table, pos, p.rows, true)
}

// readRecord returns the `r` event record of a chunk row: no
// tx_id, before null.
func readRecord(id change.ID, t *Table, tsMs int64, row *pipeline.ChunkRow, version uint64) pipeline.Record {
	ev := change.Event{
		ID: id, Source: t.Source, Op: change.OpRead, TsMs: tsMs, Key: row.Key, After: row.After,
		SchemaVersion: version,
	}
	return pipeline.NewEventRecord(&ev)
}

// routeChunk routes the rows at positions keep (all rows when keep is nil)
// as `r` events into a new group of tx, seals the group and returns it.
// Row i gets the id at the chunk's LSN with n = chunk × chunk_rows + i.
func routeChunk(ctx context.Context, tx pipeline.HeldTx, c *chunkEmit) (pipeline.TxGroup, error) {
	g, err := tx.NewGroup(c.readAt)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open chunk group: %w", err)
	}
	emit := func(i int) error {
		id, err := watermark.RowID(c.at, c.run, c.chunk, uint64(c.chunkRows), uint64(i)) //nolint:gosec // G115: non-negative
		if err != nil {
			return fmt.Errorf("snapshot: chunk %d of %s: %w", c.chunk, c.table.Name, err)
		}
		rec := readRecord(id, c.table, c.tsMs, &c.rows[i], c.version)
		if _, err := g.Route(ctx, rec); err != nil {
			return fmt.Errorf("snapshot: route %s: %w", id, err)
		}
		return nil
	}
	if c.keep == nil {
		for i := range c.rows {
			if err := emit(i); err != nil {
				return nil, err
			}
		}
	} else {
		for _, i := range c.keep {
			if err := emit(i); err != nil {
				return nil, err
			}
		}
	}
	if err := g.Seal(); err != nil {
		return nil, fmt.Errorf("snapshot: seal chunk group: %w", err)
	}
	return g, nil
}

// chunkEmit describes the `r` events of one chunk.
type chunkEmit struct {
	table     *Table
	run       uint64
	chunk     uint64
	chunkRows int
	at        lsn.LSN
	tsMs      int64
	version   uint64
	rows      []pipeline.ChunkRow
	keep      []int
	readAt    time.Time
}
