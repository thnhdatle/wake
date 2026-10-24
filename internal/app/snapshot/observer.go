// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/watermark"
)

var _ pipeline.TxObserver = (*Snapshotter)(nil)

func (s *Snapshotter) ObserveBegin(tx pipeline.ObservedTx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inTx, s.tx, s.txHigh, s.txTriggers = true, tx, nil, nil
	if s.active != nil {
		s.mergerErr(context.Background(), s.active.merger.Begin(tx.XID))
	}
}

func (s *Snapshotter) ObserveChange(table string, key, beforeKey []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil || s.active.table.Name != table {
		return
	}
	if beforeKey != nil {
		s.mergerErr(context.Background(), s.active.merger.Touch(key, beforeKey))
		return
	}
	s.mergerErr(context.Background(), s.active.merger.Touch(key))
}

func (s *Snapshotter) ObserveTruncate(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil && s.active.table.Name == table {
		s.mergerErr(context.Background(), s.active.merger.TouchAll())
	}
}

func (s *Snapshotter) ObserveMessage(prefix string, content []byte) {
	switch prefix {
	case pgoutput.PrefixSnapshot:
		s.observeTrigger(content)
	case pgoutput.PrefixWatermark:
		s.observeWatermark(content)
	}
}

func (s *Snapshotter) observeTrigger(content []byte) {
	tr, err := watermark.ParseTrigger(content)
	if err != nil {
		s.metrics.Request(OtherTable)
		s.logger.LogAttrs(context.Background(), slog.LevelWarn, "invalid snapshot request ignored",
			slog.String("component", "snapshot"), slog.String("operation", "request"), slog.Any("error", err))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txTriggers = append(s.txTriggers, tr.Table)
}

func (s *Snapshotter) observeWatermark(content []byte) {
	p, err := watermark.ParsePayload(content)
	if err != nil || !p.IsOwn(s.cfg.Nonce) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.active
	if a == nil || !p.IsFor(s.cfg.Nonce, a.run, a.table.Name) {
		return
	}
	if p.Mark == watermark.MarkLow {
		s.mergerErr(context.Background(), a.merger.Low(&p))
		return
	}
	out, err := a.merger.High(&p)
	s.txHigh = &highResult{chunk: p.Chunk, out: out, err: err, tx: s.tx}
}

func (s *Snapshotter) ObserveCommit(ctx context.Context) (bool, error) {
	s.mu.Lock()
	if s.active != nil {
		s.mergerErr(ctx, s.active.merger.Commit())
	}
	s.inTx = false
	if s.pendingInstall != nil {
		s.active, s.pendingInstall = s.pendingInstall, nil
		close(s.installed)
		s.installed = nil
	}
	triggers, high, commit := s.txTriggers, s.txHigh, s.tx.CommitLSN
	s.txTriggers, s.txHigh = nil, nil
	hold := false
	if high != nil {
		if high.err != nil || high.out.Retry {
			s.pushResultLocked(chunkResult{
				chunk: high.chunk, retry: high.out.Retry, retryXID: high.out.RetryXID, err: high.err,
			})
		} else {
			s.held, hold = high, true
		}
	}
	s.mu.Unlock()
	if len(triggers) > 0 {
		if err := s.deps.Keeper.EnqueueRequests(ctx, triggers, commit); err != nil {
			return false, err
		}
	}
	return hold, nil
}

func (s *Snapshotter) ObserveHeld(ctx context.Context, tx pipeline.HeldTx) error {
	s.mu.Lock()
	h, a := s.held, s.active
	s.held = nil
	var rows []pipeline.ChunkRow
	ok := h != nil && a != nil && a.hasRows && a.rowsChunk == h.chunk
	if ok {
		rows = a.rows
	}
	s.mu.Unlock()
	if !ok {
		return tx.Seal()
	}
	g, err := routeChunk(ctx, tx, &chunkEmit{
		table: &a.table, run: a.run, chunk: h.chunk, chunkRows: a.chunkRows, at: h.tx.CommitLSN, tsMs: h.tx.TsMs,
		version: s.deps.Versions.Version(a.table.Name), rows: rows, keep: h.out.Keep, readAt: s.deps.Clock.Now(),
	})
	if err != nil {
		return err
	}
	if err := tx.Seal(); err != nil {
		return fmt.Errorf("snapshot: seal high watermark transaction: %w", err)
	}
	s.mu.Lock()
	s.pushResultLocked(chunkResult{chunk: h.chunk, kept: uint64(len(h.out.Keep)), done: g.Done()})
	s.mu.Unlock()
	return nil
}

func (s *Snapshotter) mergerErr(ctx context.Context, err error) {
	if err != nil {
		s.logger.LogAttrs(ctx, slog.LevelError, "watermark merger out of sequence",
			slog.String("component", "snapshot"), slog.String("operation", "observe"), slog.Any("error", err))
	}
}
