// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/watermark"
)

type sinkRow struct {
	present bool
	version uint64
}

type modelSink struct {
	rows       [modelKeys]sinkRow
	lastStream [modelKeys]uint64
	ids        map[change.ID]bool
}

func (s *modelSink) id(t *rapid.T, id change.ID) {
	require.False(t, s.ids[id], "duplicate event id %s", id)
	s.ids[id] = true
}

func (s *modelSink) stream(t *rapid.T, id change.ID, c modelChange) {
	s.id(t, id)
	if c.before >= 0 {
		s.rows[c.before] = sinkRow{version: c.version}
		s.lastStream[c.before] = c.version
	}
	s.rows[c.key] = sinkRow{present: c.present, version: c.version}
	s.lastStream[c.key] = c.version
}

func (s *modelSink) read(t *rapid.T, id change.ID, k int, version uint64) {
	s.id(t, id)
	last := s.lastStream[k]
	require.False(t, last != 0 && !s.rows[k].present && version < last,
		"key %d resurrected by r version %d after its delete/move at version %d", k, version, last)
	require.GreaterOrEqual(t, version, last, "key %d: stale r version after stream version", k)
	s.rows[k] = sinkRow{present: true, version: version}
}

type phase uint8

const (
	phaseIdle phase = iota
	phaseLowBegin
	phaseLowCommit
	phaseRead
	phaseStatus
	phaseHigh
	phaseWait
	phaseDone
)

type modelStats struct {
	runs, chunks, retries, droppedWindow, droppedInvisible, aboveXmax int
}

type modelWake struct {
	db        *modelDB
	sink      modelSink
	pos       int
	merger    *watermark.Merger
	skipEmpty bool
	progress  watermark.Progress[int]
	acks      []uint64
	stats     *modelStats

	phase    phase
	chunk    uint64
	upper    int
	lowTx    *modelTx
	snap     watermark.Snapshot
	rows     []int
	versions []uint64
	last     bool
	xip      []uint64
}

func newModelWake(db *modelDB, stats *modelStats) *modelWake {
	return &modelWake{db: db, sink: modelSink{ids: make(map[change.ID]bool)}, upper: -1, stats: stats}
}

func recordLSN(i int) lsn.LSN { return lsn.LSN(0x1000 + i*0x100) }

func (w *modelWake) readOne(t *rapid.T) {
	rec := w.db.wal[w.pos]
	at := recordLSN(w.pos)
	w.pos++
	if w.skipEmpty && len(rec.changes) == 0 && rec.message == nil {
		return
	}
	if w.merger != nil {
		require.NoError(t, w.merger.Begin(uint32(rec.xid)))
	}
	for i, c := range rec.changes {
		w.sink.stream(t, change.StreamID(at, uint64(i)), c)
		if w.merger == nil {
			continue
		}
		keys := [][]byte{keyBytes(c.key)}
		if c.before >= 0 {
			keys = append(keys, keyBytes(c.before))
		}
		require.NoError(t, w.merger.Touch(keys...))
	}
	if rec.message != nil && w.merger != nil {
		w.message(t, at, rec.message)
	}
	if w.merger != nil {
		require.NoError(t, w.merger.Commit())
	}
}

func (w *modelWake) message(t *rapid.T, at lsn.LSN, content []byte) {
	p, err := watermark.ParsePayload(content)
	require.NoError(t, err)
	if !p.IsFor(modelNonce, modelRun, modelTable) {
		return
	}
	if p.Mark == watermark.MarkLow {
		require.NoError(t, w.merger.Low(&p))
		return
	}
	out, err := w.merger.High(&p)
	require.NoError(t, err)
	w.resolve(t, at, out)
}

func (w *modelWake) resolve(t *rapid.T, high lsn.LSN, out watermark.Outcome) {
	require.Equal(t, phaseWait, w.phase)
	require.Equal(t, w.chunk, out.Chunk)
	w.chunk++
	if out.Retry {
		require.Contains(t, w.xip, out.RetryXID)
		w.stats.retries++
		w.phase = phaseLowBegin
		return
	}
	w.stats.chunks++
	w.stats.droppedWindow += out.DroppedWindow
	w.stats.droppedInvisible += out.DroppedInvisible
	require.Len(t, out.Keep, len(w.rows)-out.DroppedWindow-out.DroppedInvisible)
	for _, i := range out.Keep {
		id, err := watermark.RowID(high, modelRun, out.Chunk, modelChunkRows, uint64(i)) //nolint:gosec // G115: i >= 0
		require.NoError(t, err)
		w.sink.read(t, id, w.rows[i], w.versions[i])
	}
	if len(w.rows) > 0 {
		w.upper = w.rows[len(w.rows)-1]
	}
	require.NoError(t, w.progress.Register(out.Chunk, w.upper))
	w.acks = append(w.acks, out.Chunk)
	w.phase = phaseLowBegin
	if w.last {
		w.phase = phaseDone
	}
}

func (w *modelWake) advance(t *rapid.T) {
	switch w.phase {
	case phaseIdle:
		w.merger = watermark.NewMerger()
		w.stats.runs++
		w.phase = phaseLowBegin
	case phaseLowBegin:
		w.lowTx = w.db.begin(true)
		w.phase = phaseLowCommit
	case phaseLowCommit:
		w.db.commitMessage(w.lowTx, w.payload(t, watermark.MarkLow))
		w.phase = phaseRead
	case phaseRead:
		w.readChunk(t)
		w.phase = phaseStatus
	case phaseStatus:
		w.xip = nil
		for _, x := range w.snap.XIP {
			if w.db.committed[x] {
				w.xip = append(w.xip, x)
			}
		}
		w.phase = phaseHigh
	case phaseHigh:
		keys := make([][]byte, len(w.rows))
		for i, k := range w.rows {
			keys[i] = keyBytes(k)
		}
		w.merger.Rows(w.chunk, keys)
		w.db.commitMessage(w.db.begin(true), w.payload(t, watermark.MarkHigh))
		w.phase = phaseWait
	case phaseWait, phaseDone:
	}
}

func (w *modelWake) readChunk(t *rapid.T) {
	snap, seen := w.db.snapshot(t)
	w.snap, w.rows, w.versions = snap, nil, nil
	for k := w.upper + 1; k < modelKeys && len(w.rows) < modelChunkRows; k++ {
		if seen[k].present {
			w.rows = append(w.rows, k)
			w.versions = append(w.versions, seen[k].version)
		}
	}
	w.last = len(w.rows) < modelChunkRows
	for x, tx := range w.db.procs {
		if tx.state == txCommitted && x >= snap.Xmax {
			w.stats.aboveXmax++
		}
	}
}

func (w *modelWake) payload(t *rapid.T, mark watermark.Mark) []byte {
	p := watermark.Payload{Nonce: modelNonce, Run: modelRun, Table: modelTable, Chunk: w.chunk, Mark: mark}
	if mark == watermark.MarkHigh {
		p.Snapshot, p.CommittedXIP = w.snap, w.xip
	}
	b, err := p.MarshalJSON()
	require.NoError(t, err)
	return b
}

func (w *modelWake) ack(t *rapid.T) {
	if len(w.acks) == 0 {
		return
	}
	i := rapid.IntRange(0, len(w.acks)-1).Draw(t, "ack")
	require.NoError(t, w.progress.Complete(w.acks[i]))
	w.acks = append(w.acks[:i], w.acks[i+1:]...)
}
