// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/watermark"
)

// foreignNonce is the nonce of a crashed earlier process whose watermarks
// are still in the WAL.
const foreignNonce = "01JAZ3X9Q8D7M6K5H4G3F2E1CZ"

// TestMerger_High_ModelProperty is the model property of the merge: random
// interleavings of a small table's writers (inserts, updates, deletes,
// key-changing updates, commit records that become visible late, aborts),
// a lagging stream and one table snapshot run, including retries. After the
// run and a stream drain, the sink equals the database for every key (no
// lost key); no r event is older than a stream event already emitted for its
// key (no stale value) and none brings back a deleted or moved-away key (no
// resurrection). Run longer with -rapid.checks=2000 (or more); rapid prints
// the failing seed.
func TestMerger_High_ModelProperty(t *testing.T) {
	t.Parallel()
	var total modelStats
	rapid.Check(t, func(rt *rapid.T) {
		stats := runModel(rt)
		total.runs += stats.runs
		total.chunks += stats.chunks
		total.retries += stats.retries
		total.droppedWindow += stats.droppedWindow
		total.droppedInvisible += stats.droppedInvisible
		total.aboveXmax += stats.aboveXmax
	})
	t.Logf("model coverage: %+v", total)
	// The model must exercise every path it exists for.
	assert.Positive(t, total.chunks, "chunks emitted")
	assert.Positive(t, total.retries, "retry path")
	assert.Positive(t, total.droppedWindow, "window drops")
	assert.Positive(t, total.droppedInvisible, "visibility-rule drops")
	assert.Positive(t, total.aboveXmax, "committed transactions at or above xmax")
}

func runModel(t *rapid.T) modelStats {
	var stats modelStats
	db := newModelDB(t)
	w := newModelWake(db, &stats)
	w.skipEmpty = rapid.Bool().Draw(t, "pg15_skip_empty")
	for range rapid.IntRange(100, 600).Draw(t, "steps") {
		step(t, db, w)
	}
	drain(t, db, w)
	for k := range modelKeys {
		cur := db.current(k)
		got := w.sink.rows[k]
		require.Equal(t, cur.present, got.present, "key %d: lost or ghost key", k)
		if cur.present {
			require.Equal(t, cur.version, got.version, "key %d: sink version", k)
		}
	}
	for len(w.acks) > 0 {
		w.ack(t)
	}
	upper, ok := w.progress.Committed()
	require.True(t, ok)
	require.Equal(t, w.upper, upper)
	require.Zero(t, w.progress.Pending())
	return stats
}

// step performs one random action of the database, the stream reader or
// the snapshotter.
func step(t *rapid.T, db *modelDB, w *modelWake) {
	switch action := rapid.IntRange(0, 99).Draw(t, "action"); {
	case action < 18:
		if len(db.procs) < modelMaxProcs {
			db.begin(false)
		}
	case action < 36:
		if tx := db.pick(t, txRunning); tx != nil {
			db.commitData(t, tx)
		}
	case action < 44:
		if tx := db.pick(t, txCommitted); tx != nil {
			db.complete(tx)
		}
	case action < 47:
		if tx := db.pick(t, txRunning); tx != nil {
			db.complete(tx)
		}
	case action < 72:
		if w.pos < len(db.wal) {
			w.readOne(t)
		}
	case action < 74:
		foreignWatermark(t, db, w)
	case action < 96:
		if w.phase != phaseIdle || rapid.IntRange(0, 3).Draw(t, "start") == 0 {
			w.advance(t)
		}
	default:
		w.ack(t)
	}
}

// foreignWatermark writes a watermark left by a crashed process: same run
// and chunk numbers, other nonce. The merger must never see it.
func foreignWatermark(t *rapid.T, db *modelDB, w *modelWake) {
	p := watermark.Payload{Nonce: foreignNonce, Run: modelRun, Table: modelTable, Chunk: w.chunk, Mark: watermark.MarkLow}
	if rapid.Bool().Draw(t, "foreign_high") {
		p.Mark = watermark.MarkHigh
		p.Snapshot = watermark.Snapshot{Xmin: db.nextXID, Xmax: db.nextXID}
	}
	b, err := p.MarshalJSON()
	require.NoError(t, err)
	db.commitMessage(db.begin(true), b)
}

// drain ends every open transaction, then runs the snapshot to its end and
// reads the whole WAL.
func drain(t *rapid.T, db *modelDB, w *modelWake) {
	for tx := db.pick(t, txRunning); tx != nil; tx = db.pick(t, txRunning) {
		if rapid.Bool().Draw(t, "drain_commit") {
			db.commitData(t, tx)
		} else {
			db.complete(tx)
		}
	}
	for tx := db.pick(t, txCommitted); tx != nil; tx = db.pick(t, txCommitted) {
		db.complete(tx)
	}
	for i := 0; w.phase != phaseDone || w.pos < len(db.wal); i++ {
		require.Less(t, i, 10000, "snapshot run does not terminate")
		if w.pos < len(db.wal) {
			w.readOne(t)
			continue
		}
		w.advance(t)
	}
}
