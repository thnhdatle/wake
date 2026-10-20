// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"maps"
	"slices"
	"strconv"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/watermark"
)

// This file models Postgres closely enough to check the chunk/stream merge:
//
//   - every transaction gets a full xid (xid8, epoch-extended, special low
//     values skipped) when it begins; pgoutput only shows its low 32 bits;
//   - a data transaction writes its rows and its commit record atomically
//     (the WAL record and clog), but stays in the proc array, invisible to
//     new snapshots, until a separate step ("committed in WAL, not yet
//     visible"); row locks are held until then, so writers of one key are
//     serialized by visibility as in Postgres;
//   - a snapshot has xmax = latestCompletedXid + 1 and xip = proc array xids
//     below xmax, so a transaction can be invisible without being listed;
//   - LOW and HIGH are transactions of their own; LOW's xid is assigned
//     before other transactions can commit ahead of it;
//   - the stream reads the WAL in commit order and may lag arbitrarily.

const (
	modelKeys      = 10
	modelChunkRows = 2
	modelMaxProcs  = 6
	modelNonce     = "01JAZ3X9Q8D7M6K5H4G3F2E1C0"
	modelRun       = 7
	modelTable     = "public.items"
)

type txState uint8

const (
	txRunning txState = iota
	txCommitted
)

type modelTx struct {
	xid   uint64
	state txState
	wake  bool
	locks []int
}

type modelWrite struct {
	xid     uint64 // 0: written before the simulation, visible to all
	version uint64
	present bool
}

type modelChange struct {
	key     int
	before  int // key moved away from, -1 unless a key-changing update
	present bool
	version uint64
}

type walRecord struct {
	xid     uint64
	changes []modelChange
	message []byte
}

type modelDB struct {
	nextXID         uint64
	latestCompleted uint64
	procs           map[uint64]*modelTx
	committed       map[uint64]bool
	hist            [modelKeys][]modelWrite
	lockedBy        [modelKeys]uint64
	version         uint64
	wal             []walRecord
}

// advanceXID mirrors FullTransactionIdAdvance: low halves 0..2 are special.
func advanceXID(x uint64) uint64 {
	x++
	if uint32(x) < 3 {
		x = x&^0xFFFFFFFF | 3
	}
	return x
}

func newModelDB(t *rapid.T) *modelDB {
	epoch := rapid.Uint64Range(0, 2).Draw(t, "epoch")
	low := rapid.SampledFrom([]uint32{3, 1000, 0x7FFFFFF8, 0xFFFFFFF0}).Draw(t, "xid_low")
	db := &modelDB{
		nextXID:   epoch<<32 | uint64(low),
		procs:     make(map[uint64]*modelTx),
		committed: make(map[uint64]bool),
	}
	db.latestCompleted = db.nextXID - 1
	for k := range modelKeys {
		if rapid.Bool().Draw(t, "initial_row") {
			db.version++
			db.hist[k] = append(db.hist[k], modelWrite{version: db.version, present: true})
		}
	}
	return db
}

func (db *modelDB) begin(wake bool) *modelTx {
	tx := &modelTx{xid: db.nextXID, wake: wake}
	db.nextXID = advanceXID(db.nextXID)
	db.procs[tx.xid] = tx
	return tx
}

// pick returns a random proc array transaction of Wake's workload in the
// given state, or nil.
func (db *modelDB) pick(t *rapid.T, state txState) *modelTx {
	var xs []uint64
	for _, x := range slices.Sorted(maps.Keys(db.procs)) {
		if tx := db.procs[x]; !tx.wake && tx.state == state {
			xs = append(xs, x)
		}
	}
	if len(xs) == 0 {
		return nil
	}
	return db.procs[rapid.SampledFrom(xs).Draw(t, "tx")]
}

func (db *modelDB) current(k int) modelWrite {
	if h := db.hist[k]; len(h) > 0 {
		return h[len(h)-1]
	}
	return modelWrite{}
}

func (db *modelDB) lockable(tx *modelTx, k int) bool {
	return db.lockedBy[k] == 0 || db.lockedBy[k] == tx.xid
}

func (db *modelDB) write(tx *modelTx, k int, present bool) uint64 {
	db.version++
	db.hist[k] = append(db.hist[k], modelWrite{xid: tx.xid, version: db.version, present: present})
	if db.lockedBy[k] != tx.xid {
		db.lockedBy[k] = tx.xid
		tx.locks = append(tx.locks, k)
	}
	return db.version
}

// commitData draws the transaction's row changes, applies them and writes
// its commit record; the transaction stays invisible until complete.
func (db *modelDB) commitData(t *rapid.T, tx *modelTx) {
	var changes []modelChange
	for range rapid.IntRange(0, 3).Draw(t, "ops") {
		k := rapid.IntRange(0, modelKeys-1).Draw(t, "key")
		kind := rapid.IntRange(0, 2).Draw(t, "kind")
		if !db.lockable(tx, k) {
			continue
		}
		changes = append(changes, db.applyOp(t, tx, k, kind))
	}
	db.wal = append(db.wal, walRecord{xid: tx.xid, changes: changes})
	tx.state = txCommitted
	db.committed[tx.xid] = true
}

func (db *modelDB) applyOp(t *rapid.T, tx *modelTx, k, kind int) modelChange {
	if !db.current(k).present {
		return modelChange{key: k, before: -1, present: true, version: db.write(tx, k, true)}
	}
	switch kind {
	case 1:
		return modelChange{key: k, before: -1, version: db.write(tx, k, false)}
	case 2:
		k2 := rapid.IntRange(0, modelKeys-1).Draw(t, "new_key")
		if k2 != k && db.lockable(tx, k2) && !db.current(k2).present {
			db.write(tx, k, false)
			return modelChange{key: k2, before: k, present: true, version: db.write(tx, k2, true)}
		}
	}
	return modelChange{key: k, before: -1, present: true, version: db.write(tx, k, true)}
}

// commitMessage writes a watermark transaction, which Wake's emitter only
// returns from once it is visible.
func (db *modelDB) commitMessage(tx *modelTx, payload []byte) {
	db.wal = append(db.wal, walRecord{xid: tx.xid, message: payload})
	db.committed[tx.xid] = true
	db.complete(tx)
}

// complete removes the transaction from the proc array (visibility for a
// committed one, end of an aborted one) and releases its row locks.
func (db *modelDB) complete(tx *modelTx) {
	delete(db.procs, tx.xid)
	db.latestCompleted = max(db.latestCompleted, tx.xid)
	for _, k := range tx.locks {
		db.lockedBy[k] = 0
	}
}

// snapshot takes pg_current_snapshot() and the row state it sees.
func (db *modelDB) snapshot(t *rapid.T) (watermark.Snapshot, [modelKeys]modelWrite) {
	snap := watermark.Snapshot{Xmax: advanceXID(db.latestCompleted)}
	snap.Xmin = snap.Xmax
	for _, x := range slices.Sorted(maps.Keys(db.procs)) {
		if x < snap.Xmax {
			snap.XIP = append(snap.XIP, x)
			snap.Xmin = min(snap.Xmin, x)
		}
	}
	require.NoError(t, snap.Validate())
	var seen [modelKeys]modelWrite
	for k := range modelKeys {
		invisibleSeen := false
		for _, w := range db.hist[k] {
			_, running := db.procs[w.xid]
			visible := w.xid == 0 || !running
			if w.xid != 0 {
				require.Equal(t, visible, snap.IsVisible(uint32(w.xid)), "IsVisible(%d) on %s", w.xid, snap)
			}
			require.False(t, visible && invisibleSeen, "model: visible write after an invisible one")
			invisibleSeen = invisibleSeen || !visible
			if visible {
				seen[k] = w
			}
		}
	}
	return snap, seen
}

// keyBytes returns the canonical key of model key k.
func keyBytes(k int) []byte {
	return change.AppendCanonicalKey(nil, change.Row{{Name: "id", Value: strconv.AppendInt(nil, int64(k), 10)}})
}
