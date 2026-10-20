// SPDX-License-Identifier: Apache-2.0

package watermark

import "fmt"

// Merger merges the chunks of one table snapshot run with the replication
// stream. It is fed the stream in WAL order, one transaction at a time:
// Begin, then Touch or TouchAll for every row change of the snapshotted
// table, Low or High when the transaction carries this run's watermark
// (filter with Payload.IsFor), then Commit. Every transaction the stream delivers must be fed, including
// those that touch other tables and the watermark transactions themselves.
// The snapshotter hands each chunk's keys to Rows before it emits the
// chunk's HIGH. The Merger is pure and not safe for concurrent use; callers
// serialize the stream and snapshotter sides.
//
// Retention. The Merger retains every transaction whose commit it read since
// it was created (which must happen before the run's first LOW is emitted),
// with the canonical keys (key and before key) it touched in the table. At
// each HIGH it forgets the transactions the HIGH snapshot shows as visible:
// every later snapshot sees them too, so they can never again be invisible
// to a chunk read. Retaining only the transactions since the previous
// chunk's HIGH would not be enough: this superset keeps transactions that
// stay invisible across a HIGH (a commit record written long before the proc
// array is updated, as with synchronous replication) droppable instead of
// forcing a retry.
// Transactions that touched nothing in the table are only kept while their
// xid precedes the run's first LOW, the only ones a retry decision can need.
//
// Decision at HIGH, for the chunk read's snapshot S:
//   - retry when an xid in committed_xip is not retained and precedes the
//     xid of the run's first LOW: it committed before the Merger existed, so
//     its keys are unknown. An unretained committed xid that does not precede
//     the first LOW began after the Merger existed, so had it touched the
//     table its commit would have been read and retained; it is ignored
//     (pgoutput on Postgres 15+ skips transactions without published
//     changes, which would otherwise retry chunks spuriously under load);
//   - otherwise drop every row whose key was touched by a transaction
//     committed between LOW and HIGH, or by a transaction committed before
//     LOW that is invisible to S (listed in xip or at or above xmax,
//     Snapshot.IsVisible), and emit the rest.
//
// committed_xip only decides retries, never which keys are dropped.
type Merger struct {
	tx   openTx
	inTx bool
	// ord numbers commits; retained transactions with ord >= lowOrd
	// committed inside the open window.
	ord      uint64
	retained map[uint32]*retainedTx

	inWindow bool
	window   uint64
	lowOrd   uint64

	// firstLowXID is the 32-bit xid of the first LOW transaction read;
	// horizon is its full xid, fixed at the first HIGH; lastXmax is the
	// latest HIGH snapshot's xmax (a recent full reference).
	firstLowSeen bool
	firstLowXID  uint32
	horizon      uint64
	lastXmax     uint64

	hasRows   bool
	rowsChunk uint64
	rows      [][]byte
}

type openTx struct {
	xid  uint32
	keys []string
	all  bool
}

type retainedTx struct {
	ord  uint64
	keys []string
	all  bool
}

// NewMerger returns a Merger for one snapshot run. Create it before the
// run's first LOW watermark is emitted and feed it every transaction the
// stream delivers from then on.
func NewMerger() *Merger {
	return &Merger{retained: make(map[uint32]*retainedTx)}
}

// Begin opens the stream transaction with the given 32-bit xid (pgoutput
// Begin).
func (m *Merger) Begin(xid uint32) error {
	if m.inTx {
		return fmt.Errorf("%w: begin %d inside %d", ErrOpenTransaction, xid, m.tx.xid)
	}
	m.tx = openTx{xid: xid}
	m.inTx = true
	return nil
}

// Touch records that the open transaction changed rows of the snapshotted
// table with the given canonical keys (change.AppendCanonicalKey of the
// event's key and, when it differs, of its before replica-identity key).
func (m *Merger) Touch(keys ...[]byte) error {
	if !m.inTx {
		return ErrNoTransaction
	}
	for _, k := range keys {
		m.tx.keys = append(m.tx.keys, string(k))
	}
	return nil
}

// TouchAll records that the open transaction changed every row of the
// snapshotted table (TRUNCATE).
func (m *Merger) TouchAll() error {
	if !m.inTx {
		return ErrNoTransaction
	}
	m.tx.all = true
	return nil
}

// Commit closes the open transaction at its commit record and retains it.
func (m *Merger) Commit() error {
	if !m.inTx {
		return ErrNoTransaction
	}
	m.inTx = false
	m.ord++
	tx := m.tx
	m.tx = openTx{}
	if len(tx.keys) == 0 && !tx.all && !m.mayForceRetry(tx.xid) {
		return nil
	}
	m.retained[tx.xid] = &retainedTx{ord: m.ord, keys: tx.keys, all: tx.all}
	return nil
}

// mayForceRetry reports whether a transaction that touched nothing in the
// table must still be retained: its xid precedes the run's first LOW (or no
// LOW was read yet), so a retry decision may look it up.
func (m *Merger) mayForceRetry(xid uint32) bool {
	switch {
	case !m.firstLowSeen:
		return true
	case m.horizon == 0:
		return precedes(xid, m.firstLowXID)
	default:
		return FullXID(xid, m.lastXmax) < m.horizon
	}
}

// Low records that the open transaction carries the LOW watermark p of this
// run, opening the window of chunk p.Chunk. A LOW read while another window
// is open abandons that window (its chunk was given up without a HIGH);
// nothing retained is forgotten.
func (m *Merger) Low(p *Payload) error {
	if !m.inTx {
		return ErrNoTransaction
	}
	if p.Mark != MarkLow {
		return fmt.Errorf("%w: %q passed to Low", ErrWrongMark, p.Mark)
	}
	if !m.firstLowSeen {
		m.firstLowSeen = true
		m.firstLowXID = m.tx.xid
	}
	m.inWindow = true
	m.window = p.Chunk
	m.lowOrd = m.ord + 1
	return nil
}

// Rows hands the merger the canonical keys of the rows chunk read, in chunk
// order. Call it after reading the chunk and before emitting its HIGH. The
// merger keeps the slice until High; the caller must not modify it.
func (m *Merger) Rows(chunk uint64, keys [][]byte) {
	m.hasRows = true
	m.rowsChunk = chunk
	m.rows = keys
}

// Retained returns the number of retained transactions.
func (m *Merger) Retained() int { return len(m.retained) }

// High records that the open transaction carries the HIGH watermark p of
// this run and returns the chunk's outcome (see Merger). It fails without
// changing state when p is not a valid HIGH, when no LOW of the same chunk
// was read since the previous HIGH (ErrNoLow) or when Rows was not called
// for the chunk (ErrNoRows); the caller then re-runs the chunk.
func (m *Merger) High(p *Payload) (Outcome, error) {
	if !m.inTx {
		return Outcome{}, ErrNoTransaction
	}
	if p.Mark != MarkHigh {
		return Outcome{}, fmt.Errorf("%w: %q passed to High", ErrWrongMark, p.Mark)
	}
	if err := p.Validate(); err != nil {
		return Outcome{}, err
	}
	if !m.inWindow || m.window != p.Chunk {
		return Outcome{}, fmt.Errorf("%w: chunk %d", ErrNoLow, p.Chunk)
	}
	if !m.hasRows || m.rowsChunk != p.Chunk {
		return Outcome{}, fmt.Errorf("%w: chunk %d", ErrNoRows, p.Chunk)
	}
	snap := p.Snapshot
	if m.horizon == 0 {
		m.horizon = FullXID(m.firstLowXID, snap.Xmax)
	}
	out := Outcome{Chunk: p.Chunk}
	if x, retry := m.retryXID(p.CommittedXIP); retry {
		out.Retry, out.RetryXID = true, x
	} else {
		m.filter(snap, &out)
	}
	m.prune(snap)
	m.lastXmax = snap.Xmax
	m.inWindow, m.hasRows, m.rows = false, false, nil
	return out, nil
}

// retryXID returns the first committed xid of xip that precedes the
// retention horizon and is not retained.
func (m *Merger) retryXID(committed []uint64) (uint64, bool) {
	for _, x := range committed {
		if x >= m.horizon {
			continue
		}
		if _, ok := m.retained[uint32(x)]; !ok { //nolint:gosec // G115: retained by 32-bit xid (MatchXID)
			return x, true
		}
	}
	return 0, false
}

// filter fills out.Keep and the drop counters.
func (m *Merger) filter(snap Snapshot, out *Outcome) {
	window, invisible := newKeySet(), newKeySet()
	for xid, tx := range m.retained {
		switch {
		case len(tx.keys) == 0 && !tx.all:
		case tx.ord >= m.lowOrd:
			window.add(tx)
		case !snap.IsVisible(xid):
			invisible.add(tx)
		}
	}
	out.Keep = make([]int, 0, len(m.rows))
	for i, k := range m.rows {
		switch {
		case window.has(k):
			out.DroppedWindow++
		case invisible.has(k):
			out.DroppedInvisible++
		default:
			out.Keep = append(out.Keep, i)
		}
	}
}

// prune forgets transactions visible to snap and, among those that touched
// nothing, the ones that cannot force a retry.
func (m *Merger) prune(snap Snapshot) {
	for xid, tx := range m.retained {
		touched := len(tx.keys) > 0 || tx.all
		if snap.IsVisible(xid) || (!touched && FullXID(xid, snap.Xmax) >= m.horizon) {
			delete(m.retained, xid)
		}
	}
}

// keySet is a set of canonical keys that may hold every key.
type keySet struct {
	keys map[string]struct{}
	all  bool
}

func newKeySet() keySet { return keySet{keys: make(map[string]struct{})} }

func (s *keySet) add(tx *retainedTx) {
	s.all = s.all || tx.all
	for _, k := range tx.keys {
		s.keys[k] = struct{}{}
	}
}

func (s *keySet) has(k []byte) bool {
	if s.all {
		return true
	}
	_, ok := s.keys[string(k)]
	return ok
}
