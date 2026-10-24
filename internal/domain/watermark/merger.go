// SPDX-License-Identifier: Apache-2.0

package watermark

import "fmt"

// Merger merges snapshot chunks with the change stream.
type Merger struct {
	tx       openTx
	inTx     bool
	ord      uint64
	retained map[uint32]*retainedTx

	inWindow bool
	window   uint64
	lowOrd   uint64

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

// NewMerger returns an empty merger.
func NewMerger() *Merger {
	return &Merger{retained: make(map[uint32]*retainedTx)}
}

func (m *Merger) Begin(xid uint32) error {
	if m.inTx {
		return fmt.Errorf("%w: begin %d inside %d", ErrOpenTransaction, xid, m.tx.xid)
	}
	m.tx = openTx{xid: xid}
	m.inTx = true
	return nil
}

func (m *Merger) Touch(keys ...[]byte) error {
	if !m.inTx {
		return ErrNoTransaction
	}
	for _, k := range keys {
		m.tx.keys = append(m.tx.keys, string(k))
	}
	return nil
}

func (m *Merger) TouchAll() error {
	if !m.inTx {
		return ErrNoTransaction
	}
	m.tx.all = true
	return nil
}

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

func (m *Merger) Rows(chunk uint64, keys [][]byte) {
	m.hasRows = true
	m.rowsChunk = chunk
	m.rows = keys
}

func (m *Merger) Retained() int { return len(m.retained) }

// High closes a chunk window and returns the rows to emit.
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

func (m *Merger) prune(snap Snapshot) {
	for xid, tx := range m.retained {
		touched := len(tx.keys) > 0 || tx.all
		if snap.IsVisible(xid) || (!touched && FullXID(xid, snap.Xmax) >= m.horizon) {
			delete(m.retained, xid)
		}
	}
}

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
