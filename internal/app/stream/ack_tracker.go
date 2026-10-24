// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const DefaultMaxInFlight = 1 << 16

type TxHandle struct{ seq uint64 }

type Group struct{ tx, id uint64 }

type groupEntry struct {
	tx      uint64
	readAt  time.Time
	pending int
	sealed  bool
	done    chan struct{}
}

type AckTrackerOption func(*AckTracker)

func WithMaxInFlight(n int) AckTrackerOption {
	return func(t *AckTracker) { t.maxInFlight = n }
}

// AckTracker computes the confirmed LSN from the acknowledged transactions.
type AckTracker struct {
	mu          sync.Mutex
	ring        txRing
	groups      map[uint64]*groupEntry
	nextGroup   uint64
	confirmed   lsn.LSN
	lastEnd     lsn.LSN
	space       chan struct{}
	maxInFlight int

	changed chan struct{}
	closed  chan struct{}
}

// NewAckTracker returns a tracker whose confirmed LSN starts at start.
func NewAckTracker(start lsn.LSN, opts ...AckTrackerOption) (*AckTracker, error) {
	t := &AckTracker{
		ring:        newTxRing(),
		groups:      map[uint64]*groupEntry{},
		confirmed:   start,
		lastEnd:     start,
		maxInFlight: DefaultMaxInFlight,
		changed:     make(chan struct{}, 1),
		closed:      make(chan struct{}),
	}
	close(t.closed)
	for _, o := range opts {
		o(t)
	}
	if t.maxInFlight <= 0 {
		return nil, fmt.Errorf("%w: max in-flight transactions %d", ErrInvalidOption, t.maxInFlight)
	}
	return t, nil
}

// Begin registers the next transaction in commit order.
func (t *AckTracker) Begin(ctx context.Context, commit, end lsn.LSN, readAt time.Time) (TxHandle, error) {
	for {
		t.mu.Lock()
		if commit < t.lastEnd || end < commit {
			last := t.lastEnd
			t.mu.Unlock()
			return TxHandle{}, fmt.Errorf("%w: commit %s end %s after end %s", ErrOutOfOrder, commit, end, last)
		}
		if t.ring.len() < t.maxInFlight {
			seq := t.ring.push(txEntry{commit: commit, end: end, readAt: readAt})
			t.lastEnd = end
			t.mu.Unlock()
			return TxHandle{seq: seq}, nil
		}
		if t.space == nil {
			t.space = make(chan struct{})
		}
		space := t.space
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			return TxHandle{}, fmt.Errorf("stream: wait for in-flight space: %w", ctx.Err())
		case <-space:
		}
	}
}

func (t *AckTracker) Add(tx TxHandle) (pipeline.AckToken, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, err := t.openTx(tx.seq)
	if err != nil {
		return pipeline.AckToken{}, err
	}
	e.pending++
	e.ungrouped++
	return pipeline.AckToken{Tx: tx.seq}, nil
}

func (t *AckTracker) NewGroup(tx TxHandle, readAt time.Time) (Group, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, err := t.openTx(tx.seq)
	if err != nil {
		return Group{}, err
	}
	t.nextGroup++
	t.groups[t.nextGroup] = &groupEntry{tx: tx.seq, readAt: readAt}
	e.openGroups++
	return Group{tx: tx.seq, id: t.nextGroup}, nil
}

func (t *AckTracker) AddToGroup(g Group) (pipeline.AckToken, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ge, err := t.openGroup(g)
	if err != nil {
		return pipeline.AckToken{}, err
	}
	ge.pending++
	t.ring.get(g.tx).pending++
	return pipeline.AckToken{Tx: g.tx, Group: g.id}, nil
}

func (t *AckTracker) SealGroup(g Group) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ge, err := t.openGroup(g)
	if err != nil {
		return err
	}
	ge.sealed = true
	t.ring.get(g.tx).openGroups--
	if ge.pending == 0 {
		t.finishGroup(g.id, ge)
	}
	return nil
}

func (t *AckTracker) Seal(tx TxHandle) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, err := t.openTx(tx.seq)
	if err != nil {
		return err
	}
	if e.openGroups > 0 {
		return fmt.Errorf("%w: %d open", ErrGroupOpen, e.openGroups)
	}
	e.sealed = true
	if e.pending == 0 {
		t.completeTx(e)
	}
	return nil
}

// Ack acknowledges one delivery.
func (t *AckTracker) Ack(tok pipeline.AckToken) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.ring.get(tok.Tx)
	if e == nil || e.pending == 0 {
		return fmt.Errorf("%w: tx %d", ErrUnknownDelivery, tok.Tx)
	}
	if err := t.ackPart(e, tok); err != nil {
		return err
	}
	e.pending--
	if e.sealed && e.pending == 0 {
		t.completeTx(e)
	}
	return nil
}

func (t *AckTracker) Confirmed() lsn.LSN {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.confirmed
}

func (t *AckTracker) ConfirmedChanged() <-chan struct{} { return t.changed }

func (t *AckTracker) Done(tx TxHandle) <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.ring.get(tx.seq)
	if e == nil || e.complete {
		return t.closed
	}
	if e.done == nil {
		e.done = make(chan struct{})
	}
	return e.done
}

func (t *AckTracker) GroupDone(g Group) <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	ge := t.groups[g.id]
	if ge == nil {
		return t.closed
	}
	if ge.done == nil {
		ge.done = make(chan struct{})
	}
	return ge.done
}

func (t *AckTracker) OldestUnacked() (readAt time.Time, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range t.ring.len() {
		if e := t.ring.at(i); e.ungrouped > 0 {
			readAt, ok = e.readAt, true
			break
		}
	}
	for _, ge := range t.groups {
		if ge.pending > 0 && (!ok || ge.readAt.Before(readAt)) {
			readAt, ok = ge.readAt, true
		}
	}
	return readAt, ok
}

func (t *AckTracker) InFlight() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ring.len()
}

func (t *AckTracker) openTx(seq uint64) (*txEntry, error) {
	e := t.ring.get(seq)
	switch {
	case e == nil || e.complete:
		return nil, fmt.Errorf("%w: tx %d", ErrUnknownTx, seq)
	case e.sealed:
		return nil, fmt.Errorf("%w: tx %d", ErrSealed, seq)
	default:
		return e, nil
	}
}

func (t *AckTracker) openGroup(g Group) (*groupEntry, error) {
	ge := t.groups[g.id]
	switch {
	case ge == nil || ge.tx != g.tx:
		return nil, fmt.Errorf("%w: group %d", ErrUnknownGroup, g.id)
	case ge.sealed:
		return nil, fmt.Errorf("%w: group %d", ErrSealed, g.id)
	default:
		return ge, nil
	}
}

func (t *AckTracker) ackPart(e *txEntry, tok pipeline.AckToken) error {
	if tok.Group == 0 {
		if e.ungrouped == 0 {
			return fmt.Errorf("%w: tx %d", ErrUnknownDelivery, tok.Tx)
		}
		e.ungrouped--
		return nil
	}
	ge := t.groups[tok.Group]
	if ge == nil || ge.tx != tok.Tx || ge.pending == 0 {
		return fmt.Errorf("%w: tx %d group %d", ErrUnknownDelivery, tok.Tx, tok.Group)
	}
	ge.pending--
	if ge.sealed && ge.pending == 0 {
		t.finishGroup(tok.Group, ge)
	}
	return nil
}

func (t *AckTracker) finishGroup(id uint64, ge *groupEntry) {
	if ge.done != nil {
		close(ge.done)
	}
	delete(t.groups, id)
}

func (t *AckTracker) completeTx(e *txEntry) {
	e.complete = true
	if e.done != nil {
		close(e.done)
		e.done = nil
	}
	advanced := false
	for t.ring.len() > 0 && t.ring.at(0).complete {
		t.confirmed = t.ring.at(0).end
		t.ring.popFront()
		advanced = true
	}
	if !advanced {
		return
	}
	select {
	case t.changed <- struct{}{}:
	default:
	}
	if t.space != nil {
		close(t.space)
		t.space = nil
	}
}
