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

// DefaultMaxInFlight is the default bound on registered, not yet confirmed
// transactions.
const DefaultMaxInFlight = 1 << 16

// TxHandle identifies a transaction registered with AckTracker.Begin. The
// zero handle is invalid.
type TxHandle struct{ seq uint64 }

// Group identifies a completion group inside a transaction (one snapshot
// chunk). The zero group is invalid.
type Group struct{ tx, id uint64 }

// groupEntry is one open completion group.
type groupEntry struct {
	tx      uint64
	readAt  time.Time
	pending int
	sealed  bool
	done    chan struct{}
}

// AckTrackerOption configures an AckTracker.
type AckTrackerOption func(*AckTracker)

// WithMaxInFlight bounds the transactions registered and not yet confirmed;
// Begin blocks while the bound is reached (default DefaultMaxInFlight).
func WithMaxInFlight(n int) AckTrackerOption {
	return func(t *AckTracker) { t.maxInFlight = n }
}

// AckTracker computes the confirmed LSN: the end
// LSN of the latest transaction T such that T and every transaction
// registered before it are sealed and have every delivery acknowledged. It
// never skips a gap.
//
// Transactions are registered with Begin in commit (stream) order: the
// initial-snapshot pseudo-transaction at the consistent point first, then
// stream transactions and non-transactional forwarded messages (a
// single-record transaction at the message LSN). Deliveries (record × sink)
// are added with Add or, for snapshot chunks, AddToGroup; a transaction may
// receive deliveries after later transactions were registered (the HIGH
// watermark transaction receives its chunk's `r` events) until it is sealed.
// A sealed transaction without deliveries is acknowledged at once. Sinks
// acknowledge through Ack in any order.
//
// Memory: one entry per registered, unconfirmed transaction in a ring
// (O(1) amortized Begin, Add and Ack), bounded by WithMaxInFlight, plus one
// entry per open group. Begin blocks when the bound is reached; sinks'
// bounded buffers bound the pending deliveries themselves.
//
// AckTracker is safe for concurrent use; Begin is called by one goroutine
// (the reader), so that registration follows commit order.
type AckTracker struct {
	mu          sync.Mutex // protects every field below except the immutable channels
	ring        txRing
	groups      map[uint64]*groupEntry
	nextGroup   uint64
	confirmed   lsn.LSN
	lastEnd     lsn.LSN       // end LSN of the last registered transaction, or the start
	space       chan struct{} // non-nil while Begin waits; closed when a slot frees
	maxInFlight int

	changed chan struct{} // capacity 1: coalesced signal of confirmed advances
	closed  chan struct{} // always closed: Done of completed transactions and groups
}

// NewAckTracker returns a tracker whose confirmed LSN starts at start, the
// slot's confirmed_flush_lsn (or its consistent point when just created).
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

// Begin registers the next transaction in commit order. commit and end are
// its commit LSN and end LSN (both the consistent point for the
// initial-snapshot pseudo-transaction; the message LSN for a
// non-transactional message); readAt is when Wake read it. It returns
// ErrOutOfOrder unless commit ≥ the previous end and end ≥ commit, and
// blocks while WithMaxInFlight transactions are unconfirmed.
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

// Add adds one delivery outside any group to an unsealed transaction and
// returns its token.
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

// NewGroup opens a completion group in an unsealed transaction, for the `r`
// events of one snapshot chunk. Its deliveries age from readAt.
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

// AddToGroup adds one delivery to an unsealed group and returns its token.
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

// SealGroup closes the group to further deliveries; it completes once its
// deliveries are acknowledged (at once when it has none).
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

// Seal closes the transaction to further deliveries; it returns ErrGroupOpen
// while one of its groups is unsealed.
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

// Ack acknowledges one delivery; it implements pipeline.Acker. It returns
// ErrUnknownDelivery for a token that is not pending.
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

// Confirmed returns the confirmed LSN.
func (t *AckTracker) Confirmed() lsn.LSN {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.confirmed
}

// ConfirmedChanged returns a channel that receives after the confirmed LSN
// advanced. Signals coalesce (capacity 1, never blocks the tracker), so it
// serves a single consumer, the standby status updater.
func (t *AckTracker) ConfirmedChanged() <-chan struct{} { return t.changed }

// Done returns a channel closed once the transaction is sealed and every
// delivery acknowledged, independent of earlier transactions.
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

// GroupDone returns a channel closed once the group is sealed and every
// delivery acknowledged: the chunk is complete.
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

// OldestUnacked returns the read time of the oldest delivery not yet
// acknowledged, for wake_oldest_unacked_age_seconds and lag.max_ack_age:
// the read time of the earliest transaction with pending deliveries outside
// groups, or of the earliest group with pending deliveries, whichever is
// older. ok is false when nothing is pending.
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

// InFlight returns the number of registered transactions not yet confirmed.
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

// ackPart acknowledges the delivery in its group, or among e's ungrouped
// deliveries, after checking that one is pending.
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

// completeTx marks e complete and advances the confirmed LSN over the
// complete prefix of the ring.
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
