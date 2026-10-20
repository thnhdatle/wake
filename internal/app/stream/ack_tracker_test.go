// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

var epoch = time.Unix(1_760_000_000, 0)

func newTracker(t *testing.T, start lsn.LSN, opts ...AckTrackerOption) *AckTracker {
	t.Helper()
	tr, err := NewAckTracker(start, opts...)
	require.NoError(t, err)
	return tr
}

func begin(t *testing.T, tr *AckTracker, commit, end lsn.LSN) TxHandle {
	t.Helper()
	tx, err := tr.Begin(context.Background(), commit, end, epoch)
	require.NoError(t, err)
	return tx
}

func add(t *testing.T, tr *AckTracker, tx TxHandle) pipeline.AckToken {
	t.Helper()
	tok, err := tr.Add(tx)
	require.NoError(t, err)
	return tok
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestNewAckTracker_InvalidMaxInFlight(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, -1} {
		_, err := NewAckTracker(0, WithMaxInFlight(n))
		require.ErrorIs(t, err, ErrInvalidOption)
	}
}

func TestAckTracker_Begin_OutOfOrder(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		commit, end lsn.LSN
		wantErr     bool
	}{
		{name: "commit before start", commit: 99, end: 120, wantErr: true},
		{name: "end before commit", commit: 130, end: 129, wantErr: true},
		{name: "commit at previous end", commit: 120, end: 120},
		{name: "commit after previous end", commit: 125, end: 140},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := newTracker(t, 100)
			begin(t, tr, 110, 120)
			_, err := tr.Begin(context.Background(), tc.commit, tc.end, epoch)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrOutOfOrder)
				assert.Equal(t, 1, tr.InFlight())
			} else {
				require.NoError(t, err)
				assert.Equal(t, 2, tr.InFlight())
			}
		})
	}
}

func TestAckTracker_Confirmed_ContiguousPrefix(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100)
	assert.Equal(t, lsn.LSN(100), tr.Confirmed())
	tx1 := begin(t, tr, 110, 120)
	a1 := add(t, tr, tx1)
	b1 := add(t, tr, tx1)
	require.NoError(t, tr.Seal(tx1))
	tx2 := begin(t, tr, 130, 140)
	a2 := add(t, tr, tx2)
	require.NoError(t, tr.Seal(tx2))

	require.NoError(t, tr.Ack(a2))
	assert.Equal(t, lsn.LSN(100), tr.Confirmed(), "gap at tx1")
	assert.True(t, isClosed(tr.Done(tx2)), "tx2 itself is complete")
	assert.False(t, isClosed(tr.Done(tx1)))
	require.NoError(t, tr.Ack(a1))
	assert.Equal(t, lsn.LSN(100), tr.Confirmed(), "tx1 still waits for its second sink")
	require.NoError(t, tr.Ack(b1))
	assert.Equal(t, lsn.LSN(140), tr.Confirmed())
	assert.Equal(t, 0, tr.InFlight())
	assert.True(t, isClosed(tr.Done(tx1)))
	assert.True(t, isClosed(tr.ConfirmedChanged()))
}

func TestAckTracker_Seal_UnroutedIsAcknowledged(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100)
	tx := begin(t, tr, 110, 120)
	assert.Equal(t, lsn.LSN(100), tr.Confirmed(), "not sealed yet")
	require.NoError(t, tr.Seal(tx))
	assert.Equal(t, lsn.LSN(120), tr.Confirmed())
}

func TestAckTracker_PseudoTransactionOrderedFirst(t *testing.T) {
	t.Parallel()
	const cp = lsn.LSN(100)
	tr := newTracker(t, cp)
	initial := begin(t, tr, cp, cp)
	g, err := tr.NewGroup(initial, epoch)
	require.NoError(t, err)
	r1, err := tr.AddToGroup(g)
	require.NoError(t, err)
	require.NoError(t, tr.SealGroup(g))

	stx := begin(t, tr, 110, 120)
	require.NoError(t, tr.Seal(stx))
	assert.Equal(t, cp, tr.Confirmed(), "stream tx waits for the initial snapshot")

	require.NoError(t, tr.Ack(r1))
	assert.True(t, isClosed(tr.GroupDone(g)))
	assert.Equal(t, cp, tr.Confirmed(), "pseudo-transaction not sealed")
	require.NoError(t, tr.Seal(initial))
	assert.Equal(t, lsn.LSN(120), tr.Confirmed())
}

func TestAckTracker_HighAttachmentAfterLaterTransactions(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100)
	high := begin(t, tr, 110, 120)
	later := begin(t, tr, 130, 140)
	l1 := add(t, tr, later)
	require.NoError(t, tr.Seal(later))
	require.NoError(t, tr.Ack(l1))
	assert.Equal(t, lsn.LSN(100), tr.Confirmed())

	g, err := tr.NewGroup(high, epoch.Add(time.Second))
	require.NoError(t, err)
	r1, err := tr.AddToGroup(g)
	require.NoError(t, err)
	r2, err := tr.AddToGroup(g)
	require.NoError(t, err)
	require.NoError(t, tr.SealGroup(g))
	require.NoError(t, tr.Seal(high))
	done := tr.GroupDone(g)
	require.NoError(t, tr.Ack(r2))
	assert.False(t, isClosed(done))
	require.NoError(t, tr.Ack(r1))
	assert.True(t, isClosed(done), "chunk complete")
	assert.Equal(t, lsn.LSN(140), tr.Confirmed())
}

func TestAckTracker_Errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		run  func(t *testing.T, tr *AckTracker) error
		want error
	}{
		{name: "add to sealed", want: ErrSealed, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			add(t, tr, tx)
			require.NoError(t, tr.Seal(tx))
			_, err := tr.Add(tx)
			return err
		}},
		{name: "add to completed", want: ErrUnknownTx, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			require.NoError(t, tr.Seal(tx))
			_, err := tr.Add(tx)
			return err
		}},
		{name: "add to zero handle", want: ErrUnknownTx, run: func(_ *testing.T, tr *AckTracker) error {
			_, err := tr.Add(TxHandle{})
			return err
		}},
		{name: "seal twice", want: ErrSealed, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			add(t, tr, tx)
			require.NoError(t, tr.Seal(tx))
			return tr.Seal(tx)
		}},
		{name: "seal with open group", want: ErrGroupOpen, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			_, err := tr.NewGroup(tx, epoch)
			require.NoError(t, err)
			return tr.Seal(tx)
		}},
		{name: "group in sealed tx", want: ErrSealed, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			add(t, tr, tx)
			require.NoError(t, tr.Seal(tx))
			_, err := tr.NewGroup(tx, epoch)
			return err
		}},
		{name: "add to sealed group", want: ErrSealed, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			g, err := tr.NewGroup(tx, epoch)
			require.NoError(t, err)
			_, err = tr.AddToGroup(g)
			require.NoError(t, err)
			require.NoError(t, tr.SealGroup(g))
			_, err = tr.AddToGroup(g)
			return err
		}},
		{name: "seal group twice", want: ErrSealed, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			g, err := tr.NewGroup(tx, epoch)
			require.NoError(t, err)
			_, err = tr.AddToGroup(g)
			require.NoError(t, err)
			require.NoError(t, tr.SealGroup(g))
			return tr.SealGroup(g)
		}},
		{name: "completed group", want: ErrUnknownGroup, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			g, err := tr.NewGroup(tx, epoch)
			require.NoError(t, err)
			require.NoError(t, tr.SealGroup(g))
			_, err = tr.AddToGroup(g)
			return err
		}},
		{name: "zero group", want: ErrUnknownGroup, run: func(_ *testing.T, tr *AckTracker) error {
			return tr.SealGroup(Group{})
		}},
		{name: "ack twice", want: ErrUnknownDelivery, run: func(t *testing.T, tr *AckTracker) error {
			t.Helper()
			tx := begin(t, tr, 110, 120)
			a := add(t, tr, tx)
			add(t, tr, tx)
			require.NoError(t, tr.Ack(a))
			require.NoError(t, tr.Ack(a), "second delivery of the same tx is still pending")
			return tr.Ack(a)
		}},
		{name: "ack unknown tx", want: ErrUnknownDelivery, run: func(_ *testing.T, tr *AckTracker) error {
			return tr.Ack(pipeline.AckToken{Tx: 42})
		}},
		{
			name: "ungrouped ack with only grouped pending", want: ErrUnknownDelivery,
			run: func(t *testing.T, tr *AckTracker) error {
				t.Helper()
				tx := begin(t, tr, 110, 120)
				g, err := tr.NewGroup(tx, epoch)
				require.NoError(t, err)
				tok, err := tr.AddToGroup(g)
				require.NoError(t, err)
				return tr.Ack(pipeline.AckToken{Tx: tok.Tx})
			},
		},
		{
			name: "group ack with wrong group", want: ErrUnknownDelivery,
			run: func(t *testing.T, tr *AckTracker) error {
				t.Helper()
				tx := begin(t, tr, 110, 120)
				tok := add(t, tr, tx)
				return tr.Ack(pipeline.AckToken{Tx: tok.Tx, Group: 7})
			},
		},
		{
			name: "group ack of drained group", want: ErrUnknownDelivery,
			run: func(t *testing.T, tr *AckTracker) error {
				t.Helper()
				tx := begin(t, tr, 110, 120)
				g, err := tr.NewGroup(tx, epoch)
				require.NoError(t, err)
				tok, err := tr.AddToGroup(g)
				require.NoError(t, err)
				add(t, tr, tx)
				require.NoError(t, tr.Ack(tok))
				return tr.Ack(tok)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := newTracker(t, 100)
			require.ErrorIs(t, tc.run(t, tr), tc.want)
		})
	}
}

func TestAckTracker_SealGroup_EmptyGroupDoneAtOnce(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100)
	tx := begin(t, tr, 110, 120)
	g, err := tr.NewGroup(tx, epoch)
	require.NoError(t, err)
	done := tr.GroupDone(g)
	assert.False(t, isClosed(done))
	require.NoError(t, tr.SealGroup(g))
	assert.True(t, isClosed(done))
	assert.True(t, isClosed(tr.GroupDone(g)), "finished groups report done")
	assert.True(t, isClosed(tr.GroupDone(Group{})))
	require.NoError(t, tr.Seal(tx))
	assert.Equal(t, lsn.LSN(120), tr.Confirmed())
}

func TestAckTracker_Done(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100)
	tx := begin(t, tr, 110, 120)
	tok := add(t, tr, tx)
	done := tr.Done(tx)
	assert.Equal(t, done, tr.Done(tx), "one channel per transaction")
	require.NoError(t, tr.Seal(tx))
	assert.False(t, isClosed(done))
	require.NoError(t, tr.Ack(tok))
	assert.True(t, isClosed(done))
	assert.True(t, isClosed(tr.Done(tx)), "confirmed transactions report done")
	assert.True(t, isClosed(tr.Done(TxHandle{})))
}

func TestAckTracker_OldestUnacked(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100)
	_, ok := tr.OldestUnacked()
	assert.False(t, ok)

	high := begin(t, tr, 110, 120)
	_, ok = tr.OldestUnacked()
	assert.False(t, ok, "an open transaction without deliveries has nothing unacknowledged")

	later, err := tr.Begin(context.Background(), 130, 140, epoch.Add(2*time.Second))
	require.NoError(t, err)
	l1 := add(t, tr, later)
	at, ok := tr.OldestUnacked()
	require.True(t, ok)
	assert.Equal(t, epoch.Add(2*time.Second), at)

	g, err := tr.NewGroup(high, epoch.Add(5*time.Second))
	require.NoError(t, err)
	r1, err := tr.AddToGroup(g)
	require.NoError(t, err)
	at, _ = tr.OldestUnacked()
	assert.Equal(t, epoch.Add(2*time.Second), at, "newer group does not hide an older delivery")

	require.NoError(t, tr.Ack(l1))
	at, ok = tr.OldestUnacked()
	require.True(t, ok)
	assert.Equal(t, epoch.Add(5*time.Second), at, "group deliveries age from the group")

	g2, err := tr.NewGroup(high, epoch.Add(time.Second))
	require.NoError(t, err)
	_, err = tr.AddToGroup(g2)
	require.NoError(t, err)
	at, _ = tr.OldestUnacked()
	assert.Equal(t, epoch.Add(time.Second), at, "older group wins")

	require.NoError(t, tr.Ack(r1))
	at, _ = tr.OldestUnacked()
	assert.Equal(t, epoch.Add(time.Second), at)
}

func TestAckTracker_Begin_BlocksAtMaxInFlight(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 100, WithMaxInFlight(2))
	tx1 := begin(t, tr, 110, 120)
	tok := add(t, tr, tx1)
	require.NoError(t, tr.Seal(tx1))
	begin(t, tr, 130, 140)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := tr.Begin(ctx, 150, 160, epoch)
	require.ErrorIs(t, err, context.Canceled)

	type result struct {
		tx  TxHandle
		err error
	}
	got := make(chan result, 1)
	go func() {
		tx, err := tr.Begin(context.Background(), 150, 160, epoch)
		got <- result{tx, err}
	}()
	select {
	case <-got:
		t.Fatal("Begin did not block at the bound")
	default:
	}
	require.NoError(t, tr.Ack(tok))
	r := <-got
	require.NoError(t, r.err)
	assert.Equal(t, 2, tr.InFlight())
	assert.Equal(t, lsn.LSN(120), tr.Confirmed())
}

func TestAckTracker_RingGrowsAndWraps(t *testing.T) {
	t.Parallel()
	tr := newTracker(t, 0)
	var toks []pipeline.AckToken
	var end lsn.LSN
	for round := range 3 {
		toks = toks[:0]
		for i := range 40 {
			c := lsn.LSN(round*1000 + i*10 + 10)
			tx := begin(t, tr, c, c+5)
			toks = append(toks, add(t, tr, tx))
			require.NoError(t, tr.Seal(tx))
			end = c + 5
		}
		for i := len(toks) - 1; i >= 0; i-- {
			if i > 0 {
				require.NoError(t, tr.Ack(toks[i]))
				assert.Less(t, tr.Confirmed(), end)
			}
		}
		require.NoError(t, tr.Ack(toks[0]))
		assert.Equal(t, end, tr.Confirmed())
		assert.Equal(t, 0, tr.InFlight())
	}
}
