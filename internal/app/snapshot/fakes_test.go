// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakestate"
)

var errFakeSealed = errors.New("fake: sealed")

type fakeHeld struct {
	mu     sync.Mutex
	commit lsn.LSN
	groups []*fakeGroup
	sealed bool
}

func (h *fakeHeld) NewGroup(time.Time) (pipeline.TxGroup, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sealed {
		return nil, errFakeSealed
	}
	g := &fakeGroup{done: make(chan struct{})}
	h.groups = append(h.groups, g)
	return g, nil
}

func (h *fakeHeld) Seal() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sealed {
		return errFakeSealed
	}
	h.sealed = true
	return nil
}

func (h *fakeHeld) isSealed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sealed
}

func (h *fakeHeld) allGroups() []*fakeGroup {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.groups)
}

func (h *fakeHeld) records() []pipeline.Record {
	var out []pipeline.Record
	for _, g := range h.allGroups() {
		out = append(out, g.all()...)
	}
	return out
}

type fakeGroup struct {
	mu       sync.Mutex
	recs     []pipeline.Record
	sealed   bool
	done     chan struct{}
	closed   bool
	routeErr error
}

func (g *fakeGroup) Route(_ context.Context, rec pipeline.Record) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.routeErr != nil {
		return 0, g.routeErr
	}
	g.recs = append(g.recs, rec)
	return 1, nil
}

func (g *fakeGroup) Seal() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sealed = true
	if len(g.recs) == 0 {
		g.closeLocked()
	}
	return nil
}

func (g *fakeGroup) Done() <-chan struct{} { return g.done }

func (g *fakeGroup) ack() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closeLocked()
}

func (g *fakeGroup) closeLocked() {
	if !g.closed {
		g.closed = true
		close(g.done)
	}
}

func (g *fakeGroup) all() []pipeline.Record {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.recs)
}

type fakeOpener struct {
	mu   sync.Mutex
	err  error
	txs  []*fakeHeld
	ends []lsn.LSN
}

func (o *fakeOpener) OpenTx(_ context.Context, commit, end lsn.LSN, _ time.Time) (pipeline.HeldTx, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return nil, o.err
	}
	h := &fakeHeld{commit: commit}
	o.txs = append(o.txs, h)
	o.ends = append(o.ends, end)
	return h, nil
}

func (o *fakeOpener) first(t *testing.T) *fakeHeld {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	require.NotEmpty(t, o.txs)
	return o.txs[0]
}

type fixedVersions uint64

func (v fixedVersions) Version(string) uint64 { return uint64(v) }

type staticTables struct {
	tables []Table
	err    error
}

func (s staticTables) CapturedTables(context.Context) ([]Table, error) { return s.tables, s.err }

func table(name string) Table {
	return Table{
		Name: "public." + name, Schema: "public", Table: name,
		Source: change.Source{DB: "app", Schema: "public", Table: name},
	}
}

type recMetrics struct {
	mu       sync.Mutex
	rows     map[string]uint64
	requests []string
	retries  []string
}

func (m *recMetrics) RowsAcked(t string, n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rows == nil {
		m.rows = map[string]uint64{}
	}
	m.rows[t] += n
}

func (m *recMetrics) Request(t string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, t)
}

func (m *recMetrics) ChunkRetry(t string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries = append(m.retries, t)
}

func (m *recMetrics) get() (map[string]uint64, []string, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := map[string]uint64{}
	for k, v := range m.rows {
		rows[k] = v
	}
	return rows, slices.Clone(m.requests), slices.Clone(m.retries)
}

func keeperWith(t *testing.T, st pipeline.State) (*StateKeeper, *fakestate.Store) {
	t.Helper()
	store := fakestate.NewWith(st)
	k, err := NewStateKeeper(store, &st)
	require.NoError(t, err)
	return k, store
}

type fakeStream struct {
	mu       sync.Mutex
	obs      pipeline.TxObserver
	xid      uint32
	at       lsn.LSN
	held     []*fakeHeld
	failNext int
	failErr  error
	autoAck  bool
	errs     []error
}

func newFakeStream() *fakeStream { return &fakeStream{xid: 100, at: 0x10000, autoAck: true} }

func (f *fakeStream) next() (uint32, lsn.LSN, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return 0, 0, f.failErr
	}
	f.xid++
	f.at += 0x100
	return f.xid, f.at, nil
}

func (f *fakeStream) EmitMessage(ctx context.Context, prefix string, content []byte) (lsn.LSN, error) {
	xid, at, err := f.next()
	if err != nil {
		return 0, err
	}
	f.tx(ctx, xid, at, func() { f.obs.ObserveMessage(prefix, content) })
	return at, nil
}

func (f *fakeStream) change(ctx context.Context, table string, keys ...string) {
	xid, at, _ := f.next()
	f.tx(ctx, xid, at, func() {
		for _, k := range keys {
			f.obs.ObserveChange(table, []byte(k), nil)
		}
	})
}

func (f *fakeStream) tx(ctx context.Context, xid uint32, at lsn.LSN, body func()) {
	f.obs.ObserveBegin(pipeline.ObservedTx{XID: xid, CommitLSN: at, TsMs: int64(at)})
	body()
	hold, err := f.obs.ObserveCommit(ctx)
	if err == nil && hold {
		h := &fakeHeld{commit: at}
		f.mu.Lock()
		f.held = append(f.held, h)
		f.mu.Unlock()
		err = f.obs.ObserveHeld(ctx, h)
		if f.autoAck {
			for _, g := range h.allGroups() {
				g.ack()
			}
		}
	}
	if err != nil {
		f.mu.Lock()
		f.errs = append(f.errs, err)
		f.mu.Unlock()
	}
}

func (f *fakeStream) heldTxs() []*fakeHeld {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.held)
}

func (f *fakeStream) reads() []pipeline.Record {
	var out []pipeline.Record
	for _, h := range f.heldTxs() {
		out = append(out, h.records()...)
	}
	return out
}
