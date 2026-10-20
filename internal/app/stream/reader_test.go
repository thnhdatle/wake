// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil/fakerepl"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
	"github.com/thnhdatle/wake/internal/testutil/fakespill"
)

func TestNewReader_Invalid(t *testing.T) {
	t.Parallel()
	tr, err := NewAckTracker(0)
	require.NoError(t, err)
	router, err := NewRouter(tr, nil)
	require.NoError(t, err)
	src := fakerepl.New(1)
	clk := clock.NewFake(time.Unix(0, 0))
	status, err := NewStatusUpdater(src, tr, clk)
	require.NoError(t, err)
	schemas, err := NewSchemas(schema.RegistryState{})
	require.NoError(t, err)
	policy, err := pipeline.NewTablePolicy(pipeline.TablePolicyConfig{DB: "app"})
	require.NoError(t, err)
	full := ReaderDeps{
		Source: src, Router: router, Status: status, Schemas: schemas, Tables: policy, Spill: fakespill.New(), Clock: clk,
	}
	cfg := ReaderConfig{Slot: "wake_app", Publications: []string{"wake_app"}}
	cases := []struct {
		name string
		deps func(d ReaderDeps) ReaderDeps
		cfg  func(c ReaderConfig) ReaderConfig
		opts []ReaderOption
	}{
		{name: "nil source", deps: func(d ReaderDeps) ReaderDeps { d.Source = nil; return d }},
		{name: "nil spill", deps: func(d ReaderDeps) ReaderDeps { d.Spill = nil; return d }},
		{name: "no slot", cfg: func(c ReaderConfig) ReaderConfig { c.Slot = ""; return c }},
		{name: "no publications", cfg: func(c ReaderConfig) ReaderConfig { c.Publications = nil; return c }},
		{name: "negative max tx bytes", cfg: func(c ReaderConfig) ReaderConfig { c.MaxTxBytes = -1; return c }},
		{name: "nil logger", opts: []ReaderOption{WithReaderLogger(nil)}},
		{name: "nil metrics", opts: []ReaderOption{WithReaderMetrics(nil)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, c := full, cfg
			if tc.deps != nil {
				d = tc.deps(d)
			}
			if tc.cfg != nil {
				c = tc.cfg(c)
			}
			_, nerr := NewReader(d, c, tc.opts...)
			require.ErrorIs(t, nerr, ErrInvalidOption)
		})
	}
	r, err := NewReader(full, cfg)
	require.NoError(t, err)
	assert.Equal(t, DefaultProtoVersion, r.cfg.ProtoVersion)
	assert.Equal(t, int64(DefaultMaxTxBytes), r.cfg.MaxTxBytes)
}

func TestReader_Run_StartsReplicationAndRemovesSpill(t *testing.T) {
	t.Parallel()
	f := newFixture(t, fixtureOpts{cfg: ReaderConfig{Start: 0x100, ProtoVersion: 1}})
	f.spill.Seed("crashed", []byte("x"))
	f.start(t)
	require.ErrorIs(t, f.finish(t), fakerepl.ErrClosed)
	assert.Empty(t, f.spill.Files())
	assert.Equal(t, 1, f.spill.RemoveAlls())
	assert.Equal(t, []pipeline.StartOptions{{
		Slot: "wake_app", Start: 0x100, ProtoVersion: 1, Publications: []string{"wake_app"}, Messages: true,
	}}, f.src.Started())
}

func TestReader_Run_ConfirmedAdvancesOnlyAfterAcks(t *testing.T) {
	t.Parallel()
	msgs := readRecorded(t, "identity.hex")
	f := newFixture(t, fixtureOpts{cfg: ReaderConfig{Start: 0x1000}})
	f.start(t)
	f.push(t, msgs...)
	require.ErrorIs(t, f.finish(t), fakerepl.ErrClosed)
	recs := f.sink.Received()
	require.Len(t, recs, 10)
	assert.Equal(t, lsn.LSN(0x1000), f.tracker.Confirmed(), "nothing acknowledged yet")
	// Acknowledging the first transaction's record confirms its end LSN.
	_, err := f.sink.AckNext(1)
	require.NoError(t, err)
	assert.Equal(t, mustLSN(t, "0/197CEC8"), f.tracker.Confirmed())
	// Out-of-order acknowledgements never skip the gap.
	require.NoError(t, f.sink.AckAt(2))
	assert.Equal(t, mustLSN(t, "0/197CEC8"), f.tracker.Confirmed())
	require.NoError(t, f.sink.AckAll())
	// The keyless transaction is unrouted (skipped table) and counts as
	// acknowledged at Commit, so the last end LSN is confirmed.
	assert.Equal(t, mustLSN(t, "0/197D4C0"), f.tracker.Confirmed())
}

func TestReader_Run_KeepaliveReplyAndReceivedPosition(t *testing.T) {
	t.Parallel()
	f := newFixture(t, fixtureOpts{})
	ctx := context.Background()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	statusDone := make(chan error, 1)
	go func() { statusDone <- f.status.Run(runCtx) }()
	f.start(t)
	require.NoError(t, f.src.Push(ctx, pipeline.ReplicationMessage{
		Kind: pipeline.ReplicationMessageKeepalive, Keepalive: pipeline.Keepalive{ServerWALEnd: 0x5000, ReplyRequested: true},
	}))
	got, err := f.src.WaitStatuses(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, lsn.LSN(0x5000), got[0].Written)
	cancel()
	require.NoError(t, <-statusDone)
}

func TestReader_Run_SpillProducesIdenticalOutput(t *testing.T) {
	t.Parallel()
	for _, file := range []string{"multi.hex", "toast.hex", "schema.hex"} {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			msgs := readRecorded(t, file)
			cfg := ReaderConfig{EmitTruncate: true, EmitMessages: true}
			want, _ := runAll(t, fixtureOpts{cfg: cfg, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}}, msgs)
			cfg.MaxTxBytes = 1
			metrics := &fakeMetrics{}
			got, f := runAll(t, fixtureOpts{
				cfg: cfg, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()},
				readerOpts: []ReaderOption{WithReaderMetrics(metrics)},
			}, msgs)
			assert.Equal(t, render(want), render(got))
			created, removed := f.spill.Counts()
			assert.Positive(t, created)
			assert.Equal(t, created, removed, "every spill file removed after dispatch")
			assert.Empty(t, f.spill.Files())
			assert.Equal(t, created, metrics.spilled)
		})
	}
}

func TestReader_Run_SpillErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("disk full")
	cases := []struct {
		name string
		fail func(s *fakespill.Store)
	}{
		{name: "create", fail: func(s *fakespill.Store) { s.FailCreate(boom) }},
		{name: "append", fail: func(s *fakespill.Store) { s.FailAppend(boom) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, fixtureOpts{cfg: ReaderConfig{MaxTxBytes: 1}})
			tc.fail(f.spill)
			f.start(t)
			f.push(t, readRecorded(t, "multi.hex")...)
			require.ErrorIs(t, f.finish(t), boom)
		})
	}
}

func TestReader_Run_GateHoldsAndPausesUntilReleased(t *testing.T) {
	t.Parallel()
	msgs := readRecorded(t, "identity.hex")
	gate := newFakeGate()
	f := newFixture(t, fixtureOpts{
		cfg:      ReaderConfig{MaxTxBytes: 600},
		sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}, readerOpts: []ReaderOption{WithGate(gate)},
	})
	f.start(t)
	f.push(t, msgs...)
	ctx := context.Background()
	// Reading pauses once the held-back transactions reach max_tx_bytes:
	// the script is not drained and nothing reaches the sink.
	pushed := make(chan error, 1)
	go func() {
		pushed <- f.src.Push(ctx, pipeline.ReplicationMessage{Kind: pipeline.ReplicationMessageKeepalive})
	}()
	require.NoError(t, <-pushed)
	assert.Empty(t, f.sink.Received())
	assert.Equal(t, lsn.InvalidLSN, f.tracker.Confirmed())
	gate.release()
	require.NoError(t, f.sink.WaitReceived(ctx, 10))
	require.ErrorIs(t, f.finish(t), fakerepl.ErrClosed)
	assert.Equal(t, mustLSN(t, "0/197D4C0"), f.tracker.Confirmed())
	want, _ := runAll(t, fixtureOpts{sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}}, msgs)
	assert.Equal(t, render(want), render(f.sink.Received()))
}

func TestReader_Run_GateReleasedWhileIdleFlushes(t *testing.T) {
	t.Parallel()
	gate := newFakeGate()
	f := newFixture(t, fixtureOpts{
		sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}, readerOpts: []ReaderOption{WithGate(gate)},
	})
	f.start(t)
	f.push(t, readRecorded(t, "types.hex")...)
	// The reader is now blocked in Receive with transactions held back; the
	// release alone dispatches them.
	gate.release()
	require.NoError(t, f.sink.WaitReceived(context.Background(), 3))
	require.ErrorIs(t, f.finish(t), fakerepl.ErrClosed)
}

func TestReader_Run_SkipsTransactionsBeforeStart(t *testing.T) {
	t.Parallel()
	msgs := readRecorded(t, "multi.hex")
	recs, _ := runAll(t, fixtureOpts{
		cfg:      ReaderConfig{Start: mustLSN(t, "0/197D7B0"), EmitTruncate: true, EmitMessages: true},
		sinkOpts: []fakesink.Option{fakesink.WithAutoAck()},
	}, msgs)
	// The first transaction and the non-transactional message precede the
	// start; the heartbeat transaction starts exactly at it.
	assert.Equal(t, []string{"0/197DDB0-0"}, ids(recs))
}

func TestReader_Run_Metrics(t *testing.T) {
	t.Parallel()
	metrics := &fakeMetrics{}
	activity := &fakeActivity{}
	msgs := make([]walMsg, 0, 64)
	msgs = append(msgs, readRecorded(t, "types.hex")...)
	msgs = append(msgs, readRecorded(t, "identity.hex")...)
	msgs = append(msgs, readRecorded(t, "multi.hex")...)
	// The keyless table again: its skip is logged and counted once only.
	s := synth{at: 0x2000000}
	s.begin(0x2000100, 900)
	s.relation(16425, "rec", "keyless", 'n', "b")
	s.insert(16425, "1", "k")
	s.commit(0x2000100, 0x2000110)
	msgs = append(msgs, s.msgs...)
	msgs = append(msgs, walMsg{at: 0x2000200, data: []byte{'X', 1, 2}}, walMsg{at: 0x2000201, data: []byte{'X'}})
	_, _ = runAll(t, fixtureOpts{
		sinkOpts:   []fakesink.Option{fakesink.WithAutoAck()},
		readerOpts: []ReaderOption{WithReaderMetrics(metrics), WithActivity(activity)},
	}, msgs)
	assert.Equal(t, []string{"rec.mood"}, metrics.types, "unknown type counted once")
	assert.Equal(t, []byte{'X'}, metrics.messages, "unknown message type counted once")
	assert.Equal(t, []string{"rec.evolve"}, metrics.truncates)
	assert.Equal(t, []string{"rec.keyless:table_keyless"}, metrics.skipped)
	assert.Positive(t, activity.count())
}

func TestReader_Run_Errors(t *testing.T) {
	t.Parallel()
	var nested, orphanCommit, orphanRow, unknownRel, txMsgOutside, plainMsgInside, stream, badRow synth
	nested.begin(0x100, 1)
	nested.begin(0x200, 2)
	orphanCommit.commit(0x100, 0x110)
	orphanRow.insert(1, "1")
	unknownRel.begin(0x100, 1)
	unknownRel.insert(99, "1")
	txMsgOutside.message(true, 0x100, "app", "x")
	plainMsgInside.begin(0x100, 1)
	plainMsgInside.message(false, 0x50, "app", "x")
	stream.add(wire{'S'}.u32(1).u8(1))
	badRow.begin(0x100, 1)
	badRow.relation(1, "public", "t", 'd', "v")
	badRow.insert(1, "1")
	cases := []struct {
		name string
		msgs []walMsg
		want error
	}{
		{name: "nested begin", msgs: nested.msgs, want: ErrProtocol},
		{name: "commit without begin", msgs: orphanCommit.msgs, want: ErrProtocol},
		{name: "row outside transaction", msgs: orphanRow.msgs, want: ErrProtocol},
		{name: "unknown relation", msgs: unknownRel.msgs},
		{name: "transactional message outside", msgs: txMsgOutside.msgs, want: ErrProtocol},
		{name: "plain message inside", msgs: plainMsgInside.msgs, want: ErrProtocol},
		{name: "streaming", msgs: stream.msgs},
		{name: "column count mismatch", msgs: badRow.msgs},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, fixtureOpts{})
			f.start(t)
			f.push(t, tc.msgs...)
			err := f.finish(t)
			require.Error(t, err)
			require.NotErrorIs(t, err, fakerepl.ErrClosed)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}

func TestReader_Run_UnknownReplicationKind(t *testing.T) {
	t.Parallel()
	f := newFixture(t, fixtureOpts{})
	f.start(t)
	require.NoError(t, f.src.Push(context.Background(), pipeline.ReplicationMessage{Kind: 9}))
	require.ErrorIs(t, f.finish(t), ErrProtocol)
}

// beginSignal closes began at the first ObserveBegin.
type beginSignal struct {
	fakeObserver
	began chan struct{}
}

func (b *beginSignal) ObserveBegin(tx pipeline.ObservedTx) {
	b.fakeObserver.ObserveBegin(tx)
	select {
	case <-b.began:
	default:
		close(b.began)
	}
}

func TestReader_Run_CancelReturnsNilAndRemovesSpill(t *testing.T) {
	t.Parallel()
	obs := &beginSignal{began: make(chan struct{})}
	f := newFixture(t, fixtureOpts{cfg: ReaderConfig{MaxTxBytes: 1}, readerOpts: []ReaderOption{WithObserver(obs)}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.reader.Run(ctx) }()
	var s synth
	s.begin(0x100, 1)
	s.relation(1, "public", "t", 'd', "v")
	s.insert(1, "1", "a")
	s.insert(1, "2", "b")
	f.push(t, s.msgs...)
	<-obs.began
	cancel()
	require.NoError(t, <-done)
	assert.Empty(t, f.spill.Files(), "spill file of the open transaction removed")
}

func TestReader_Run_CanceledBeforeStart(t *testing.T) {
	t.Parallel()
	f := newFixture(t, fixtureOpts{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, f.reader.Run(ctx))
}

func TestReader_Run_SchemaVersionsPersistedAfterConfirm(t *testing.T) {
	t.Parallel()
	store := &fakeSchemaStore{}
	f := newFixture(t, fixtureOpts{readerOpts: []ReaderOption{WithSchemaStore(store)}})
	f.start(t)
	f.push(t, readRecorded(t, "schema.hex")...)
	require.NoError(t, f.sink.WaitReceived(context.Background(), 9))
	assert.Empty(t, store.Saved(), "nothing confirmed yet")
	require.NoError(t, f.sink.AckAll())
	// The next message lets the reader see the confirmed LSN.
	require.NoError(t, f.src.Push(context.Background(), pipeline.ReplicationMessage{
		Kind: pipeline.ReplicationMessageKeepalive,
	}))
	require.ErrorIs(t, f.finish(t), fakerepl.ErrClosed)
	saved := store.Saved()
	require.NotEmpty(t, saved)
	last := saved[len(saved)-1]
	require.Len(t, last.Tables, 1)
	assert.Equal(t, "rec.evolve", last.Tables[0].Table)
	assert.Equal(t, uint64(5), last.Tables[0].Version)
}

func TestReader_Run_SchemaStoreError(t *testing.T) {
	t.Parallel()
	boom := errors.New("state file")
	store := &fakeSchemaStore{err: boom}
	f := newFixture(t, fixtureOpts{
		sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}, readerOpts: []ReaderOption{WithSchemaStore(store)},
	})
	f.start(t)
	f.push(t, readRecorded(t, "types.hex")...)
	require.ErrorIs(t, f.finish(t), boom)
}

func TestReader_Run_SeededSchemaDetectsChangeAcrossRestart(t *testing.T) {
	t.Parallel()
	seed := schema.RegistryState{Tables: []schema.TableState{{
		Table: "rec.evolve", Version: 7, Identity: schema.IdentityDefault,
		Columns: []schema.Column{{Name: "id", TypeOID: schema.OIDInt4, TypeMod: -1, Key: true}},
	}}}
	recs, _ := runAll(t, fixtureOpts{schemaState: seed, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()}},
		readRecorded(t, "schema.hex"))
	require.GreaterOrEqual(t, len(recs), 2)
	assert.Equal(t, "0/197DF98-s0", recs[0].ID.String(), "first Relation differs from the stored shape")
	assert.Equal(t, uint64(8), recs[0].SchemaVersion)
	assert.Equal(t, uint64(8), recs[1].SchemaVersion)
}

func TestReader_Run_KeyColumnsError(t *testing.T) {
	t.Parallel()
	boom := errors.New("catalog down")
	f := newFixture(t, fixtureOpts{readerOpts: []ReaderOption{WithKeyColumns(keyColumns{err: boom})}})
	f.start(t)
	f.push(t, readRecorded(t, "toast.hex")...)
	require.ErrorIs(t, f.finish(t), boom)
}

func TestReader_Run_KeyColumnsUnknownColumn(t *testing.T) {
	t.Parallel()
	keys := keyColumns{keys: map[string][]string{"rec.docs_full": {"nope"}}}
	f := newFixture(t, fixtureOpts{readerOpts: []ReaderOption{WithKeyColumns(keys)}})
	f.start(t)
	f.push(t, readRecorded(t, "toast.hex")...)
	require.Error(t, f.finish(t))
}

func TestReader_Run_ObserverSeesTransactionsInOrder(t *testing.T) {
	t.Parallel()
	obs := &fakeObserver{}
	var s synth
	s.begin(0x1000, 10)
	s.relation(1, "public", "users", 'd', "v")
	s.relation(2, "public", "other", 'n', "v")
	s.insert(1, "1", "a")
	s.insert(2, "9", "z")                                 // keyless table, skipped
	s.update(1, 'K', []string{"1", "\x00null"}, "2", "b") // key change 1 -> 2
	s.update(1, 0, nil, "2", "c")                         // key unchanged
	s.delete(1, "2", "\x00null")
	s.truncate(1, 2)
	s.message(true, 0x0FF0, "wake.watermark", `{"w":1}`)
	s.message(true, 0x0FF1, "wake.snapshot", `{"table":"public.users"}`)
	s.message(true, 0x0FF2, "wake.heartbeat", `{}`)
	s.message(true, 0x0FF3, "app.audit", `x`)
	s.commit(0x1000, 0x1010)
	recs, _ := runAll(t, fixtureOpts{
		cfg: ReaderConfig{EmitTruncate: true}, sinkOpts: []fakesink.Option{fakesink.WithAutoAck()},
		readerOpts: []ReaderOption{WithObserver(obs)},
	}, s.msgs)
	assert.Equal(t, []observed{
		{call: "begin", key: "0/1000/10"},
		{call: "change", table: "public.users", key: `{"id":1}`},
		{call: "change", table: "public.users", key: `{"id":2}`, before: `{"id":1}`},
		{call: "change", table: "public.users", key: `{"id":2}`},
		{call: "change", table: "public.users", key: `{"id":2}`},
		{call: "truncate", table: "public.users"},
		{call: "message", table: "wake.watermark", key: `{"w":1}`},
		{call: "message", table: "wake.snapshot", key: `{"table":"public.users"}`},
		{call: "commit"},
	}, obs.Calls())
	assert.Equal(t, []string{"0/1000-0", "0/1000-1", "0/1000-2", "0/1000-3", "0/1000-4"}, ids(recs))
}

func TestReader_Run_HeldTransactionReceivesRecordsBeforeLaterOnes(t *testing.T) {
	t.Parallel()
	obs := &fakeObserver{holdNext: true}
	var heldDone <-chan struct{}
	obs.held = func(ctx context.Context, tx pipeline.HeldTx) error {
		g, err := tx.NewGroup(time.Unix(0, 0))
		if err != nil {
			return err
		}
		ev := change.Event{
			ID: change.SnapshotID(0x1000, 1, 0), Source: change.Source{DB: "app", Schema: "public", Table: "users"},
			Op: change.OpRead, Key: change.Row{{Name: "id", Value: []byte("5")}},
		}
		if _, err := g.Route(ctx, pipeline.NewEventRecord(&ev)); err != nil {
			return err
		}
		if err := g.Seal(); err != nil {
			return err
		}
		heldDone = g.Done()
		return tx.Seal()
	}
	var s synth
	s.begin(0x1000, 10)
	s.relation(1, "public", "users", 'd', "v")
	s.message(true, 0x0FF0, "wake.watermark", `{}`)
	s.commit(0x1000, 0x1010)
	s.begin(0x2000, 11)
	s.insert(1, "5", "a")
	s.commit(0x2000, 0x2010)
	f := newFixture(t, fixtureOpts{readerOpts: []ReaderOption{WithObserver(obs)}})
	f.start(t)
	f.push(t, s.msgs...)
	require.ErrorIs(t, f.finish(t), fakerepl.ErrClosed)
	assert.Equal(t, []string{"0/1000-r1-0", "0/2000-0"}, ids(f.sink.Received()))
	require.NotNil(t, heldDone)
	select {
	case <-heldDone:
		t.Fatal("group done before acknowledgement")
	default:
	}
	_, err := f.sink.AckNext(1)
	require.NoError(t, err)
	<-heldDone
	assert.Equal(t, lsn.LSN(0x1010), f.tracker.Confirmed())
}

func TestReader_Run_ObserverErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("state")
	cases := []struct {
		name string
		obs  *fakeObserver
	}{
		{name: "commit", obs: &fakeObserver{commitErr: boom}},
		{name: "held", obs: &fakeObserver{
			holdNext: true, held: func(context.Context, pipeline.HeldTx) error { return boom },
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s synth
			s.begin(0x1000, 10)
			s.commit(0x1000, 0x1010)
			f := newFixture(t, fixtureOpts{readerOpts: []ReaderOption{WithObserver(tc.obs)}})
			f.start(t)
			f.push(t, s.msgs...)
			require.ErrorIs(t, f.finish(t), boom)
		})
	}
}

func TestReader_Run_OutOfOrderTransaction(t *testing.T) {
	t.Parallel()
	var s synth
	s.begin(0x2000, 10)
	s.commit(0x2000, 0x2010)
	s.begin(0x1000, 11)
	s.commit(0x1000, 0x1010)
	f := newFixture(t, fixtureOpts{})
	f.start(t)
	f.push(t, s.msgs...)
	require.ErrorIs(t, f.finish(t), ErrOutOfOrder)
}
