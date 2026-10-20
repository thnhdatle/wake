// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
	"github.com/thnhdatle/wake/internal/testutil/fakerepl"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
	"github.com/thnhdatle/wake/internal/testutil/fakespill"
)

// recordedDir holds real Postgres 17 traffic recorded by
// scripts/record_pgoutput.sh: one "<lsn> <xid> <hex>" line per message.
const recordedDir = "../../../testdata/pgoutput"

// walMsg is one recorded or synthesized XLogData message.
type walMsg struct {
	at   lsn.LSN
	data []byte
}

func readRecorded(tb testing.TB, name string) []walMsg {
	tb.Helper()
	f, err := os.Open(recordedDir + "/" + name)
	require.NoError(tb, err)
	defer f.Close()
	var msgs []walMsg
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		require.Len(tb, fields, 3, "line %q", sc.Text())
		at, err := lsn.Parse(fields[0])
		require.NoError(tb, err)
		b, err := hex.DecodeString(fields[2])
		require.NoError(tb, err)
		msgs = append(msgs, walMsg{at: at, data: b})
	}
	require.NoError(tb, sc.Err())
	return msgs
}

// serverTime is the XLogData server time of pushed messages.
var serverTime = time.UnixMilli(1_792_308_700_000).UTC()

func xlog(m walMsg) pipeline.ReplicationMessage {
	return pipeline.ReplicationMessage{Kind: pipeline.ReplicationMessageXLogData, XLogData: pipeline.XLogData{
		WALStart: m.at, ServerWALEnd: m.at, ServerTime: serverTime, Data: m.data,
	}}
}

// fixture is a reader wired to fakes.
type fixture struct {
	src     *fakerepl.Source
	tracker *AckTracker
	router  *Router
	sink    *fakesink.Sink
	status  *StatusUpdater
	schemas *Schemas
	spill   *fakespill.Store
	clock   *clock.Fake
	reader  *Reader
	done    chan error
}

type fixtureOpts struct {
	cfg         ReaderConfig
	policy      pipeline.TablePolicyConfig
	sinkOpts    []fakesink.Option
	readerOpts  []ReaderOption
	schemaState schema.RegistryState
	// route replaces the fake sink as the only route (benchmarks); f.sink
	// is then unused.
	route pipeline.Sink
}

func newFixture(tb testing.TB, o fixtureOpts) *fixture {
	tb.Helper()
	f := &fixture{src: fakerepl.New(4096), spill: fakespill.New(), clock: clock.NewFake(time.Unix(1_800_000_000, 0))}
	var err error
	f.tracker, err = NewAckTracker(o.cfg.Start)
	require.NoError(tb, err)
	f.sink = fakesink.New("main", o.sinkOpts...)
	var route pipeline.Sink = f.sink
	if o.route != nil {
		route = o.route
	}
	f.router, err = NewRouter(f.tracker, []SinkRoute{{Sink: route, Tables: []string{"*"}}})
	require.NoError(tb, err)
	f.status, err = NewStatusUpdater(f.src, f.tracker, f.clock)
	require.NoError(tb, err)
	f.schemas, err = NewSchemas(o.schemaState)
	require.NoError(tb, err)
	if o.policy.DB == "" {
		o.policy.DB = "app"
	}
	policy, err := pipeline.NewTablePolicy(o.policy)
	require.NoError(tb, err)
	if o.cfg.Slot == "" {
		o.cfg.Slot = "wake_app"
		o.cfg.Publications = []string{"wake_app"}
	}
	deps := ReaderDeps{
		Source: f.src, Router: f.router, Status: f.status, Schemas: f.schemas, Tables: policy, Spill: f.spill,
		Clock: f.clock,
	}
	opts := append([]ReaderOption{WithReaderLogger(testutil.DiscardLogger())}, o.readerOpts...)
	f.reader, err = NewReader(deps, o.cfg, opts...)
	require.NoError(tb, err)
	return f
}

// start runs the reader until the test ends.
func (f *fixture) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.done = make(chan error, 1)
	go func() { f.done <- f.reader.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-f.done
	})
}

// push scripts messages.
func (f *fixture) push(t *testing.T, msgs ...walMsg) {
	t.Helper()
	for _, m := range msgs {
		require.NoError(t, f.src.Push(context.Background(), xlog(m)))
	}
}

// finish closes the script and returns Run's error, which wraps
// fakerepl.ErrClosed once every message was processed.
func (f *fixture) finish(t *testing.T) error {
	t.Helper()
	f.src.Close()
	select {
	case err := <-f.done:
		f.done <- err
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("reader did not stop")
		return nil
	}
}

// runAll processes msgs to completion and returns the sink's records.
func runAll(t *testing.T, o fixtureOpts, msgs []walMsg) ([]pipeline.Record, *fixture) {
	t.Helper()
	f := newFixture(t, o)
	f.start(t)
	f.push(t, msgs...)
	require.ErrorIs(t, f.finish(t), fakerepl.ErrClosed)
	return f.sink.Received(), f
}

// render formats records as golden lines.
func render(recs []pipeline.Record) string {
	var b strings.Builder
	for i := range recs {
		key, _ := json.Marshal(string(recs[i].Key))
		b.WriteString(`{"dest":` + strconv.Quote(recs[i].Destination) + `,"key":` + string(key) +
			`,"barrier":` + strconv.FormatBool(recs[i].Barrier) + `,"payload":` + string(recs[i].Payload) + "}\n")
	}
	return b.String()
}

func ids(recs []pipeline.Record) []string {
	out := make([]string, len(recs))
	for i := range recs {
		out[i] = recs[i].ID.String()
	}
	return out
}

// wire is a minimal pgoutput encoder for synthesized traffic.
type wire []byte

func (w wire) u8(v byte) wire     { return append(w, v) }
func (w wire) u16(v uint16) wire  { return binary.BigEndian.AppendUint16(w, v) }
func (w wire) u32(v uint32) wire  { return binary.BigEndian.AppendUint32(w, v) }
func (w wire) u64(v uint64) wire  { return binary.BigEndian.AppendUint64(w, v) }
func (w wire) cstr(s string) wire { return append(append(w, s...), 0) }

func (w wire) text(vals ...string) wire {
	w = w.u16(uint16(len(vals))) //nolint:gosec // G115: small test tuples
	for _, v := range vals {
		if v == "\x00null" {
			w = w.u8('n')
			continue
		}
		w = append(w.u8('t').u32(uint32(len(v))), v...) //nolint:gosec // G115: small test values
	}
	return w
}

// synth builds pgoutput messages with increasing WAL positions.
type synth struct {
	at   lsn.LSN
	msgs []walMsg
}

func (s *synth) add(w wire) {
	s.at += 0x10
	s.msgs = append(s.msgs, walMsg{at: s.at, data: w})
}

func (s *synth) begin(final lsn.LSN, xid uint32) {
	s.add(wire{'B'}.u64(uint64(final)).u64(0).u32(xid))
}

func (s *synth) commit(final, end lsn.LSN) {
	s.add(wire{'C'}.u8(0).u64(uint64(final)).u64(uint64(end)).u64(0))
}

// relation adds a table with an int4 key column "id" (unless keyless) and
// text columns.
func (s *synth) relation(oid uint32, ns, name string, identity byte, cols ...string) {
	w := wire{'R'}.u32(oid).cstr(ns).cstr(name).u8(identity).u16(uint16(len(cols) + 1)) //nolint:gosec // small
	flag := byte(1)
	if identity == 'n' {
		flag = 0
	}
	w = w.u8(flag).cstr("id").u32(schema.OIDInt4).u32(0xFFFFFFFF)
	for _, c := range cols {
		f := byte(0)
		if identity == 'f' {
			f = 1
		}
		w = w.u8(f).cstr(c).u32(schema.OIDText).u32(0xFFFFFFFF)
	}
	s.add(w)
}

func (s *synth) insert(oid uint32, vals ...string) { s.add(wire{'I'}.u32(oid).u8('N').text(vals...)) }

func (s *synth) update(oid uint32, oldKind byte, old []string, vals ...string) {
	w := wire{'U'}.u32(oid)
	if oldKind != 0 {
		w = w.u8(oldKind).text(old...)
	}
	s.add(w.u8('N').text(vals...))
}

func (s *synth) delete(oid uint32, vals ...string) { s.add(wire{'D'}.u32(oid).u8('K').text(vals...)) }

func (s *synth) truncate(oids ...uint32) {
	w := wire{'T'}.u32(uint32(len(oids))).u8(0) //nolint:gosec // small
	for _, o := range oids {
		w = w.u32(o)
	}
	s.add(w)
}

func (s *synth) message(transactional bool, at lsn.LSN, prefix, content string) {
	flags := byte(0)
	if transactional {
		flags = 1
	}
	w := wire{'M'}.u8(flags).u64(uint64(at)).cstr(prefix).u32(uint32(len(content))) //nolint:gosec // small
	s.add(append(w, content...))
}

// observed is one TxObserver call.
type observed struct {
	call   string
	table  string
	key    string
	before string
}

// fakeObserver records calls and can hold transactions open.
type fakeObserver struct {
	mu        sync.Mutex
	calls     []observed
	holdNext  bool
	commitErr error
	held      func(ctx context.Context, tx pipeline.HeldTx) error
}

func (o *fakeObserver) record(c observed) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, c)
}

func (o *fakeObserver) ObserveBegin(tx pipeline.ObservedTx) {
	o.record(observed{call: "begin", key: tx.CommitLSN.String() + "/" + strconv.FormatUint(uint64(tx.XID), 10)})
}

func (o *fakeObserver) ObserveChange(table string, key, before []byte) {
	o.record(observed{call: "change", table: table, key: string(key), before: string(before)})
}

func (o *fakeObserver) ObserveTruncate(table string) {
	o.record(observed{call: "truncate", table: table})
}

func (o *fakeObserver) ObserveMessage(prefix string, content []byte) {
	o.record(observed{call: "message", table: prefix, key: string(content)})
}

func (o *fakeObserver) ObserveCommit(context.Context) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, observed{call: "commit"})
	hold := o.holdNext
	o.holdNext = false
	return hold, o.commitErr
}

func (o *fakeObserver) ObserveHeld(ctx context.Context, tx pipeline.HeldTx) error {
	o.record(observed{call: "held"})
	if o.held != nil {
		return o.held(ctx, tx)
	}
	return tx.Seal()
}

func (o *fakeObserver) Calls() []observed {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.calls)
}

// fakeMetrics records reader metrics.
type fakeMetrics struct {
	mu        sync.Mutex
	types     []string
	messages  []byte
	truncates []string
	skipped   []string
	spilled   int
}

func (m *fakeMetrics) UnknownType(n string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.types = append(m.types, n)
}

func (m *fakeMetrics) UnknownMessage(t byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, t)
}

func (m *fakeMetrics) TruncateSkipped(table string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.truncates = append(m.truncates, table)
}

func (m *fakeMetrics) TableSkipped(table string, reason pipeline.SkipReason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skipped = append(m.skipped, table+":"+string(reason))
}

func (m *fakeMetrics) Spilled(int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spilled++
}

// fakeGate is a Gate released by the test.
type fakeGate struct {
	ch   chan struct{}
	once sync.Once
}

func newFakeGate() *fakeGate                  { return &fakeGate{ch: make(chan struct{})} }
func (g *fakeGate) Released() <-chan struct{} { return g.ch }
func (g *fakeGate) release()                  { g.once.Do(func() { close(g.ch) }) }

// fakeSchemaStore records saved registry states.
type fakeSchemaStore struct {
	mu     sync.Mutex
	states []schema.RegistryState
	err    error
}

func (s *fakeSchemaStore) SaveSchemas(_ context.Context, st schema.RegistryState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.states = append(s.states, st)
	return nil
}

func (s *fakeSchemaStore) Saved() []schema.RegistryState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.states)
}

// fakeActivity counts Touch calls.
type fakeActivity struct {
	mu sync.Mutex
	n  int
}

func (a *fakeActivity) Touch(time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
}

func (a *fakeActivity) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

// keyColumns maps "<schema>.<table>" to primary keys.
type keyColumns struct {
	keys map[string][]string
	err  error
}

func (k keyColumns) KeyColumns(_ context.Context, schemaName, table string) ([]string, error) {
	return k.keys[schemaName+"."+table], k.err
}

var _ pipeline.TxObserver = (*fakeObserver)(nil)

func mustLSN(tb testing.TB, s string) lsn.LSN {
	tb.Helper()
	l, err := lsn.Parse(s)
	require.NoError(tb, err)
	return l
}
