// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/internal/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const waitTimeout = 10 * time.Second

const (
	testStream = "wake_app"
	testPrefix = "cdc"
)

var errInjected = errors.New("injected")

type published struct {
	msg *nats.Msg
	id  string
	fut *fakeFuture
}

type fakeFuture struct {
	ok  chan *jetstream.PubAck
	err chan error
}

func newFuture() *fakeFuture {
	return &fakeFuture{ok: make(chan *jetstream.PubAck, 1), err: make(chan error, 1)}
}

func (f *fakeFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *fakeFuture) Err() <-chan error            { return f.err }

func (f *fakeFuture) resolve(err error) {
	if err != nil {
		f.err <- err
		return
	}
	f.ok <- &jetstream.PubAck{Stream: testStream}
}

type fakeConn struct {
	mu        sync.Mutex
	ep        uint64
	payload   int64
	connected bool
	lastErr   error
	streams   map[string]bool
	ensureErr error
	ensured   []streamSpec
	all       []published
	open      []published
	auto      bool
	pubErr    error
	closed    bool
	changed   chan struct{}
}

func newFakeConn(auto bool) *fakeConn {
	return &fakeConn{
		payload: 1 << 20, connected: true, streams: map[string]bool{}, auto: auto, changed: make(chan struct{}),
	}
}

func (f *fakeConn) dialer() dialer { return func(*Sink) (conn, error) { return f, nil } }

func (f *fakeConn) publish(msg *nats.Msg, id string) (ackFuture, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pubErr != nil {
		return nil, f.pubErr
	}
	p := published{msg: msg, id: id, fut: newFuture()}
	f.all = append(f.all, p)
	if f.auto {
		p.fut.resolve(nil)
	} else {
		f.open = append(f.open, p)
	}
	close(f.changed)
	f.changed = make(chan struct{})
	return p.fut, nil
}

func (f *fakeConn) epoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ep
}

func (f *fakeConn) maxPayload() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.payload
}

func (f *fakeConn) state() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected, f.lastErr
}

func (f *fakeConn) ensureStream(_ context.Context, spec streamSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensured = append(f.ensured, spec)
	if f.ensureErr != nil {
		return f.ensureErr
	}
	f.streams[spec.name] = true
	return nil
}

func (f *fakeConn) streamExists(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.streams[name] {
		return ErrUnknownStream
	}
	return nil
}

func (f *fakeConn) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeConn) set(fn func(f *fakeConn)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeConn) complete(n int, err error) {
	f.mu.Lock()
	n = min(n, len(f.open))
	done := slices.Clone(f.open[:n])
	f.open = f.open[n:]
	f.mu.Unlock()
	for _, p := range done {
		p.fut.resolve(err)
	}
}

func (f *fakeConn) snapshot() []published {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.all)
}

func (f *fakeConn) waitPublished(t *testing.T, n int) []published {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		f.mu.Lock()
		got, ch := len(f.all), f.changed
		f.mu.Unlock()
		if got >= n {
			return f.snapshot()
		}
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("waited for %d publishes, have %d", n, got)
		}
	}
}

func ids(ps []published) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.id)
	}
	return out
}

type acker struct {
	mu   sync.Mutex
	acks map[pipeline.AckToken]int
	err  error
}

func (a *acker) Ack(tok pipeline.AckToken) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks[tok]++
	return a.err
}

func (a *acker) total() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, c := range a.acks {
		n += c
	}
	return n
}

type recorder struct {
	mu       sync.Mutex
	sent     int
	acked    int
	retries  map[string]int
	inflight int
	tsMs     []int64
}

func (r *recorder) with(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn()
}

func (r *recorder) ObserveSent(string, int, int)         { r.with(func() { r.sent++ }) }
func (r *recorder) ObserveAcked(_ string, n int)         { r.with(func() { r.acked += n }) }
func (r *recorder) ObserveRetry(_, reason string)        { r.with(func() { r.retries[reason]++ }) }
func (r *recorder) ObserveLatency(string, time.Duration) {}
func (r *recorder) SetInflight(_ string, n int)          { r.with(func() { r.inflight = n }) }
func (r *recorder) ObserveAck(_ string, ts int64)        { r.with(func() { r.tsMs = append(r.tsMs, ts) }) }

func (r *recorder) retry(reason string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retries[reason]
}

func baseConfig() config.Sink {
	return config.Sink{
		Name: "nats-main", Type: config.SinkNATS, Tables: []string{"*"},
		MaxEvents: 1000, MaxBytes: 1 << 20, Linger: config.Duration(time.Millisecond),
		NATS: &config.NATSSink{
			URL: "nats://127.0.0.1:4222", SubjectPrefix: testPrefix, CreateStream: true,
			Stream: testStream, DuplicateWindow: config.Duration(2 * time.Minute),
		},
	}
}

var fastBackoff = pipeline.Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond}

type harness struct {
	sink *Sink
	conn *fakeConn
	acks *acker
	rec  *recorder
}

func withDialer(d dialer) Option { return func(o *options) { o.dial = d } }

func startSink(t *testing.T, cfg config.Sink, fc *fakeConn, opts ...Option) *harness {
	t.Helper()
	h := &harness{conn: fc, acks: &acker{acks: map[pipeline.AckToken]int{}}, rec: &recorder{retries: map[string]int{}}}
	all := append([]Option{
		withDialer(fc.dialer()), WithBackoff(fastBackoff), WithObserver(h.rec), WithAckObserver(h.rec),
		WithStallTimeout(time.Hour), WithShutdownTimeout(50 * time.Millisecond),
		WithRand(rand.New(rand.NewPCG(1, 2))), //nolint:gosec // G404: test jitter
	}, opts...)
	s, err := New(cfg, clock.System{}, testutil.DiscardLogger(), all...)
	require.NoError(t, err)
	h.sink = s
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(waitTimeout):
			t.Errorf("Run did not return")
		}
	})
	return h
}

func (h *harness) send(t *testing.T, rec pipeline.Record, tx uint64) {
	t.Helper()
	rec.Ack = pipeline.NewAckHandle(h.acks, pipeline.AckToken{Tx: tx})
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	require.NoError(t, h.sink.Send(ctx, rec))
}

func (h *harness) waitHealth(t *testing.T, state pipeline.SinkState) pipeline.SinkHealth {
	t.Helper()
	require.Eventually(t, func() bool { return h.sink.Health().State == state }, waitTimeout, time.Millisecond,
		"health %v, want %v", h.sink.Health(), state)
	return h.sink.Health()
}

func (h *harness) waitAcks(t *testing.T, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return h.acks.total() >= n }, waitTimeout, time.Millisecond,
		"acks %d, want %d", h.acks.total(), n)
}

func event(op change.Op, k uint64, key string) pipeline.Record {
	ev := &change.Event{
		ID:            change.StreamID(lsn.LSN(0x16B3748), k),
		Source:        change.Source{DB: "app", Schema: "public", Table: "users"},
		Op:            op,
		TsMs:          1_760_600_000_000 + int64(k),
		SchemaVersion: 3,
	}
	if key != "" {
		ev.Key = change.Row{{Name: "id", Value: []byte(key)}}
		ev.After = change.Row{{Name: "id", Value: []byte(key)}}
	}
	return pipeline.NewEventRecord(ev)
}
