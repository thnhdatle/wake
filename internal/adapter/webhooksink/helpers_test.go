// SPDX-License-Identifier: Apache-2.0

package webhooksink_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
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

const (
	testSecret  = "whsec_test"
	secretEnv   = "WAKE_TEST_WEBHOOK_SECRET"
	waitTimeout = 20 * time.Second
)

var epoch = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

type tb interface {
	require.TestingT
	Cleanup(f func())
}

func getenv(k string) string {
	if k == secretEnv {
		return testSecret
	}
	return ""
}

func baseConfig(url string) config.Sink {
	return config.Sink{
		Name: "hook", Type: config.SinkWebhook,
		MaxEvents: 100, MaxBytes: 1 << 20, MaxEventBytes: 1 << 20,
		Webhook: &config.WebhookSink{
			URL: url, SecretEnv: secretEnv, Timeout: config.Duration(5 * time.Second),
			Lanes: 4, MaxAttempts: 3,
		},
	}
}

type action struct {
	status     int
	reset      bool
	hang       bool
	wait       <-chan struct{}
	delay      time.Duration
	retryAfter string
}

type received struct {
	n           int
	id          string
	sig         string
	contentType string
	body        []byte
	sigErr      error
}

func (r *received) items(t require.TestingT) []item {
	var out []item
	require.NoError(t, json.Unmarshal(r.body, &out))
	return out
}

type item struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	K    string `json:"k"`
	N    uint64 `json:"n"`
}

type receiver struct {
	srv    *httptest.Server
	clk    clock.Clock
	decide func(n int, r *received) action

	mu       sync.Mutex
	all      []*received
	accepted []*received

	reqs chan *received
}

func newReceiver(t tb, clk clock.Clock, decide func(n int, r *received) action) *receiver {
	rv := &receiver{clk: clk, decide: decide, reqs: make(chan *received, 1<<16)}
	if rv.decide == nil {
		rv.decide = func(int, *received) action { return action{} }
	}
	rv.srv = httptest.NewServer(rv)
	t.Cleanup(rv.srv.Close)
	return rv
}

func (rv *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	rec := &received{
		id: r.Header.Get(webhooksink.DeliveryIDHeader), sig: r.Header.Get(webhooksink.SignatureHeader),
		contentType: r.Header.Get("Content-Type"), body: body,
	}
	rec.sigErr = webhooksink.Verify([]byte(testSecret), rec.sig, body, rv.clk.Now(), webhooksink.DefaultTolerance)
	rv.mu.Lock()
	rec.n = len(rv.all)
	rv.all = append(rv.all, rec)
	act := rv.decide(rec.n, rec)
	rv.mu.Unlock()
	rv.reqs <- rec
	if !rv.await(r.Context(), act) {
		return
	}
	if act.reset {
		resetConn(w)
		return
	}
	status := act.status
	if status == 0 {
		status = http.StatusOK
	}
	if status/100 == 2 {
		rv.mu.Lock()
		rv.accepted = append(rv.accepted, rec)
		rv.mu.Unlock()
	}
	if act.retryAfter != "" {
		w.Header().Set("Retry-After", act.retryAfter)
	}
	w.WriteHeader(status)
}

func (rv *receiver) await(ctx context.Context, act action) bool {
	if act.hang {
		<-ctx.Done()
		return false
	}
	wait := act.wait
	var timer <-chan time.Time
	if act.delay > 0 {
		timer = time.After(act.delay)
	}
	if wait == nil && timer == nil {
		return true
	}
	select {
	case <-wait:
	case <-timer:
	case <-ctx.Done():
		return false
	}
	return true
}

func resetConn(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

func (rv *receiver) url() string { return rv.srv.URL }

func (rv *receiver) next(t require.TestingT) *received {
	select {
	case r := <-rv.reqs:
		return r
	case <-time.After(waitTimeout):
		require.FailNow(t, "no request arrived")
		return nil
	}
}

func (rv *receiver) noRequest(t require.TestingT) {
	select {
	case r := <-rv.reqs:
		require.FailNow(t, "unexpected request", "request %d: %s", r.n, r.body)
	default:
	}
}

func (rv *receiver) acceptedRequests() []*received {
	rv.mu.Lock()
	defer rv.mu.Unlock()
	return append([]*received(nil), rv.accepted...)
}

func (rv *receiver) allRequests() []*received {
	rv.mu.Lock()
	defer rv.mu.Unlock()
	return append([]*received(nil), rv.all...)
}

type acker struct {
	mu      sync.Mutex
	acked   map[uint64]int
	order   []uint64
	changed chan struct{}
}

func newAcker() *acker {
	return &acker{acked: map[uint64]int{}, changed: make(chan struct{})}
}

func (a *acker) Ack(tok pipeline.AckToken) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acked[tok.Tx]++
	a.order = append(a.order, tok.Tx)
	close(a.changed)
	a.changed = make(chan struct{})
	if a.acked[tok.Tx] > 1 {
		return fmt.Errorf("token %d acknowledged twice", tok.Tx)
	}
	return nil
}

func (a *acker) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.order)
}

func (a *acker) duplicates() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	d := 0
	for _, n := range a.acked {
		d += n - 1
	}
	return d
}

func (a *acker) waitAcks(t require.TestingT, n int) {
	deadline := time.After(waitTimeout)
	for {
		a.mu.Lock()
		got, ch := len(a.order), a.changed
		a.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			require.FailNow(t, "acknowledgements missing", "got %d of %d", got, n)
		}
	}
}

func keyedRec(a pipeline.Acker, seq uint64, key string) pipeline.Record {
	id := change.StreamID(lsn.LSN(0x16B3748), seq)
	return pipeline.Record{
		Kind: pipeline.RecordKindEvent, ID: id, Op: change.OpUpdate,
		Source: change.Source{DB: "app", Schema: "public", Table: "users"}, Destination: "public.users",
		Key:     []byte(key),
		Payload: fmt.Appendf(nil, `{"id":%q,"k":%q,"n":%d}`, id.String(), key, seq),
		TsMs:    epoch.UnixMilli(),
		Ack:     pipeline.NewAckHandle(a, pipeline.AckToken{Tx: seq}),
	}
}

func barrierRec(a pipeline.Acker, seq uint64) pipeline.Record {
	id := change.SchemaID(lsn.LSN(0x16B3748), seq)
	return pipeline.Record{
		Kind: pipeline.RecordKindSchemaChange, ID: id, Barrier: true, Destination: pipeline.DestinationSchema,
		Payload: fmt.Appendf(nil, `{"type":"schema_change","id":%q,"n":%d}`, id.String(), seq),
		TsMs:    epoch.UnixMilli(),
		Ack:     pipeline.NewAckHandle(a, pipeline.AckToken{Tx: seq}),
	}
}

func newSink(t tb, cfg config.Sink, clk clock.Clock, opts ...webhooksink.Option) *webhooksink.Sink {
	opts = append([]webhooksink.Option{webhooksink.WithRand(seeded(1))}, opts...)
	s, err := webhooksink.New(cfg, getenv, clk, testutil.DiscardLogger(), opts...)
	require.NoError(t, err)
	return s
}

func start(t tb, s *webhooksink.Sink) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})
}

func autoAdvance(t tb, clk *clock.Fake, step time.Duration) {
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			ch := clk.WaiterChange()
			if clk.Waiters() > 0 {
				clk.Advance(step)
				continue
			}
			select {
			case <-stop:
				return
			case <-ch:
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
}

func waitTimer(t require.TestingT, clk *clock.Fake) {
	deadline := time.After(waitTimeout)
	for {
		ch := clk.WaiterChange()
		if clk.Waiters() == 1 {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			require.FailNow(t, "no timer", "have %d timers", clk.Waiters())
		}
	}
}

func waitHealth(t require.TestingT, s *webhooksink.Sink, state pipeline.SinkState) pipeline.SinkHealth {
	deadline := time.After(waitTimeout)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if h := s.Health(); h.State == state {
			return h
		}
		select {
		case <-tick.C:
		case <-deadline:
			require.FailNow(t, "health not reached", "want %s, have %+v", state, s.Health())
		}
	}
}

func send(t require.TestingT, s *webhooksink.Sink, recs ...pipeline.Record) {
	for _, r := range recs {
		require.NoError(t, s.Send(context.Background(), r))
	}
}

func seeded(n uint64) *rand.Rand { return rand.New(rand.NewPCG(n, n)) }

type recorder struct {
	mu           sync.Mutex
	sent         int
	acked        int
	retries      map[string]int
	latencies    int
	inflight     []int
	deadLettered int
	ackTs        []int64
}

func newRecorder() *recorder { return &recorder{retries: map[string]int{}} }

func (r *recorder) ObserveSent(_ string, events, _ int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent += events
}

func (r *recorder) ObserveAcked(_ string, events int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acked += events
}

func (r *recorder) ObserveRetry(_, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retries[reason]++
}

func (r *recorder) ObserveLatency(string, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.latencies++
}

func (r *recorder) SetInflight(_ string, events int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inflight = append(r.inflight, events)
}

func (r *recorder) ObserveDeadLettered(_ string, events int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deadLettered += events
}

func (r *recorder) ObserveAck(_ string, tsMs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ackTs = append(r.ackTs, tsMs)
}

func (r *recorder) retriesFor(reason string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retries[reason]
}
