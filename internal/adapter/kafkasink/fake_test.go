// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
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

// waitTimeout bounds every wait on real time; the tests poll state, they
// never sleep.
const waitTimeout = 10 * time.Second

// produced is one record handed to the fake client.
type produced struct {
	rec     *kgo.Record
	promise func(*kgo.Record, error)
}

// fakeClient is an in-memory client. In auto mode it completes every record
// synchronously with the outcome of result (nil: success); otherwise the
// test completes records with complete.
type fakeClient struct {
	mu          sync.Mutex
	topics      map[string]topicInfo
	describeErr error
	created     []string
	createdArgs [][2]int
	records     []produced // every Produce call, in call order
	open        []produced // not yet completed, in call order
	auto        bool
	result      func(*kgo.Record) error
	flushed     bool
	closed      bool
	changed     chan struct{}
}

func newFakeClient(auto bool) *fakeClient {
	return &fakeClient{topics: map[string]topicInfo{}, auto: auto, changed: make(chan struct{})}
}

func (f *fakeClient) factory() clientFactory {
	return func([]kgo.Opt) (client, error) { return f, nil }
}

func (f *fakeClient) setTopic(name string, info topicInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics[name] = info
}

func (f *fakeClient) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	info := f.topics[r.Topic]
	r.Partition = int32(partitionFor(r, max(info.partitions, 1))) //nolint:gosec // G115: test partitions
	p := produced{rec: r, promise: promise}
	f.records = append(f.records, p)
	auto, result := f.auto, f.result
	if !auto {
		f.open = append(f.open, p)
	}
	f.notifyLocked()
	f.mu.Unlock()
	if auto {
		var err error
		if result != nil {
			err = result(r)
		}
		promise(r, err)
	}
}

func (f *fakeClient) Flush(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushed = true
	return nil
}

func (f *fakeClient) Close() {
	f.mu.Lock()
	open := f.open
	f.open = nil
	f.closed = true
	f.mu.Unlock()
	for _, p := range open {
		p.promise(p.rec, kgo.ErrClientClosed)
	}
}

func (f *fakeClient) describeTopic(_ context.Context, topic string) (topicInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.describeErr != nil {
		return topicInfo{}, f.describeErr
	}
	return f.topics[topic], nil
}

func (f *fakeClient) createTopic(_ context.Context, topic string, partitions int32, rf int16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, topic)
	f.createdArgs = append(f.createdArgs, [2]int{int(partitions), int(rf)})
	n := int(partitions)
	if n < 1 {
		n = 1
	}
	f.topics[topic] = topicInfo{exists: true, partitions: n}
	return nil
}

// complete finishes the n oldest open records with err.
func (f *fakeClient) complete(n int, err error) {
	f.mu.Lock()
	n = min(n, len(f.open))
	done := slices.Clone(f.open[:n])
	f.open = f.open[n:]
	f.mu.Unlock()
	for _, p := range done {
		p.promise(p.rec, err)
	}
}

func (f *fakeClient) snapshot() []produced {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.records)
}

func (f *fakeClient) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// waitRecords blocks until at least n records were produced.
func (f *fakeClient) waitRecords(t *testing.T, n int) []produced {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		f.mu.Lock()
		got, ch := len(f.records), f.changed
		f.mu.Unlock()
		if got >= n {
			return f.snapshot()
		}
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("waited for %d produced records, have %d", n, got)
		}
	}
}

// acker counts acknowledgements per token.
type acker struct {
	mu   sync.Mutex
	acks map[pipeline.AckToken]int
	err  error
}

func newAcker() *acker { return &acker{acks: map[pipeline.AckToken]int{}} }

func (a *acker) Ack(tok pipeline.AckToken) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acks[tok]++
	return a.err
}

func (a *acker) count(tx uint64) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.acks[pipeline.AckToken{Tx: tx}]
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

// recorder is an Observer and AckObserver that counts calls.
type recorder struct {
	mu       sync.Mutex
	sent     int
	acked    int
	retries  map[string]int
	latency  int
	inflight int
	tsMs     []int64
}

func newRecorder() *recorder { return &recorder{retries: map[string]int{}} }

// with runs fn under the recorder's lock.
func (r *recorder) with(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn()
}

func (r *recorder) ObserveSent(string, int, int)         { r.with(func() { r.sent++ }) }
func (r *recorder) ObserveAcked(_ string, n int)         { r.with(func() { r.acked += n }) }
func (r *recorder) ObserveRetry(_, reason string)        { r.with(func() { r.retries[reason]++ }) }
func (r *recorder) ObserveLatency(string, time.Duration) { r.with(func() { r.latency++ }) }
func (r *recorder) SetInflight(_ string, n int)          { r.with(func() { r.inflight = n }) }
func (r *recorder) ObserveAck(_ string, ts int64)        { r.with(func() { r.tsMs = append(r.tsMs, ts) }) }

func (r *recorder) retry(reason string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retries[reason]
}

const testTopicPrefix = "cdc"

func baseConfig() config.Sink {
	return config.Sink{
		Name: "kafka-main", Type: config.SinkKafka, Tables: []string{"*"},
		MaxEvents: 1000, MaxBytes: 1 << 20, Linger: config.Duration(time.Millisecond),
		Kafka: &config.KafkaSink{
			Brokers: []string{"127.0.0.1:9092"}, TopicPrefix: testTopicPrefix, Tombstones: true,
			Partitions: -1, ReplicationFactor: -1,
		},
	}
}

func getenv(string) string { return "" }

// fastBackoff keeps retry loops short in tests.
var fastBackoff = pipeline.Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond}

// harness runs a sink with a fake client until the test ends.
type harness struct {
	sink   *Sink
	client *fakeClient
	acks   *acker
	rec    *recorder
}

func startSink(t *testing.T, cfg config.Sink, fc *fakeClient, opts ...Option) *harness {
	t.Helper()
	h := &harness{client: fc, acks: newAcker(), rec: newRecorder()}
	all := append([]Option{
		withClientFactory(fc.factory()), WithBackoff(fastBackoff), WithObserver(h.rec), WithAckObserver(h.rec),
		WithRand(rand.New(rand.NewPCG(1, 2))), //nolint:gosec // G404: deterministic test jitter
	}, opts...)
	s, err := New(cfg, getenv, clock.System{}, testutil.DiscardLogger(), all...)
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

func withClientFactory(f clientFactory) Option { return func(o *options) { o.newClient = f } }

// send hands a record to the sink with an ack handle for tx.
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

// event builds a row event record of public.users.
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
	}
	if op != change.OpDelete && op != change.OpTruncate {
		ev.After = change.Row{{Name: "id", Value: []byte(key)}, {Name: "email", Value: []byte(`"a@example.com"`)}}
	}
	return pipeline.NewEventRecord(ev)
}

const usersTopic = testTopicPrefix + ".public.users"

var errInjected = errors.New("injected")
