// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

var epoch = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const waitTimeout = 10 * time.Second

type recObserver struct {
	mu         sync.Mutex
	lagBytes   []int64
	walStatus  []string
	ages       []time.Duration
	pollErrors []error
	active     map[string]bool
	changes    []string
}

func newRecObserver() *recObserver { return &recObserver{active: map[string]bool{}} }

func (o *recObserver) ObserveSlot(lagBytes int64, walStatus string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lagBytes = append(o.lagBytes, lagBytes)
	o.walStatus = append(o.walStatus, walStatus)
}

func (o *recObserver) ObserveOldestUnackedAge(age time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ages = append(o.ages, age)
}

func (o *recObserver) ObservePollError(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pollErrors = append(o.pollErrors, err)
}

func (o *recObserver) SetAlertActive(rule string, active bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.active[rule] = active
	o.changes = append(o.changes, rule)
}

func (o *recObserver) isActive(rule string) (active, set bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	active, set = o.active[rule]
	return active, set
}

func (o *recObserver) errors() []error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]error(nil), o.pollErrors...)
}

type fakeAcks struct {
	mu     sync.Mutex
	readAt time.Time
	ok     bool
}

func (a *fakeAcks) OldestUnacked() (time.Time, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.readAt, a.ok
}

func (a *fakeAcks) set(readAt time.Time, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.readAt, a.ok = readAt, ok
}

type attempt struct {
	alert pipeline.Alert
	at    time.Time
	err   error
}

type chanNotifier struct {
	clk      clock.Clock
	attempts chan attempt
	mu       sync.Mutex
	err      error
}

func newChanNotifier(clk clock.Clock) *chanNotifier {
	return &chanNotifier{clk: clk, attempts: make(chan attempt, 64)}
}

func (n *chanNotifier) Notify(_ context.Context, a pipeline.Alert) error {
	n.mu.Lock()
	err := n.err
	n.mu.Unlock()
	n.attempts <- attempt{alert: a, at: n.clk.Now(), err: err}
	return err
}

func (n *chanNotifier) fail(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.err = err
}

func (n *chanNotifier) next(t *testing.T) attempt {
	t.Helper()
	select {
	case a := <-n.attempts:
		return a
	case <-time.After(waitTimeout):
		require.FailNow(t, "no Notify call")
		return attempt{}
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func jsonLogger(buf *syncBuffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func waitWaiter(t *testing.T, clk *clock.Fake) {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		ch := clk.WaiterChange()
		if clk.Waiters() > 0 {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			require.FailNow(t, "clock waiters not registered")
		}
	}
}

type runner struct {
	cancel context.CancelFunc
	errc   chan error
}

func start(run func(context.Context) error) *runner {
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{cancel: cancel, errc: make(chan error, 1)}
	go func() { r.errc <- run(ctx) }()
	return r
}

func (r *runner) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.errc:
		require.NoError(t, err)
	case <-time.After(waitTimeout):
		require.FailNow(t, "Run did not return after cancel")
	}
}
