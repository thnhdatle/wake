// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestMain fails the package when a goroutine (keep-in-progress ticker,
// client) outlives Consume or Run.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// evt builds a CDC event payload; key, before and after are raw JSON
// (use "null" for none).
func evt(op, table, key, before, after string) []byte {
	schema, name, _ := splitTable(table)
	tx := `,"tx_id":7`
	id := "0/16B3748-0"
	if op == "r" {
		tx, id = "", "0/16B3748-r1-0"
	}
	return fmt.Appendf(nil, `{"id":%q,"source":{"db":"app","schema":%q,"table":%q},"op":%q,"ts_ms":1760600000000,`+
		`"lsn":"0/16B3748"%s,"key":%s,"before":%s,"after":%s,"schema_version":1}`,
		id, schema, name, op, tx, key, before, after)
}

// fakeDeleter records Delete calls and returns scripted errors first.
type fakeDeleter struct {
	mu    sync.Mutex
	errs  []error
	calls [][]string
	keys  []string // keys of successful calls
	// called is signaled (non-blocking) after every call.
	called chan struct{}
}

func newFakeDeleter(errs ...error) *fakeDeleter {
	return &fakeDeleter{errs: errs, called: make(chan struct{}, 100)}
}

func (d *fakeDeleter) Delete(_ context.Context, keys ...string) error {
	d.mu.Lock()
	defer func() {
		d.mu.Unlock()
		select {
		case d.called <- struct{}{}:
		default:
		}
	}()
	d.calls = append(d.calls, slices.Clone(keys))
	if len(d.errs) > 0 {
		err := d.errs[0]
		d.errs = d.errs[1:]
		return err
	}
	d.keys = append(d.keys, keys...)
	return nil
}

func (d *fakeDeleter) deleted() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.keys)
}

func (d *fakeDeleter) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

// counterValue returns the value of the counter name with labels in g.
func counterValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := g.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m, labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	n := 0
	for _, lp := range m.GetLabel() {
		v, ok := want[lp.GetName()]
		if !ok || v != lp.GetValue() {
			return false
		}
		n++
	}
	return n == len(want)
}

// eventually polls cond every millisecond until it holds, for at most 10 s.
func eventually(tb testing.TB, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatal("condition not reached")
		}
		<-tick.C
	}
}
