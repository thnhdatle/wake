// SPDX-License-Identifier: Apache-2.0

package l2fake_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/platform/clock"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2/l2test"
	"github.com/thnhdatle/wake/pkg/cache/internal/l2fake"
)

// TestClient_Conformance runs the shared l2 contract suite against the fake,
// the same suite pkg/cache/redisl2 runs against real Redis.
func TestClient_Conformance(t *testing.T) {
	t.Parallel()
	l2test.Run(t, func(t *testing.T) l2test.Harness {
		t.Helper()
		clk := clock.NewFake(start)
		return &fakeHarness{clk: clk, srv: l2fake.NewServer(clk)}
	})
}

// fakeHarness is one fake server with its own fake clock.
type fakeHarness struct {
	clk *clock.Fake
	srv *l2fake.Server
}

func (h *fakeHarness) Backend(t *testing.T, cfg l2.Config) l2.Backend {
	t.Helper()
	cli := h.srv.Client(cfg)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	return cli
}

func (h *fakeHarness) Now(t *testing.T) time.Time {
	t.Helper()
	return h.clk.Now()
}

func (h *fakeHarness) Elapse(t *testing.T, d time.Duration) {
	t.Helper()
	h.clk.Advance(d)
}

func (*fakeHarness) Tolerance() time.Duration { return 0 }

func (h *fakeHarness) Gen(t *testing.T, ns string, epoch uint64, key string) (uint64, bool) {
	t.Helper()
	return h.srv.Gen(ns, epoch, key)
}

func (h *fakeHarness) GenTTL(t *testing.T, ns string, epoch uint64, key string) (time.Duration, bool) {
	t.Helper()
	return h.srv.GenTTL(ns, epoch, key)
}

func (h *fakeHarness) Value(t *testing.T, ns string, epoch uint64, key string) (l2.StoredValue, bool) {
	t.Helper()
	return h.srv.Value(ns, epoch, key)
}

func (h *fakeHarness) Locked(t *testing.T, ns string, epoch uint64, key string) bool {
	t.Helper()
	return h.srv.Locked(ns, epoch, key)
}

func (h *fakeHarness) EpochOf(t *testing.T, ns string) (uint64, bool) {
	t.Helper()
	return h.srv.EpochOf(ns)
}

func (h *fakeHarness) RawSet(t *testing.T, key, val string) {
	t.Helper()
	h.srv.RawSet(key, val)
}

func (h *fakeHarness) Delete(t *testing.T, key string) {
	t.Helper()
	h.srv.Delete(key)
}

func (*fakeHarness) DropBus(t *testing.T, b l2.Backend) {
	t.Helper()
	cli, ok := b.(*l2fake.Client)
	require.True(t, ok, "backend %T", b)
	cli.SetBusDown(true)
	cli.SetBusDown(false)
}
