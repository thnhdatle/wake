// SPDX-License-Identifier: Apache-2.0

package l2test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

// Harness adapts one L2 deployment to the suite. Every method fails t on an
// infrastructure error.
type Harness interface {
	// Backend returns a new backend for cfg (one cache instance with its own
	// connections); the harness closes it when t ends.
	Backend(t *testing.T, cfg l2.Config) l2.Backend
	// Now returns the server clock (Redis TIME).
	Now(t *testing.T) time.Time
	// Elapse returns once at least d has passed on the server clock.
	Elapse(t *testing.T, d time.Duration)
	// Tolerance bounds the server time that may pass between two calls of
	// one test (0 for a fake clock); TTL checks allow it.
	Tolerance() time.Duration
	// Gen returns the stored generation of key.
	Gen(t *testing.T, ns string, epoch uint64, key string) (uint64, bool)
	// GenTTL returns the remaining TTL of the generation key.
	GenTTL(t *testing.T, ns string, epoch uint64, key string) (time.Duration, bool)
	// Value returns the decoded value record of key.
	Value(t *testing.T, ns string, epoch uint64, key string) (l2.StoredValue, bool)
	// Locked reports whether the load lock of key exists.
	Locked(t *testing.T, ns string, epoch uint64, key string) bool
	// EpochOf returns the stored namespace epoch.
	EpochOf(t *testing.T, ns string) (uint64, bool)
	// RawSet stores a raw string without expiry.
	RawSet(t *testing.T, key, val string)
	// Delete removes a key, as if it had expired or been evicted.
	Delete(t *testing.T, key string)
	// DropBus breaks the bus connection of b once; b must report
	// EventDisconnected, reconnect by itself and report EventConnected.
	DropBus(t *testing.T, b l2.Backend)
}

// Run runs every conformance test as a parallel subtest of t, each with its
// own harness from newHarness and its own namespace.
func Run(t *testing.T, newHarness func(t *testing.T) Harness) {
	t.Helper()
	for _, tc := range cases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(newSuite(t, newHarness(t)))
		})
	}
}

type testCase struct {
	name string
	run  func(s *suite)
}

// suite is the per-test state: harness, a unique namespace and helpers.
type suite struct {
	t   *testing.T
	h   Harness
	ns  string
	cfg l2.Config
}

const (
	hour      = time.Hour
	eventWait = 15 * time.Second
	short     = 300 * time.Millisecond
)

func newSuite(t *testing.T, h Harness) *suite {
	t.Helper()
	var b [4]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails (Go >= 1.24)
	name := t.Name()
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	ns := strings.Map(func(r rune) rune {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if ok {
			return r
		}
		return '-'
	}, name) + "-" + hex.EncodeToString(b[:])
	return &suite{t: t, h: h, ns: ns, cfg: l2.Config{Namespace: ns, Channel: l2.DefaultChannel(ns)}}
}

// backend returns a backend whose subscription is active.
func (s *suite) backend() l2.Backend {
	return s.backendOn(s.cfg)
}

func (s *suite) backendOn(cfg l2.Config) l2.Backend {
	s.t.Helper()
	b := s.h.Backend(s.t, cfg)
	s.expect(b, l2.Event{Kind: l2.EventConnected})
	return b
}

func (s *suite) next(b l2.Backend) l2.Event {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.t.Context(), eventWait)
	defer cancel()
	ev, err := b.Receive(ctx)
	require.NoError(s.t, err, "waiting for a bus event")
	return ev
}

func (s *suite) expect(b l2.Backend, want l2.Event) {
	s.t.Helper()
	require.Equal(s.t, want, s.next(b))
}

// quiet asserts that b received nothing since the last expected event:
// it publishes a marker invalidation for key in an unused epoch (same hash
// slot as key) and requires it to be the next event.
func (s *suite) quiet(b l2.Backend, key string) {
	s.t.Helper()
	const markerEpoch = 999_999
	res, err := b.Invalidate(s.t.Context(), l2.InvalidateRequest{Epoch: markerEpoch, Key: key, GenTTL: hour})
	require.NoError(s.t, err)
	s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: markerEpoch, Gen: res.Gen, Key: key})
}

func (s *suite) read(b l2.Backend, epoch uint64, key string, genTTL time.Duration) l2.ReadResult {
	s.t.Helper()
	r, err := b.Read(s.t.Context(), l2.ReadRequest{Epoch: epoch, Key: key, GenTTL: genTTL})
	require.NoError(s.t, err)
	return r
}

func (s *suite) fill(b l2.Backend, req l2.FillRequest) bool {
	s.t.Helper()
	stored, err := b.Fill(s.t.Context(), req)
	require.NoError(s.t, err)
	return stored
}

func (s *suite) invalidate(b l2.Backend, epoch uint64, key string, genTTL time.Duration) l2.WriteResult {
	s.t.Helper()
	res, err := b.Invalidate(s.t.Context(), l2.InvalidateRequest{Epoch: epoch, Key: key, GenTTL: genTTL})
	require.NoError(s.t, err)
	return res
}

func (s *suite) set(b l2.Backend, req l2.SetRequest) l2.WriteResult {
	s.t.Helper()
	res, err := b.Set(s.t.Context(), req)
	require.NoError(s.t, err)
	return res
}

func (s *suite) epoch(b l2.Backend) uint64 {
	s.t.Helper()
	e, err := b.Epoch(s.t.Context())
	require.NoError(s.t, err)
	return e
}

// lock takes the load lock of "k" in epoch 1.
func (s *suite) lock(b l2.Backend, token string, ttl time.Duration) bool {
	s.t.Helper()
	ok, err := b.Lock(s.t.Context(), 1, "k", token, ttl)
	require.NoError(s.t, err)
	return ok
}

// genTTL asserts the remaining TTL of key's generation is want, allowing
// the harness tolerance below it.
func (s *suite) genTTL(key string, want time.Duration) {
	s.t.Helper()
	got, ok := s.h.GenTTL(s.t, s.ns, 1, key)
	require.True(s.t, ok, "generation key exists")
	s.within(got, want)
}

// within asserts want-tolerance <= got <= want.
func (s *suite) within(got, want time.Duration) {
	s.t.Helper()
	require.LessOrEqual(s.t, got, want)
	require.GreaterOrEqual(s.t, got, want-s.h.Tolerance())
}

// micros returns t as Redis TIME in microseconds.
func micros(t time.Time) uint64 {
	return uint64(t.UnixMicro()) //nolint:gosec // G115: test clocks are after 1970
}
