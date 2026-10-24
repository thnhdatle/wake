// SPDX-License-Identifier: Apache-2.0

package l2test

import (
	"context"
	"sync"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

func cases() []testCase {
	return []testCase{
		{"read_initializes_generation_from_time", readInitializesGeneration},
		{"fill_is_generation_checked", fillIsGenerationChecked},
		{"fill_after_invalidate_is_rejected", fillAfterInvalidateRejected},
		{"fill_releases_lock_and_publishes_loaded", fillReleasesLock},
		{"set_and_invalidate_increment_and_publish", setAndInvalidatePublish},
		{"write_initializes_missing_epoch", writeInitializesEpoch},
		{"gen_ttl_extended_never_shortened", genTTLExtendOnly},
		{"generation_never_repeats_after_expiry", generationNeverRepeats},
		{"lock_nx_px_and_compare_delete", lockNXAndCompareDelete},
		{"epoch_initialized_from_time_and_bumped", epochInitAndBump},
		{"bus_delivers_to_every_backend", busDeliversToAll},
		{"binary_payloads_and_odd_keys", binaryPayloadsAndOddKeys},
		{"corrupt_records_are_misses", corruptRecordsAreMisses},
		{"epochs_isolate_keys", epochsIsolateKeys},
		{"concurrent_invalidations_are_atomic", concurrentInvalidations},
		{"receive_honors_context_and_close", receiveContextAndClose},
		{"bus_reconnects_after_drop", busReconnects},
	}
}

func readInitializesGeneration(s *suite) {
	t, h := s.t, s.h
	before := micros(h.Now(t))
	b := s.backend()
	r1 := s.read(b, 1, "k", hour)
	assert.False(t, r1.Hit)
	assert.GreaterOrEqual(t, r1.Gen, before, "initialized to TIME")
	assert.LessOrEqual(t, r1.Gen, micros(h.Now(t).Add(time.Second)))
	r2 := s.read(b, 1, "k", hour)
	assert.Equal(t, r1.Gen, r2.Gen, "stable while it exists")
	r3 := s.read(b, 1, "other", hour)
	assert.Greater(t, r3.Gen, r1.Gen, "a later script reads a later TIME")
	g, ok := h.Gen(t, s.ns, 1, "k")
	require.True(t, ok)
	assert.Equal(t, r1.Gen, g)
	s.genTTL("k", hour)
}

func fillIsGenerationChecked(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	r := s.read(b, 1, "k", hour)
	assert.False(t, s.fill(b, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen + 1, Payload: []byte("x"), TTL: hour}))
	_, ok := h.Value(t, s.ns, 1, "k")
	assert.False(t, ok, "a rejected fill writes nothing")

	startMS := h.Now(t).UnixMilli()
	fresh := l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen, Payload: []byte("v"), TTL: short, GenTTL: hour}
	require.True(t, s.fill(b, fresh))
	s.expect(b, l2.Event{Kind: l2.EventLoaded, Epoch: 1, Key: "k"})
	v, ok := h.Value(t, s.ns, 1, "k")
	require.True(t, ok)
	assert.Equal(t, r.Gen, v.Gen)
	assert.Equal(t, []byte("v"), v.Payload)
	assert.GreaterOrEqual(t, v.ExpiresAtMS, startMS+short.Milliseconds())
	hit := s.read(b, 1, "k", hour)
	require.True(t, hit.Hit)
	assert.Equal(t, r.Gen, hit.Gen)
	assert.Equal(t, []byte("v"), hit.Payload)
	s.within(hit.TTL, short)

	h.Elapse(t, short)
	miss := s.read(b, 1, "k", hour)
	assert.False(t, miss.Hit, "expired value")
	assert.Equal(t, r.Gen, miss.Gen)
	_, ok = h.Value(t, s.ns, 1, "k")
	assert.False(t, ok, "value key expired with PX")
}

func fillAfterInvalidateRejected(s *suite) {
	t := s.t
	b := s.backend()
	r := s.read(b, 1, "k", hour)
	inv := s.invalidate(b, 1, "k", hour)
	assert.Equal(t, r.Gen+1, inv.Gen)
	s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: inv.Gen, Key: "k"})
	assert.False(t, s.fill(b, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen, Payload: []byte("stale"), TTL: hour}))
	after := s.read(b, 1, "k", hour)
	assert.False(t, after.Hit)
	assert.Equal(t, inv.Gen, after.Gen)
	s.quiet(b, "k")
}

func fillReleasesLock(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	r := s.read(b, 1, "k", hour)
	require.True(t, s.lock(b, "tok", hour))
	stale := l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen + 5, Payload: []byte("x"), TTL: hour, LockToken: "tok"}
	assert.False(t, s.fill(b, stale))
	assert.False(t, h.Locked(t, s.ns, 1, "k"), "released even when the write is rejected")
	s.expect(b, l2.Event{Kind: l2.EventLoaded, Epoch: 1, Key: "k"})

	require.True(t, s.lock(b, "mine", hour))
	other := l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen, Payload: []byte("v"), TTL: hour, LockToken: "theirs"}
	assert.True(t, s.fill(b, other))
	assert.True(t, h.Locked(t, s.ns, 1, "k"), "another token does not release")
	s.expect(b, l2.Event{Kind: l2.EventLoaded, Epoch: 1, Key: "k"})

	stale.LockToken = ""
	assert.False(t, s.fill(b, stale))
	s.quiet(b, "k")
}

func setAndInvalidatePublish(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	epoch := s.epoch(b)
	r := s.read(b, 1, "k", hour)
	require.True(t, s.fill(b, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen, Payload: []byte("v"), TTL: hour}))
	s.expect(b, l2.Event{Kind: l2.EventLoaded, Epoch: 1, Key: "k"})

	inv := s.invalidate(b, 1, "k", hour)
	assert.Equal(t, l2.WriteResult{Gen: r.Gen + 1, Epoch: epoch}, inv)
	s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: r.Gen + 1, Key: "k"})
	_, ok := h.Value(t, s.ns, 1, "k")
	assert.False(t, ok, "value deleted")

	set := s.set(b, l2.SetRequest{Epoch: 1, Key: "k", Payload: []byte("s"), TTL: hour, GenTTL: hour})
	assert.Equal(t, l2.WriteResult{Gen: r.Gen + 2, Epoch: epoch}, set)
	s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: r.Gen + 2, Key: "k"})
	v, ok := h.Value(t, s.ns, 1, "k")
	require.True(t, ok)
	assert.Equal(t, set.Gen, v.Gen)
	hit := s.read(b, 1, "k", hour)
	require.True(t, hit.Hit)
	assert.Equal(t, []byte("s"), hit.Payload)
	assert.False(t, s.fill(b, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen + 1, Payload: []byte("old"), TTL: hour}))
	s.within(hit.TTL, hour)
}

func writeInitializesEpoch(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	before := micros(h.Now(t))
	inv := s.invalidate(b, 1, "k", hour)
	assert.GreaterOrEqual(t, inv.Epoch, before, "initialized to TIME")
	stored, ok := h.EpochOf(t, s.ns)
	require.True(t, ok)
	assert.Equal(t, inv.Epoch, stored)
	assert.Equal(t, inv.Epoch, s.epoch(b))
	s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: inv.Gen, Key: "k"})

	h.Delete(t, l2.EpochKey(s.ns))
	set := s.set(b, l2.SetRequest{Epoch: 1, Key: "k", Payload: []byte("v"), TTL: hour, GenTTL: hour})
	assert.Greater(t, set.Epoch, inv.Epoch, "re-initialized to a later TIME")
	assert.Equal(t, set.Epoch, s.epoch(b))
}

func genTTLExtendOnly(s *suite) {
	b := s.backend()
	s.read(b, 1, "k", hour)
	s.genTTL("k", hour)
	s.read(b, 1, "k", time.Minute)
	s.genTTL("k", hour)
	s.read(b, 1, "k", 2*hour)
	s.genTTL("k", 2*hour)
	s.invalidate(b, 1, "k", time.Minute)
	s.genTTL("k", 2*hour)
	s.set(b, l2.SetRequest{Epoch: 1, Key: "k", Payload: []byte("v"), TTL: time.Minute, GenTTL: 3 * hour})
	s.genTTL("k", 3*hour)
	r := s.read(b, 1, "k", time.Minute)
	s.fill(b, l2.FillRequest{Epoch: 1, Key: "k", Gen: r.Gen, Payload: []byte("v"), TTL: time.Minute, GenTTL: 4 * hour})
	s.genTTL("k", 4*hour)
	s.set(b, l2.SetRequest{Epoch: 1, Key: "new", Payload: []byte("v"), TTL: time.Minute, GenTTL: 5 * hour})
	s.genTTL("new", 5*hour)
}

func generationNeverRepeats(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	g1 := s.read(b, 1, "k", short).Gen
	g2 := s.invalidate(b, 1, "k", short).Gen
	g3 := s.invalidate(b, 1, "k", short).Gen
	assert.Equal(t, []uint64{g1 + 1, g1 + 2}, []uint64{g2, g3})
	h.Elapse(t, short)
	_, ok := h.Gen(t, s.ns, 1, "k")
	require.False(t, ok, "generation expired")
	g4 := s.read(b, 1, "k", hour).Gen
	assert.Greater(t, g4, g3, "a re-created generation is a later TIME")

	_, gkey, _ := l2.KeyNames(s.ns, 1, "k")
	h.Delete(t, gkey)
	g5 := s.invalidate(b, 1, "k", hour).Gen
	assert.Greater(t, g5, g4+1, "an evicted generation is re-created from TIME before the increment")
}

func lockNXAndCompareDelete(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	assert.True(t, s.lock(b, "a", hour))
	assert.False(t, s.lock(b, "b", hour), "NX")
	require.NoError(t, b.Unlock(t.Context(), 1, "k", "b"))
	assert.True(t, h.Locked(t, s.ns, 1, "k"), "a wrong token does not release")
	s.quiet(b, "k")
	require.NoError(t, b.Unlock(t.Context(), 1, "k", "a"))
	assert.False(t, h.Locked(t, s.ns, 1, "k"))
	s.expect(b, l2.Event{Kind: l2.EventLoaded, Epoch: 1, Key: "k"})

	require.True(t, s.lock(b, "c", short))
	h.Elapse(t, short)
	assert.False(t, h.Locked(t, s.ns, 1, "k"), "PX expiry")
	assert.True(t, s.lock(b, "d", hour))
	require.NoError(t, b.Unlock(t.Context(), 1, "k", "c"))
	assert.True(t, h.Locked(t, s.ns, 1, "k"), "an expired holder cannot release the next one")
	s.quiet(b, "k")
}

func epochInitAndBump(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	before := micros(h.Now(t))
	e := s.epoch(b)
	assert.GreaterOrEqual(t, e, before)
	assert.LessOrEqual(t, e, micros(h.Now(t).Add(time.Second)))
	assert.Equal(t, e, s.epoch(b), "stable")
	bumped, err := b.BumpEpoch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, e+1, bumped)
	s.expect(b, l2.Event{Kind: l2.EventEpoch, Epoch: e + 1})
	stored, ok := h.EpochOf(t, s.ns)
	require.True(t, ok)
	assert.Equal(t, e+1, stored)

	h.Delete(t, l2.EpochKey(s.ns))
	e3 := s.epoch(b)
	assert.Greater(t, e3, e+1)
	h.Delete(t, l2.EpochKey(s.ns))
	bumped, err = b.BumpEpoch(t.Context())
	require.NoError(t, err)
	assert.Greater(t, bumped, e3+1, "a missing epoch is initialized before the increment")
	s.expect(b, l2.Event{Kind: l2.EventEpoch, Epoch: bumped})
}

func busDeliversToAll(s *suite) {
	t := s.t
	a, b := s.backend(), s.backend()
	key := "user:42|x ü"
	inv := s.invalidate(a, 7, key, hour)
	want := l2.Event{Kind: l2.EventInvalidate, Epoch: 7, Gen: inv.Gen, Key: key}
	s.expect(a, want)
	s.expect(b, want)
	e, err := b.BumpEpoch(t.Context())
	require.NoError(t, err)
	s.expect(a, l2.Event{Kind: l2.EventEpoch, Epoch: e})
	s.expect(b, l2.Event{Kind: l2.EventEpoch, Epoch: e})

	other := s.backendOn(l2.Config{Namespace: s.ns, Channel: s.cfg.Channel + "-other"})
	s.quiet(other, key)
}

func binaryPayloadsAndOddKeys(s *suite) {
	t := s.t
	b := s.backend()
	payload := make([]byte, 0, 512)
	for i := range 512 {
		payload = append(payload, byte(i))
	}
	for _, key := range []string{"a|b|c", "{tag}x", "a}b{c", "ключ 🔑", " "} {
		set := s.set(b, l2.SetRequest{Epoch: 3, Key: key, Payload: payload, TTL: hour, GenTTL: hour})
		s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: 3, Gen: set.Gen, Key: key})
		hit := s.read(b, 3, key, hour)
		require.True(t, hit.Hit, "key %q", key)
		assert.Equal(t, payload, hit.Payload)
		assert.True(t, s.fill(b, l2.FillRequest{Epoch: 3, Key: key, Gen: set.Gen, Payload: nil, TTL: hour}))
		s.expect(b, l2.Event{Kind: l2.EventLoaded, Epoch: 3, Key: key})
		empty := s.read(b, 3, key, hour)
		require.True(t, empty.Hit)
		assert.Empty(t, empty.Payload)
	}
}

func corruptRecordsAreMisses(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	r := s.read(b, 1, "k", hour)
	v, g, _ := l2.KeyNames(s.ns, 1, "k")
	future := h.Now(t).Add(hour).UnixMilli()
	for _, raw := range []string{
		"garbage",
		"1|2",
		formatValue(r.Gen, 0, "p")[1:],
		formatValue(r.Gen-1, future, "old"),
		formatValue(r.Gen, 1, "expired"),
		formatValue(r.Gen, future, "p")[:len(formatValue(r.Gen, future, "p"))-2] + "x|",
	} {
		h.RawSet(t, v, raw)
		got := s.read(b, 1, "k", hour)
		assert.False(t, got.Hit, "raw %q", raw)
		assert.Equal(t, r.Gen, got.Gen)
	}
	h.RawSet(t, v, formatValue(r.Gen, future, "ok"))
	ok := s.read(b, 1, "k", hour)
	require.True(t, ok.Hit, "a well-formed record written by another client is valid")
	assert.Equal(t, []byte("ok"), ok.Payload)

	h.RawSet(t, g, "not-a-number")
	before := micros(h.Now(t))
	re := s.read(b, 1, "k", hour)
	assert.False(t, re.Hit)
	assert.GreaterOrEqual(t, re.Gen, before, "an unparsable generation is re-initialized from TIME")
}

func formatValue(gen uint64, expMS int64, payload string) string {
	return string(l2.EncodeValue(l2.StoredValue{Gen: gen, ExpiresAtMS: expMS, Payload: []byte(payload)}))
}

func epochsIsolateKeys(s *suite) {
	t, h := s.t, s.h
	b := s.backend()
	set := s.set(b, l2.SetRequest{Epoch: 1, Key: "k", Payload: []byte("v"), TTL: hour, GenTTL: hour})
	s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: set.Gen, Key: "k"})
	other := s.read(b, 2, "k", hour)
	assert.False(t, other.Hit)
	inv := s.invalidate(b, 2, "k", hour)
	s.expect(b, l2.Event{Kind: l2.EventInvalidate, Epoch: 2, Gen: inv.Gen, Key: "k"})
	g1, _ := h.Gen(t, s.ns, 1, "k")
	assert.Equal(t, set.Gen, g1, "another epoch's write leaves the key alone")
	hit := s.read(b, 1, "k", hour)
	assert.True(t, hit.Hit)
}

func concurrentInvalidations(s *suite) {
	t := s.t
	const writers, each = 4, 25
	start := s.read(s.backend(), 1, "k", hour).Gen
	var wg sync.WaitGroup
	gens := make(chan uint64, writers*each)
	for range writers {
		b := s.h.Backend(t, s.cfg)
		wg.Go(func() {
			for range each {
				res, err := b.Invalidate(context.Background(), l2.InvalidateRequest{Epoch: 1, Key: "k", GenTTL: hour})
				if !assert.NoError(t, err) {
					return
				}
				gens <- res.Gen
			}
		})
	}
	wg.Wait()
	close(gens)
	seen := make(map[uint64]bool, writers*each)
	for g := range gens {
		assert.False(t, seen[g], "generation %d handed out twice", g)
		seen[g] = true
		assert.Greater(t, g, start)
		assert.LessOrEqual(t, g, start+writers*each)
	}
	assert.Len(t, seen, writers*each)
}

func receiveContextAndClose(s *suite) {
	t := s.t
	b := s.backend()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := b.Receive(ctx)
	require.ErrorIs(t, err, context.Canceled)

	got := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), eventWait)
		defer cancel()
		_, rerr := b.Receive(ctx)
		got <- rerr
	}()
	require.NoError(t, b.Close())
	require.ErrorIs(t, <-got, l2.ErrClosed, "Close unblocks a pending Receive")
	_, err = b.Receive(t.Context())
	require.ErrorIs(t, err, l2.ErrClosed)
	require.NoError(t, b.Close(), "idempotent")
}

func busReconnects(s *suite) {
	t, h := s.t, s.h
	a, b := s.backend(), s.backend()
	h.DropBus(t, a)
	s.expect(a, l2.Event{Kind: l2.EventDisconnected})
	s.expect(a, l2.Event{Kind: l2.EventConnected})
	inv := s.invalidate(b, 1, "after", hour)
	want := l2.Event{Kind: l2.EventInvalidate, Epoch: 1, Gen: inv.Gen, Key: "after"}
	s.expect(b, want)
	s.expect(a, want)
}
