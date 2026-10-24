// SPDX-License-Identifier: Apache-2.0

package l2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

func TestKeyNames_Layout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		ns, key       string
		epoch         uint64
		wantV, wantG  string
		wantL, wantEp string
	}{
		{
			"simple", "users", "user:42", 7,
			"users:e7:{user:42}:v", "users:e7:{user:42}:g", "users:e7:{user:42}:l", "users:epoch",
		},
		{
			"big epoch", "a-b_c", "k", 1760000000000000, "a-b_c:e1760000000000000:{k}:v", "a-b_c:e1760000000000000:{k}:g",
			"a-b_c:e1760000000000000:{k}:l", "a-b_c:epoch",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v, g, l := l2.KeyNames(tc.ns, tc.epoch, tc.key)
			assert.Equal(t, tc.wantV, v)
			assert.Equal(t, tc.wantG, g)
			assert.Equal(t, tc.wantL, l)
			assert.Equal(t, tc.wantEp, l2.EpochKey(tc.ns))
		})
	}
	assert.Equal(t, "wake:inval:users", l2.DefaultChannel("users"))
}

func TestEncodeMessage_Wire(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ev   l2.Event
		want string
	}{
		{
			"invalidate",
			l2.Event{Kind: l2.EventInvalidate, Epoch: 3, Gen: 1760000000000001, Key: "a|b"},
			"i|3|1760000000000001|a|b",
		},
		{"loaded", l2.Event{Kind: l2.EventLoaded, Epoch: 3, Key: "k"}, "l|3|k"},
		{"epoch", l2.Event{Kind: l2.EventEpoch, Epoch: 9}, "e|9"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, l2.EncodeMessage(tc.ev))
			got, err := l2.DecodeMessage(tc.want)
			require.NoError(t, err)
			assert.Equal(t, tc.ev, got)
		})
	}
}

func TestEncodeMessage_ConnectionEventsPanic(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() { l2.EncodeMessage(l2.Event{Kind: l2.EventConnected}) })
	assert.Panics(t, func() { l2.EncodeMessage(l2.Event{Kind: l2.EventDisconnected}) })
}

func TestDecodeMessage_Malformed(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "x", "x|1", "i|1", "i|1|2", "i|a|2|k", "i|1|b|k", "l|1", "l|z|k", "e|", "e|-1"} {
		t.Run(s, func(t *testing.T) {
			t.Parallel()
			_, err := l2.DecodeMessage(s)
			require.ErrorIs(t, err, l2.ErrMalformed)
		})
	}
}

func TestDecodeValue_Malformed(t *testing.T) {
	t.Parallel()
	for _, s := range []string{"", "1", "1|2", "a|2|p", "1|b|p"} {
		t.Run(s, func(t *testing.T) {
			t.Parallel()
			_, err := l2.DecodeValue([]byte(s))
			require.ErrorIs(t, err, l2.ErrMalformed)
		})
	}
}

func TestEncodeValue_RoundTripProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		v := l2.StoredValue{
			Gen:         rapid.Uint64().Draw(t, "gen"),
			ExpiresAtMS: rapid.Int64().Draw(t, "exp"),
			Payload:     rapid.SliceOf(rapid.Byte()).Draw(t, "payload"),
		}
		got, err := l2.DecodeValue(l2.EncodeValue(v))
		if err != nil {
			t.Fatal(err)
		}
		if got.Gen != v.Gen || got.ExpiresAtMS != v.ExpiresAtMS || string(got.Payload) != string(v.Payload) {
			t.Fatalf("round trip %+v -> %+v", v, got)
		}
	})
}

func TestEncodeMessage_RoundTripProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		kind := rapid.SampledFrom([]l2.EventKind{l2.EventInvalidate, l2.EventLoaded, l2.EventEpoch}).Draw(t, "kind")
		ev := l2.Event{Kind: kind, Epoch: rapid.Uint64().Draw(t, "epoch")}
		if kind != l2.EventEpoch {
			ev.Key = rapid.String().Draw(t, "key")
		}
		if kind == l2.EventInvalidate {
			ev.Gen = rapid.Uint64().Draw(t, "gen")
		}
		got, err := l2.DecodeMessage(l2.EncodeMessage(ev))
		if err != nil || got != ev {
			t.Fatalf("round trip %+v -> %+v, %v", ev, got, err)
		}
	})
}

func FuzzDecodeMessage(f *testing.F) {
	for _, s := range []string{"i|1|2|k", "l|1|k", "e|1", "i|1|2|a|b", "", "|||"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		ev, err := l2.DecodeMessage(s)
		if err != nil {
			return
		}
		if got := l2.EncodeMessage(ev); got != s {
			again, err := l2.DecodeMessage(got)
			if err != nil || again != ev {
				t.Fatalf("%q -> %+v -> %q", s, ev, got)
			}
		}
	})
}

func FuzzDecodeValue(f *testing.F) {
	for _, s := range []string{"1|2|p", "0|0|", "1|-5|a|b", "x"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := l2.DecodeValue(b)
		if err != nil {
			return
		}
		again, err := l2.DecodeValue(l2.EncodeValue(v))
		same := err == nil && again.Gen == v.Gen && again.ExpiresAtMS == v.ExpiresAtMS
		if !same || string(again.Payload) != string(v.Payload) {
			t.Fatalf("%q not stable", b)
		}
	})
}
