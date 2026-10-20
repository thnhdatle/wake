// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/watermark"
)

// FuzzParsePayload checks that any accepted payload re-encodes to bytes
// that decode to the same value.
func FuzzParsePayload(f *testing.F) {
	f.Add([]byte(`{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":0,"mark":"low"}`))
	f.Add([]byte(`{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":2,"mark":"high",` +
		`"snapshot":"100:110:100,104","committed_xip":[104]}`))
	f.Add([]byte(`{"mark":"high","committed_xip":[null]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := watermark.ParsePayload(data)
		if err != nil {
			require.ErrorIs(t, err, watermark.ErrInvalidPayload)
			return
		}
		out, err := p.MarshalJSON()
		require.NoError(t, err)
		again, err := watermark.ParsePayload(out)
		require.NoError(t, err, "re-decode of %s", out)
		require.Equal(t, p, again)
	})
}

// FuzzParseSnapshot checks that any accepted snapshot is valid and formats
// to text that parses to the same value.
func FuzzParseSnapshot(f *testing.F) {
	f.Add("10:20:")
	f.Add("10:20:10,12,12,19")
	f.Add("4294967299:4294967310:4294967300")
	f.Fuzz(func(t *testing.T, text string) {
		s, err := watermark.ParseSnapshot(text)
		if err != nil {
			require.ErrorIs(t, err, watermark.ErrInvalidSnapshot)
			return
		}
		require.NoError(t, s.Validate())
		again, err := watermark.ParseSnapshot(s.String())
		require.NoError(t, err)
		require.Equal(t, s, again)
	})
}

// FuzzParseTrigger checks that any accepted trigger round-trips.
func FuzzParseTrigger(f *testing.F) {
	f.Add([]byte(`{"table":"public.users"}`))
	f.Add([]byte(`{"table":"app.v1.events"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		tr, err := watermark.ParseTrigger(data)
		if err != nil {
			require.ErrorIs(t, err, watermark.ErrInvalidTrigger)
			return
		}
		out, err := tr.MarshalJSON()
		require.NoError(t, err)
		again, err := watermark.ParseTrigger(out)
		require.NoError(t, err)
		require.Equal(t, tr, again)
	})
}
