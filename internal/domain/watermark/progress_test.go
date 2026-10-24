// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/watermark"
)

func TestProgress_Complete_Order(t *testing.T) {
	t.Parallel()
	type step struct {
		complete uint64
		want     string
		ok       bool
	}
	cases := []struct {
		name  string
		steps []step
	}{
		{name: "in order", steps: []step{
			{complete: 0, want: "a", ok: true}, {complete: 2, want: "c", ok: true}, {complete: 5, want: "d", ok: true},
		}},
		{name: "out of order waits for the gap", steps: []step{
			{complete: 2}, {complete: 5}, {complete: 0, want: "d", ok: true},
		}},
		{name: "middle first", steps: []step{
			{complete: 2}, {complete: 0, want: "c", ok: true}, {complete: 5, want: "d", ok: true},
		}},
		{name: "repeated acknowledgement is a no-op", steps: []step{
			{complete: 0, want: "a", ok: true},
			{complete: 0, want: "a", ok: true},
			{complete: 2, want: "c", ok: true},
			{complete: 0, want: "c", ok: true},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var p watermark.Progress[string]
			_, ok := p.Committed()
			require.False(t, ok)
			require.NoError(t, p.Register(0, "a"))
			require.NoError(t, p.Register(2, "c"))
			require.NoError(t, p.Register(5, "d"))
			for _, s := range tc.steps {
				require.NoError(t, p.Complete(s.complete))
				got, ok := p.Committed()
				assert.Equal(t, s.ok, ok)
				assert.Equal(t, s.want, got)
			}
		})
	}
}

func TestProgress_Calls_Errors(t *testing.T) {
	t.Parallel()
	var p watermark.Progress[int]
	require.ErrorIs(t, p.Complete(0), watermark.ErrUnknownChunk)
	require.NoError(t, p.Register(3, 30))
	require.ErrorIs(t, p.Register(3, 31), watermark.ErrChunkOrder)
	require.ErrorIs(t, p.Register(2, 20), watermark.ErrChunkOrder)
	require.ErrorIs(t, p.Complete(4), watermark.ErrUnknownChunk)
	require.ErrorIs(t, p.Complete(2), watermark.ErrUnknownChunk)
	assert.Equal(t, 1, p.Pending())
	require.NoError(t, p.Complete(3))
	assert.Zero(t, p.Pending())
	require.ErrorIs(t, p.Complete(9), watermark.ErrUnknownChunk)
}

func TestResumeProgress_StartsFromPersisted(t *testing.T) {
	t.Parallel()
	p := watermark.ResumeProgress(`[41]`)
	got, ok := p.Committed()
	require.True(t, ok)
	assert.Equal(t, `[41]`, got)
	require.NoError(t, p.Register(0, `[60]`))
	require.NoError(t, p.Register(1, `[80]`))
	require.NoError(t, p.Complete(1))
	got, _ = p.Committed()
	assert.Equal(t, `[41]`, got, "chunk 0 still unacknowledged")
	require.NoError(t, p.Complete(0))
	got, _ = p.Committed()
	assert.Equal(t, `[80]`, got)
}

func TestProgress_Committed_ContiguousPrefix(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		var p watermark.Progress[int]
		var chunks []uint64
		done := map[uint64]bool{}
		next := uint64(0)
		for range rapid.IntRange(1, 60).Draw(t, "steps") {
			if rapid.Bool().Draw(t, "register") || len(chunks) == 0 {
				next += rapid.Uint64Range(0, 3).Draw(t, "gap")
				require.NoError(t, p.Register(next, int(next)*10)) //nolint:gosec // G115: small test values
				chunks = append(chunks, next)
				next++
			} else {
				c := rapid.SampledFrom(chunks).Draw(t, "complete")
				require.NoError(t, p.Complete(c))
				done[c] = true
			}
			want, wantOK := 0, false
			for _, c := range chunks {
				if !done[c] {
					break
				}
				want, wantOK = int(c)*10, true //nolint:gosec // G115: small test values
			}
			got, ok := p.Committed()
			require.Equal(t, wantOK, ok)
			require.Equal(t, want, got)
		}
	})
}
