// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestBackoff_Delay(t *testing.T) {
	t.Parallel()
	b := pipeline.Backoff{Initial: 100 * time.Millisecond, Max: time.Second}
	cases := []struct {
		name    string
		b       pipeline.Backoff
		attempt int
		r       float64
		want    time.Duration
	}{
		{name: "first retry low jitter", b: b, attempt: 0, r: 0, want: 50 * time.Millisecond},
		{name: "first retry high jitter", b: b, attempt: 0, r: 1, want: 100 * time.Millisecond},
		{name: "doubles", b: b, attempt: 2, r: 0.5, want: 300 * time.Millisecond},
		{name: "capped", b: b, attempt: 10, r: 1, want: time.Second},
		{name: "huge attempt stays capped", b: b, attempt: 1 << 30, r: 0, want: 500 * time.Millisecond},
		{name: "r clamped above", b: b, attempt: 0, r: 7, want: 100 * time.Millisecond},
		{name: "r clamped below", b: b, attempt: 0, r: -1, want: 50 * time.Millisecond},
		{name: "defaults", b: pipeline.Backoff{}, attempt: 0, r: 1, want: pipeline.DefaultBackoffInitial},
		{name: "default max", b: pipeline.Backoff{}, attempt: 100, r: 1, want: pipeline.DefaultBackoffMax},
		{name: "initial above max", b: pipeline.Backoff{Initial: time.Minute, Max: time.Second}, r: 1, want: time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.b.Delay(tc.attempt, tc.r))
		})
	}
}

func TestBackoff_Delay_BoundedAndMonotonic(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		initial := time.Duration(rapid.Int64Range(1, int64(time.Second)).Draw(t, "initial"))
		ceiling := time.Duration(rapid.Int64Range(1, int64(time.Hour)).Draw(t, "max"))
		b := pipeline.Backoff{Initial: initial, Max: ceiling}
		attempt := rapid.IntRange(0, 100).Draw(t, "attempt")
		r := rapid.Float64Range(0, 1).Draw(t, "r")
		d := b.Delay(attempt, r)
		assert.LessOrEqual(t, d, ceiling)
		assert.Positive(t, d+1)
		assert.LessOrEqual(t, b.Delay(attempt, 0), b.Delay(attempt+1, 0), "non-decreasing in attempt")
	})
}
