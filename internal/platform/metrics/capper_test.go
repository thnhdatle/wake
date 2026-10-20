// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/wake/internal/platform/metrics"
)

func TestCapper_Label(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		limit    int
		reserved []string
		in       []string
		want     []string
	}{
		{"under the limit", 3, nil, []string{"a", "b"}, []string{"a", "b"}},
		{"beyond the limit", 2, nil, []string{"a", "b", "c", "a", "d"}, []string{"a", "b", "other", "a", "other"}},
		{"admitted value keeps passing", 1, nil, []string{"a", "b", "a"}, []string{"a", "other", "a"}},
		{
			"reserved never counts", 1,
			[]string{"_other"},
			[]string{"_other", "a", "_other", "b"},
			[]string{"_other", "a", "_other", "other"},
		},
		{"default limit for zero", 0, nil, []string{"a"}, []string{"a"}},
		{"invalid utf-8 replaced", 2, nil, []string{"a\xff"}, []string{"a?"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := metrics.NewCapper(tc.limit, tc.reserved...)
			got := make([]string, 0, len(tc.in))
			for _, v := range tc.in {
				got = append(got, c.Label(v))
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCapper_DefaultLimit(t *testing.T) {
	t.Parallel()
	c := metrics.NewCapper(0)
	for i := range metrics.DefaultLabelLimit {
		v := fmt.Sprintf("t%d", i)
		assert.Equal(t, v, c.Label(v))
	}
	assert.Equal(t, metrics.OtherLabel, c.Label("one-too-many"))
	assert.Equal(t, metrics.DefaultLabelLimit, c.Len())
}

func TestCapper_Concurrent(t *testing.T) {
	t.Parallel()
	const limit = 10
	c := metrics.NewCapper(limit)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				c.Label(fmt.Sprintf("v%d", (i+g)%40))
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, limit, c.Len())
}
