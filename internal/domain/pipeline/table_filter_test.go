// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestTableFilter_Match(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		patterns []string
		table    string
		want     bool
	}{
		{name: "star matches all", patterns: []string{"*"}, table: "public.orders", want: true},
		{name: "exact", patterns: []string{"public.orders"}, table: "public.orders", want: true},
		{name: "exact other table", patterns: []string{"public.orders"}, table: "public.users", want: false},
		{name: "schema glob", patterns: []string{"public.*"}, table: "public.users", want: true},
		{name: "schema glob other schema", patterns: []string{"public.*"}, table: "sales.users", want: false},
		{name: "table glob", patterns: []string{"*.orders"}, table: "sales.orders", want: true},
		{name: "star crosses dot", patterns: []string{"pub*"}, table: "public.x", want: true},
		{name: "question mark", patterns: []string{"public.user?"}, table: "public.users", want: true},
		{name: "class", patterns: []string{"public.[ou]*"}, table: "public.users", want: true},
		{name: "second pattern", patterns: []string{"a.b", "public.*"}, table: "public.t", want: true},
		{name: "empty list matches nothing", patterns: []string{}, table: "public.t", want: false},
		{name: "nil list matches nothing", patterns: nil, table: "public.t", want: false},
		{name: "prefix only is no match", patterns: []string{"public"}, table: "public.t", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := pipeline.NewTableFilter(tc.patterns)
			require.NoError(t, err)
			assert.Equal(t, tc.want, f.Match(tc.table))
			assert.Equal(t, len(tc.patterns) == 0, f.IsEmpty())
		})
	}
}

func TestNewTableFilter_BadPattern(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		patterns []string
	}{
		{name: "empty pattern", patterns: []string{""}},
		{name: "unclosed class", patterns: []string{"public.["}},
		{name: "trailing escape", patterns: []string{"*", "public.\\"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pipeline.NewTableFilter(tc.patterns)
			require.ErrorIs(t, err, pipeline.ErrBadTablePattern)
		})
	}
}

func TestTableFilter_Patterns_ReturnsCopy(t *testing.T) {
	t.Parallel()
	in := []string{"public.*"}
	f, err := pipeline.NewTableFilter(in)
	require.NoError(t, err)
	in[0] = "x.y"
	got := f.Patterns()
	assert.Equal(t, []string{"public.*"}, got)
	got[0] = "z"
	assert.True(t, f.Match("public.a"))
}

func TestTableFilter_Match_StarMatchesEveryIdentifier(t *testing.T) {
	t.Parallel()
	ident := rapid.StringMatching(`[A-Za-z0-9_-]{1,20}`)
	all, err := pipeline.NewTableFilter([]string{"*"})
	require.NoError(t, err)
	rapid.Check(t, func(t *rapid.T) {
		schema, table := ident.Draw(t, "schema"), ident.Draw(t, "table")
		q := schema + "." + table
		assert.True(t, all.Match(q))
		exact, err := pipeline.NewTableFilter([]string{q})
		require.NoError(t, err)
		assert.True(t, exact.Match(q))
		sch, err := pipeline.NewTableFilter([]string{schema + ".*"})
		require.NoError(t, err)
		assert.True(t, sch.Match(q))
	})
}
