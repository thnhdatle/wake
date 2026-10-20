// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"path"
	"slices"
)

// TableFilter matches qualified table names "<schema>.<table>" against
// `tables` globs (sinks[].tables; also usable for tables.include/exclude).
//
// Glob semantics are those of path.Match applied to the whole qualified
// name: `*` matches any run of characters, `?` one character, `[...]` a
// character class, `\` escapes. Because CDC event identifiers never contain '/',
// `*` also matches the '.' between schema and table: "*" matches every
// table, "public.*" every table of schema public, "*.orders" every table
// named orders, and "pub*" both "public.x" and "pub_2.y". An empty pattern
// list matches nothing.
type TableFilter struct {
	patterns []string
	all      bool
}

// NewTableFilter compiles patterns; it returns ErrBadTablePattern for an
// empty or malformed pattern.
func NewTableFilter(patterns []string) (TableFilter, error) {
	f := TableFilter{patterns: slices.Clone(patterns)}
	for _, p := range patterns {
		if p == "" {
			return TableFilter{}, fmt.Errorf("%w: empty pattern", ErrBadTablePattern)
		}
		if _, err := path.Match(p, ""); err != nil {
			return TableFilter{}, fmt.Errorf("%w: %q", ErrBadTablePattern, p)
		}
		if p == "*" {
			f.all = true
		}
	}
	return f, nil
}

// Match reports whether the qualified name matches any pattern.
func (f TableFilter) Match(qualified string) bool {
	if f.all {
		return true
	}
	for _, p := range f.patterns {
		if ok, _ := path.Match(p, qualified); ok {
			return true
		}
	}
	return false
}

// IsEmpty reports whether the filter matches no table (`tables: []`).
func (f TableFilter) IsEmpty() bool { return len(f.patterns) == 0 }

// Patterns returns a copy of the patterns.
func (f TableFilter) Patterns() []string { return slices.Clone(f.patterns) }
