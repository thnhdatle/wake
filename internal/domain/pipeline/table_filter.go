// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"path"
	"slices"
)

type TableFilter struct {
	patterns []string
	all      bool
}

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

func (f TableFilter) IsEmpty() bool { return len(f.patterns) == 0 }

func (f TableFilter) Patterns() []string { return slices.Clone(f.patterns) }
