// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/thnhdatle/wake/internal/domain/change"
)

// TableState is the persisted schema state of one table.
type TableState struct {
	// Table is the registry key "<schema>.<table>" (Relation.QualifiedName).
	Table string
	// Version is the table's schema_version.
	Version uint64
	// Identity and Columns are the last observed shape; Columns is nil when
	// the shape is unknown, in which case the next observation sets it
	// without a schema change.
	Identity ReplicaIdentity
	Columns  []Column
}

// RegistryState is the registry's persisted form (state file).
type RegistryState struct {
	// Floor is the lowest version a table not seen before receives; it is
	// raised by Reinit so that tables whose state was lost never restart
	// below an earlier version.
	Floor  uint64
	Tables []TableState
}

// Observation is the outcome of Registry.Observe.
type Observation struct {
	Table   string
	Version uint64
	// Changed reports that the table's shape differs from the previous one
	// (in this process or in the seeded state): the caller emits one
	// schema-change event with Version, Before and After.
	Changed bool
	Before  []change.ColumnType
	After   []change.ColumnType
}

// Registry tracks per-table schema versions. Versions never decrease under
// any sequence of Seed, Observe and Reinit. It is not safe for concurrent
// use.
type Registry struct {
	floor  uint64
	tables map[string]*tableEntry
}

type tableEntry struct {
	version  uint64
	identity ReplicaIdentity
	columns  []Column // nil: unknown shape
	live     bool     // shape observed in this process
}

// NewRegistry returns an empty registry: every table starts at version 1.
func NewRegistry() *Registry {
	return &Registry{tables: make(map[string]*tableEntry)}
}

// Seed merges persisted state into the registry. Versions merge by maximum;
// a seeded shape replaces the current one only for tables not yet observed
// in this process. On error nothing is changed.
func (r *Registry) Seed(state RegistryState) error {
	seen := make(map[string]bool, len(state.Tables))
	for i := range state.Tables {
		t := &state.Tables[i]
		switch {
		case strings.TrimSpace(t.Table) == "":
			return fmt.Errorf("%w: empty table name", ErrInvalidState)
		case seen[t.Table]:
			return fmt.Errorf("%w: duplicate table %q", ErrInvalidState, t.Table)
		case t.Columns != nil && !t.Identity.Valid():
			return fmt.Errorf("%w: table %q: replica identity %q", ErrInvalidState, t.Table, byte(t.Identity))
		}
		seen[t.Table] = true
	}
	r.floor = max(r.floor, state.Floor)
	for i := range state.Tables {
		t := &state.Tables[i]
		e, ok := r.tables[t.Table]
		if !ok {
			e = &tableEntry{}
			r.tables[t.Table] = e
		}
		e.version = max(e.version, t.Version)
		if !e.live && t.Columns != nil {
			e.identity, e.columns = t.Identity, slices.Clone(t.Columns)
		}
	}
	return nil
}

// State exports the registry for persistence, sorted by table.
func (r *Registry) State() RegistryState {
	st := RegistryState{Floor: r.floor, Tables: make([]TableState, 0, len(r.tables))}
	for name, e := range r.tables {
		st.Tables = append(st.Tables, TableState{
			Table: name, Version: e.version, Identity: e.identity, Columns: slices.Clone(e.columns),
		})
	}
	slices.SortFunc(st.Tables, func(a, b TableState) int { return strings.Compare(a.Table, b.Table) })
	return st
}

// Reinit applies --reinit-state: every known table's version
// becomes max(version, nowUnix), and tables first seen later start at
// nowUnix at least. nowUnix is the current Unix time in seconds, passed in
// because the domain never reads the clock.
func (r *Registry) Reinit(nowUnix int64) {
	floor := uint64(max(nowUnix, 0))
	r.floor = max(r.floor, floor)
	for _, e := range r.tables {
		e.version = max(e.version, floor)
	}
}

// Version returns the current version of a table.
func (r *Registry) Version(table string) (uint64, bool) {
	e, ok := r.tables[table]
	if !ok {
		return 0, false
	}
	return e.version, true
}

// Observe records a Relation message and returns the table's version.
//
//   - Table never seen and not seeded: version max(1, floor), no change.
//   - Seeded or reinitialised without a shape: the shape is recorded, the
//     version is kept, no change.
//   - Shape equal to the previous one (Relation.SameSchema): no change.
//     pgoutput resends Relation messages in every session and after any
//     relcache invalidation, so this is the common case.
//   - Shape differs: version + 1 and Changed, with Before (previous columns)
//     and After (new columns). The version saturates at math.MaxUint64.
//
// A returned version is always at least 1, also for a table seeded with
// version 0.
func (r *Registry) Observe(rel *Relation) Observation {
	name := rel.QualifiedName()
	obs := Observation{Table: name}
	e, ok := r.tables[name]
	switch {
	case !ok:
		e = &tableEntry{version: max(1, r.floor)}
		r.tables[name] = e
	case e.columns == nil:
	case e.identity == rel.Identity && sameColumns(e.columns, rel.Columns):
	default:
		obs.Changed = true
		obs.Before = columnTypes(e.columns)
		obs.After = rel.ColumnTypes()
		if e.version < math.MaxUint64 {
			e.version++
		}
	}
	e.version = max(e.version, 1)
	e.identity, e.columns, e.live = rel.Identity, slices.Clone(rel.Columns), true
	if e.columns == nil {
		e.columns = []Column{}
	}
	obs.Version = e.version
	return obs
}
