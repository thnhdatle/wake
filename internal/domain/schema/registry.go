// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/thnhdatle/wake/internal/domain/change"
)

type TableState struct {
	Table    string
	Version  uint64
	Identity ReplicaIdentity
	Columns  []Column
}

type RegistryState struct {
	Floor  uint64
	Tables []TableState
}

type Observation struct {
	Table   string
	Version uint64
	Changed bool
	Before  []change.ColumnType
	After   []change.ColumnType
}

// Registry tracks the schema version of each table.
type Registry struct {
	floor  uint64
	tables map[string]*tableEntry
}

type tableEntry struct {
	version  uint64
	identity ReplicaIdentity
	columns  []Column
	live     bool
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{tables: make(map[string]*tableEntry)}
}

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

func (r *Registry) Reinit(nowUnix int64) {
	floor := uint64(max(nowUnix, 0))
	r.floor = max(r.floor, floor)
	for _, e := range r.tables {
		e.version = max(e.version, floor)
	}
}

func (r *Registry) Version(table string) (uint64, bool) {
	e, ok := r.tables[table]
	if !ok {
		return 0, false
	}
	return e.version, true
}

// Observe records a relation and reports whether its schema changed.
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
