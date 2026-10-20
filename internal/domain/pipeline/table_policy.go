// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"maps"
	"strings"

	"github.com/thnhdatle/wake/internal/domain/change"
)

// SkipReason tells why a published table is not captured.
type SkipReason string

// Reasons a table is skipped.
const (
	// SkipNone means the table is captured.
	SkipNone SkipReason = ""
	// SkipNotCaptured means the table matches no include glob or an exclude
	// glob.
	SkipNotCaptured SkipReason = "not_captured"
	// SkipIdentifierInvalid means the schema or table name is not a valid
	// event identifier and tables.name_map does not map it (error
	// identifier_invalid).
	SkipIdentifierInvalid SkipReason = "identifier_invalid"
	// SkipKeyless means the table has no key and tables.emit_unkeyed is off
	// (error table_keyless).
	SkipKeyless SkipReason = "table_keyless"
)

// TableDecision is a TableResolver's verdict on one table.
type TableDecision struct {
	// Capture reports whether the table's changes are emitted.
	Capture bool
	// Source is the CDC event source of the table's events: source.db and the
	// (possibly mapped) schema and table names. Zero when not captured.
	Source change.Source
	// Skip tells why the table is not captured.
	Skip SkipReason
}

// TableResolver decides whether a published table is captured and under
// which CDC event names (the table detection rules). The stream reader asks it for
// every Relation message and the snapshotter for every table it copies.
// Adapters may wrap it to apply publication refreshes.
type TableResolver interface {
	Resolve(t TableInfo) TableDecision
}

// TablePolicyConfig configures a TablePolicy.
type TablePolicyConfig struct {
	// DB is source.name, the events' source.db.
	DB string
	// NameMap is tables.name_map: "<schema>.<table>" to "<schema>.<table>".
	NameMap map[string]string
	// EmitUnkeyed is tables.emit_unkeyed.
	EmitUnkeyed bool
	// Include and Exclude are optional `tables` globs applied to the
	// Postgres name: a table is captured when it matches an include glob (an
	// empty Include captures every published table) and no exclude glob.
	Include []string
	Exclude []string
}

// TablePolicy is the static TableResolver: published tables are
// captured, renamed through tables.name_map, skipped when their names are
// not CDC event identifiers and unmapped, and skipped when keyless unless
// tables.emit_unkeyed is set. It is immutable and safe for concurrent use.
type TablePolicy struct {
	db          string
	nameMap     map[string]change.Source
	emitUnkeyed bool
	include     *TableFilter
	exclude     TableFilter
}

// NewTablePolicy validates the configuration: every name map key and target
// is "<schema>.<table>" and every target consists of CDC event identifiers.
func NewTablePolicy(cfg TablePolicyConfig) (*TablePolicy, error) {
	p := &TablePolicy{db: cfg.DB, emitUnkeyed: cfg.EmitUnkeyed, nameMap: make(map[string]change.Source, len(cfg.NameMap))}
	for from, to := range maps.All(cfg.NameMap) {
		if _, _, ok := strings.Cut(from, "."); !ok {
			return nil, fmt.Errorf("%w: name_map key %q", ErrInvalidTablePolicy, from)
		}
		schema, table, ok := strings.Cut(to, ".")
		if !ok || !change.ValidIdentifier(schema) || !change.ValidIdentifier(table) {
			return nil, fmt.Errorf("%w: name_map target %q", ErrInvalidTablePolicy, to)
		}
		p.nameMap[from] = change.Source{DB: cfg.DB, Schema: schema, Table: table}
	}
	if len(cfg.Include) > 0 {
		f, err := NewTableFilter(cfg.Include)
		if err != nil {
			return nil, err
		}
		p.include = &f
	}
	f, err := NewTableFilter(cfg.Exclude)
	if err != nil {
		return nil, err
	}
	p.exclude = f
	return p, nil
}

// Resolve implements TableResolver.
func (p *TablePolicy) Resolve(t TableInfo) TableDecision {
	qualified := t.Schema + "." + t.Table
	if (p.include != nil && !p.include.Match(qualified)) || p.exclude.Match(qualified) {
		return TableDecision{Skip: SkipNotCaptured}
	}
	src, ok := p.nameMap[qualified]
	if !ok {
		if !change.ValidIdentifier(t.Schema) || !change.ValidIdentifier(t.Table) {
			return TableDecision{Skip: SkipIdentifierInvalid}
		}
		src = change.Source{DB: p.db, Schema: t.Schema, Table: t.Table}
	}
	if !t.HasKey && !p.emitUnkeyed {
		return TableDecision{Skip: SkipKeyless}
	}
	return TableDecision{Capture: true, Source: src}
}
