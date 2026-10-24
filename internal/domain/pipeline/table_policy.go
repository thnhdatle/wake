// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"maps"
	"strings"

	"github.com/thnhdatle/wake/internal/domain/change"
)

type SkipReason string

const (
	SkipNone              SkipReason = ""
	SkipNotCaptured       SkipReason = "not_captured"
	SkipIdentifierInvalid SkipReason = "identifier_invalid"
	SkipKeyless           SkipReason = "table_keyless"
)

type TableDecision struct {
	Capture bool
	Source  change.Source
	Skip    SkipReason
}

type TableResolver interface {
	Resolve(t TableInfo) TableDecision
}

type TablePolicyConfig struct {
	DB          string
	NameMap     map[string]string
	EmitUnkeyed bool
	Include     []string
	Exclude     []string
}

// TablePolicy decides which tables are replicated.
type TablePolicy struct {
	db          string
	nameMap     map[string]change.Source
	emitUnkeyed bool
	include     *TableFilter
	exclude     TableFilter
}

// NewTablePolicy builds a table policy from its config.
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
