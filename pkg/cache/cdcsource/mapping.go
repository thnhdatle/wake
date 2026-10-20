// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/thnhdatle/wake/pkg/event"
)

// identifierRE is the CDC event naming rule for schema, table and prefix names.
var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Mapping maps tables, named "<schema>.<table>" as in the events, to the key
// templates whose rendered keys are invalidated when a row of the table
// changes. It is immutable and safe for concurrent use.
type Mapping struct {
	tables map[string][]Template
	// columns lists, per table, every column referenced by its templates.
	columns map[string][]string
	names   []string
}

// NewMapping parses templates, a map from "<schema>.<table>" to key templates
// such as {"public.users": {"user:{id}", "user:email:{email}"}}. Schema and
// table must match [A-Za-z0-9_-]+ (they name topics and subjects) and each
// table needs at least one template. All problems are reported at once,
// joined; each matches ErrMapping or ErrTemplate.
func NewMapping(templates map[string][]string) (*Mapping, error) {
	if len(templates) == 0 {
		return nil, fmt.Errorf("%w: no tables", ErrMapping)
	}
	m := &Mapping{
		tables:  make(map[string][]Template, len(templates)),
		columns: make(map[string][]string, len(templates)),
		names:   slices.Sorted(maps.Keys(templates)),
	}
	var errs []error
	for _, name := range m.names {
		if _, _, ok := splitTable(name); !ok {
			errs = append(errs, fmt.Errorf("%w: table %q must have the form <schema>.<table> with names matching "+
				"[A-Za-z0-9_-]+", ErrMapping, name))
		}
		list := templates[name]
		if len(list) == 0 {
			errs = append(errs, fmt.Errorf("%w: table %q has no key template", ErrMapping, name))
		}
		var cols []string
		for _, raw := range list {
			t, err := ParseTemplate(raw)
			if err != nil {
				errs = append(errs, fmt.Errorf("table %s: %w", name, err))
				continue
			}
			m.tables[name] = append(m.tables[name], t)
			for _, c := range t.Columns() {
				if !slices.Contains(cols, c) {
					cols = append(cols, c)
				}
			}
		}
		m.columns[name] = cols
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return m, nil
}

// splitTable splits "<schema>.<table>" and checks both names.
func splitTable(name string) (schema, table string, ok bool) {
	schema, table, found := strings.Cut(name, ".")
	if !found || !identifierRE.MatchString(schema) || !identifierRE.MatchString(table) {
		return "", "", false
	}
	return schema, table, true
}

// Tables returns the mapped tables, sorted.
func (m *Mapping) Tables() []string { return slices.Clone(m.names) }

// Templates returns the templates of table, nil when it is not mapped.
func (m *Mapping) Templates(table string) []Template { return slices.Clone(m.tables[table]) }

// Keys returns the cache keys to invalidate for e, without duplicates, in
// template order (before image first). Every template of the event's table
// is rendered against each available row image, Before and After, so a
// changed unique attribute invalidates both its old and its new key.
//
// A placeholder resolves from Key, then from the image. For the Before image
// the image comes first, so that an update which changed the primary key
// (Event.KeyChanged) also invalidates the keys of the old key. Integers
// render as their exact decimal literal, strings verbatim (Value.Text);
// a null, absent or non-scalar value skips that rendering, as does an empty
// result. Keys returns nil for unmapped tables. It does not look at Op: the
// caller decides which events to invalidate.
func (m *Mapping) Keys(e *event.Event) []string {
	tpls := m.tables[tableName(e)]
	if len(tpls) == 0 {
		return nil
	}
	var keys []string
	add := func(primary, secondary event.Row) {
		lookup := rowLookup(primary, secondary)
		for _, t := range tpls {
			if k, ok := t.render(lookup); ok && !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
	}
	if e.Before != nil {
		add(e.Before, e.Key)
	}
	if e.After != nil {
		add(e.Key, e.After)
	}
	return keys
}

// missingFromBefore returns the columns referenced by the templates of e's
// table that are not key columns and are absent from e.Before (all of them
// when Before is nil). Without REPLICA IDENTITY FULL the before image of
// updates and deletes carries the key columns only.
func (m *Mapping) missingFromBefore(e *event.Event) []string {
	var missing []string
	for _, c := range m.columns[tableName(e)] {
		if !e.Key.Has(c) && !e.Before.Has(c) {
			missing = append(missing, c)
		}
	}
	return missing
}

func tableName(e *event.Event) string { return e.Source.Schema + "." + e.Source.Table }

// rowLookup resolves a column from primary, else secondary, as template text.
func rowLookup(primary, secondary event.Row) func(string) (string, bool) {
	return func(col string) (string, bool) {
		v, ok := primary.Value(col)
		if !ok {
			v, ok = secondary.Value(col)
		}
		if !ok || v.IsNull() {
			return "", false
		}
		s, err := v.Text()
		return s, err == nil
	}
}

// TableInfo describes a source table for CheckReplicaIdentity. Schema and
// Table are the names the events carry (after any producer name mapping).
type TableInfo struct {
	Schema string
	Table  string
	// Columns are the published columns of the table.
	Columns []string
	// IdentityColumns are the replica identity key columns: the primary key
	// for the default identity, the index columns for REPLICA IDENTITY USING
	// INDEX, none for NOTHING. Ignored when IdentityFull is true.
	IdentityColumns []string
	// IdentityFull is true for REPLICA IDENTITY FULL.
	IdentityFull bool
}

// CheckReplicaIdentity checks the mapping against table metadata read from
// the source database (the validate_dsn_env check). It reports
// every mapped table missing from tables (ErrUnknownTable), every template
// column the table does not have (ErrUnknownColumn) and, as *IdentityError
// (ErrRequiresFullIdentity), the non-key columns referenced on a table whose
// replica identity is not FULL. Problems are joined; nil means none.
func (m *Mapping) CheckReplicaIdentity(tables []TableInfo) error {
	byName := make(map[string]TableInfo, len(tables))
	for _, ti := range tables {
		byName[ti.Schema+"."+ti.Table] = ti
	}
	var errs []error
	for _, name := range m.names {
		ti, ok := byName[name]
		if !ok {
			errs = append(errs, fmt.Errorf("%w: table %s not found in the source database", ErrUnknownTable, name))
			continue
		}
		var unknown, nonKey []string
		for _, c := range m.columns[name] {
			switch {
			case !slices.Contains(ti.Columns, c):
				unknown = append(unknown, c)
			case !ti.IdentityFull && !slices.Contains(ti.IdentityColumns, c):
				nonKey = append(nonKey, c)
			}
		}
		if len(unknown) > 0 {
			slices.Sort(unknown)
			errs = append(errs, fmt.Errorf("%w: table %s has no published column [%s]",
				ErrUnknownColumn, name, strings.Join(unknown, ", ")))
		}
		if len(nonKey) > 0 {
			slices.Sort(nonKey)
			errs = append(errs, &IdentityError{Table: name, Columns: nonKey})
		}
	}
	return errors.Join(errs...)
}
