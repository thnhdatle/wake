// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

// hasKeySQL is true when Postgres can identify rows of table c for UPDATE
// and DELETE: REPLICA IDENTITY FULL, DEFAULT with a primary key, or USING
// INDEX with an existing identity index (the keyless rule).
const hasKeySQL = `CASE c.relreplident
	WHEN 'f' THEN true
	WHEN 'd' THEN EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
	WHEN 'i' THEN EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid AND i.indisreplident)
	ELSE false END`

// indexColumnsPrefix + <pg_index condition> + indexColumnsSuffix lists the
// key columns of the chosen index of table c in index order.
const (
	indexColumnsPrefix = `ARRAY(SELECT a.attname::text FROM pg_catalog.pg_index i
	CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
	JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	WHERE i.indrelid = c.oid AND `
	indexColumnsSuffix   = ` AND k.ord <= i.indnkeyatts ORDER BY k.ord)`
	primaryKeyColumnsSQL = indexColumnsPrefix + "i.indisprimary" + indexColumnsSuffix
	identityColumnsSQL   = indexColumnsPrefix + "i.indisreplident" + indexColumnsSuffix
)

// Table describes a table as the publication management and the
// pre-flight check see it.
type Table struct {
	Schema string
	Name   string
	OID    uint32
	// Identity is pg_class.relreplident.
	Identity schema.ReplicaIdentity
	// HasKey is false for keyless tables.
	HasKey bool
	// Partitioned is true for a partitioned table, Partition for a leaf or
	// intermediate partition.
	Partitioned bool
	Partition   bool
	// Owned reports whether the current role owns the table (directly or
	// through role membership).
	Owned bool
}

// QualifiedName returns "<schema>.<table>".
func (t *Table) QualifiedName() string { return t.Schema + "." + t.Name }

// Info returns the table as a pipeline.TableInfo.
func (t *Table) Info() pipeline.TableInfo {
	return pipeline.TableInfo{Schema: t.Schema, Table: t.Name, OID: t.OID, HasKey: t.HasKey}
}

// sanitize quotes the table name for SQL.
func (t *Table) sanitize() string { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }

// tableColumnsSQL selects the Table fields of pg_class c.
const tableColumnsSQL = `c.oid, n.nspname::text, c.relname::text, c.relreplident::text, ` + hasKeySQL + `,
	c.relkind = 'p', c.relispartition, pg_catalog.pg_has_role(c.relowner, 'USAGE')`

// scanTables reads rows of tableColumnsSQL.
func scanTables(rows pgx.Rows) ([]Table, error) {
	defer rows.Close()
	var out []Table
	for rows.Next() {
		var t Table
		var ident string
		if err := rows.Scan(&t.OID, &t.Schema, &t.Name, &ident, &t.HasKey, &t.Partitioned, &t.Partition,
			&t.Owned); err != nil {
			return nil, fmt.Errorf("pgsql: scan table: %w", err)
		}
		if ident != "" {
			t.Identity = schema.ReplicaIdentity(ident[0])
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgsql: read tables: %w", err)
	}
	return out, nil
}

// column is a published column of a table.
type column struct {
	name    string
	typeOID uint32
}

// tableMeta is what a chunk read needs to know about a table.
type tableMeta struct {
	oid      uint32
	schema   string
	name     string
	identity schema.ReplicaIdentity
	// partitioned is true for a partitioned table (read through its root).
	partitioned bool
	// cols are the columns pgoutput publishes: not dropped, not generated,
	// in attribute order.
	cols []column
	// pk and ri are the primary key and replica identity index columns in
	// index order; nil when there is none.
	pk []string
	ri []string
}

// tableMetaSQL loads a tableMeta by schema and name.
const tableMetaSQL = `SELECT c.oid, c.relreplident::text, c.relkind = 'p',
	ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0
		AND NOT a.attisdropped AND a.attgenerated = '' ORDER BY a.attnum),
	ARRAY(SELECT a.atttypid::int8 FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0
		AND NOT a.attisdropped AND a.attgenerated = '' ORDER BY a.attnum),
	` + primaryKeyColumnsSQL + `,
	` + identityColumnsSQL + `
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')`

// loadTableMeta reads the table's columns and keys.
func loadTableMeta(ctx context.Context, q pgx.Tx, schemaName, table string) (*tableMeta, error) {
	m := &tableMeta{schema: schemaName, name: table}
	var ident string
	var names []string
	var types []int64
	err := q.QueryRow(ctx, tableMetaSQL, schemaName, table).Scan(&m.oid, &ident, &m.partitioned, &names, &types,
		&m.pk, &m.ri)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s.%s", ErrTableNotFound, schemaName, table)
	}
	if err != nil {
		return nil, fmt.Errorf("pgsql: read table %s.%s: %w", schemaName, table, err)
	}
	if len(names) != len(types) || ident == "" {
		return nil, fmt.Errorf("pgsql: table %s.%s: inconsistent catalog row", schemaName, table)
	}
	m.identity = schema.ReplicaIdentity(ident[0])
	m.cols = make([]column, len(names))
	for i := range names {
		m.cols[i] = column{name: names[i], typeOID: uint32(types[i])} //nolint:gosec // G115: oid fits uint32
	}
	return m, nil
}

// pageKey returns the keyset pagination columns: the primary key, else the
// replica identity index; nil means ctid ranges.
func (m *tableMeta) pageKey() []string {
	if len(m.pk) > 0 {
		return m.pk
	}
	if m.identity == schema.IdentityIndex && len(m.ri) > 0 {
		return m.ri
	}
	return nil
}

// eventKey returns the CDC event key columns of the table's events in the order
// the stream decoder uses (the key rule): the identity's columns in column
// order, the primary key in key order under FULL (all columns without
// one), nil for keyless tables.
func (m *tableMeta) eventKey() []string {
	switch m.identity {
	case schema.IdentityFull:
		if len(m.pk) > 0 {
			return m.pk
		}
		names := make([]string, len(m.cols))
		for i, c := range m.cols {
			names[i] = c.name
		}
		return names
	case schema.IdentityDefault:
		return m.inColumnOrder(m.pk)
	case schema.IdentityIndex:
		return m.inColumnOrder(m.ri)
	case schema.IdentityNothing:
	}
	return nil
}

// inColumnOrder returns the published columns among names in column order.
func (m *tableMeta) inColumnOrder(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, 0, len(names))
	for _, c := range m.cols {
		if slices.Contains(names, c.name) {
			out = append(out, c.name)
		}
	}
	return out
}

// sanitize quotes the table name for SQL.
func (m *tableMeta) sanitize() string { return pgx.Identifier{m.schema, m.name}.Sanitize() }
