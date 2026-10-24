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

const hasKeySQL = `CASE c.relreplident
	WHEN 'f' THEN true
	WHEN 'd' THEN EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
	WHEN 'i' THEN EXISTS (SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid AND i.indisreplident)
	ELSE false END`

const (
	indexColumnsPrefix = `ARRAY(SELECT a.attname::text FROM pg_catalog.pg_index i
	CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
	JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	WHERE i.indrelid = c.oid AND `
	indexColumnsSuffix   = ` AND k.ord <= i.indnkeyatts ORDER BY k.ord)`
	primaryKeyColumnsSQL = indexColumnsPrefix + "i.indisprimary" + indexColumnsSuffix
	identityColumnsSQL   = indexColumnsPrefix + "i.indisreplident" + indexColumnsSuffix
)

type Table struct {
	Schema      string
	Name        string
	OID         uint32
	Identity    schema.ReplicaIdentity
	HasKey      bool
	Partitioned bool
	Partition   bool
	Owned       bool
}

func (t *Table) QualifiedName() string { return t.Schema + "." + t.Name }

func (t *Table) Info() pipeline.TableInfo {
	return pipeline.TableInfo{Schema: t.Schema, Table: t.Name, OID: t.OID, HasKey: t.HasKey}
}

func (t *Table) sanitize() string { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }

const tableColumnsSQL = `c.oid, n.nspname::text, c.relname::text, c.relreplident::text, ` + hasKeySQL + `,
	c.relkind = 'p', c.relispartition, pg_catalog.pg_has_role(c.relowner, 'USAGE')`

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

type column struct {
	name    string
	typeOID uint32
}

type tableMeta struct {
	oid         uint32
	schema      string
	name        string
	identity    schema.ReplicaIdentity
	partitioned bool
	cols        []column
	pk          []string
	ri          []string
}

const tableMetaSQL = `SELECT c.oid, c.relreplident::text, c.relkind = 'p',
	ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0
		AND NOT a.attisdropped AND a.attgenerated = '' ORDER BY a.attnum),
	ARRAY(SELECT a.atttypid::int8 FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0
		AND NOT a.attisdropped AND a.attgenerated = '' ORDER BY a.attnum),
	` + primaryKeyColumnsSQL + `,
	` + identityColumnsSQL + `
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')`

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

func (m *tableMeta) pageKey() []string {
	if len(m.pk) > 0 {
		return m.pk
	}
	if m.identity == schema.IdentityIndex && len(m.ri) > 0 {
		return m.ri
	}
	return nil
}

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

func (m *tableMeta) sanitize() string { return pgx.Identifier{m.schema, m.name}.Sanitize() }
