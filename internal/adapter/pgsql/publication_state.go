// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const candidatesSQL = `SELECT ` + tableColumnsSQL + `
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition AND c.relpersistence = 'p'
	AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\_%'
	AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
		WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')
ORDER BY n.nspname, c.relname`

func candidateTables(ctx context.Context, q querier) ([]Table, error) {
	rows, err := q.Query(ctx, candidatesSQL)
	if err != nil {
		return nil, fmt.Errorf("pgsql: list tables: %w", err)
	}
	return scanTables(rows)
}

type pubState struct {
	name                             string
	exists, owned, allTables         bool
	insert, update, delete, truncate bool
	viaRoot                          bool
	members                          []Table
}

func (ps *pubState) hasOptions(publish string) bool {
	if publish == publishInsert {
		return ps.insert && !ps.update && !ps.delete && !ps.truncate && ps.viaRoot
	}
	return ps.insert && ps.update && ps.delete && ps.truncate && ps.viaRoot
}

const pubStateSQL = `SELECT p.oid, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate,
	p.pubviaroot, pg_catalog.pg_has_role(p.pubowner, 'USAGE')
FROM pg_catalog.pg_publication p WHERE p.pubname = $1`

const pubMembersSQL = `SELECT ` + tableColumnsSQL + `
FROM pg_catalog.pg_publication_rel pr
JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE pr.prpubid = $1 ORDER BY n.nspname, c.relname`

func loadPubState(ctx context.Context, q querier, name string) (pubState, error) {
	ps := pubState{name: name}
	var oid uint32
	err := q.QueryRow(ctx, pubStateSQL, name).Scan(&oid, &ps.allTables, &ps.insert, &ps.update, &ps.delete,
		&ps.truncate, &ps.viaRoot, &ps.owned)
	if errors.Is(err, pgx.ErrNoRows) {
		return ps, nil
	}
	if err != nil {
		return pubState{}, fmt.Errorf("pgsql: read publication %s: %w", name, err)
	}
	ps.exists = true
	rows, err := q.Query(ctx, pubMembersSQL, oid)
	if err != nil {
		return pubState{}, fmt.Errorf("pgsql: list tables of publication %s: %w", name, err)
	}
	if ps.members, err = scanTables(rows); err != nil {
		return pubState{}, err
	}
	return ps, nil
}

type ExistingPublication struct {
	Name                             string
	Exists                           bool
	Owned                            bool
	AllTables                        bool
	Insert, Update, Delete, Truncate bool
	ViaRoot                          bool
	Tables                           []Table
}

func (e *ExistingPublication) PartitionLeaves() []Table {
	if e.ViaRoot {
		return nil
	}
	var out []Table
	for _, t := range e.Tables {
		if t.Partition {
			out = append(out, t)
		}
	}
	return out
}

func (e *ExistingPublication) Keyless() []Table {
	var out []Table
	for _, t := range e.Tables {
		if !t.HasKey {
			out = append(out, t)
		}
	}
	return out
}

func (p *Publications) InspectExisting(ctx context.Context, name string) (ExistingPublication, error) {
	ctx, cancel := context.WithTimeout(ctx, p.o.queryTimeout)
	defer cancel()
	var out ExistingPublication
	err := p.pool.withConn(ctx, func(conn *pgx.Conn) error {
		var err error
		out, err = inspectExisting(ctx, conn, name)
		return err
	})
	return out, err
}

func inspectExisting(ctx context.Context, q querier, name string) (ExistingPublication, error) {
	ps, err := loadPubState(ctx, q, name)
	if err != nil {
		return ExistingPublication{}, err
	}
	out := ExistingPublication{
		Name: name, Exists: ps.exists, Owned: ps.owned, AllTables: ps.allTables, Insert: ps.insert,
		Update: ps.update, Delete: ps.delete, Truncate: ps.truncate, ViaRoot: ps.viaRoot,
	}
	if !ps.exists {
		return out, nil
	}
	rows, err := q.Query(ctx, publicationTablesSQL, []string{name})
	if err != nil {
		return ExistingPublication{}, fmt.Errorf("pgsql: list tables of publication %s: %w", name, err)
	}
	if out.Tables, err = scanTables(rows); err != nil {
		return ExistingPublication{}, err
	}
	return out, nil
}
