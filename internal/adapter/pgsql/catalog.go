// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Catalog reads server, slot and table metadata.
type Catalog struct {
	pool *Pool
	o    options
}

// NewCatalog returns a Catalog that uses pool.
func NewCatalog(pool *Pool, opts ...Option) (*Catalog, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrInvalidArgument)
	}
	o, err := newOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Catalog{pool: pool, o: o}, nil
}

func (c *Catalog) query(ctx context.Context, fn func(context.Context, *pgx.Conn) error) error {
	ctx, cancel := context.WithTimeout(ctx, c.o.queryTimeout)
	defer cancel()
	return c.pool.withConn(ctx, func(conn *pgx.Conn) error { return fn(ctx, conn) })
}

const settingsSQL = `SELECT name, setting, COALESCE(unit, '') FROM pg_catalog.pg_settings WHERE name = ANY($1)`

func (c *Catalog) ServerSettings(ctx context.Context) (pipeline.ServerSettings, error) {
	rowsByName := map[string]setting{}
	err := c.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, settingsSQL, settingNames)
		if err != nil {
			return fmt.Errorf("pgsql: read settings: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			var s setting
			if err := rows.Scan(&name, &s.value, &s.unit); err != nil {
				return fmt.Errorf("pgsql: scan setting: %w", err)
			}
			rowsByName[name] = s
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("pgsql: read settings: %w", err)
		}
		return nil
	})
	if err != nil {
		return pipeline.ServerSettings{}, err
	}
	return parseSettings(rowsByName)
}

const slotSQL = `SELECT COALESCE(plugin, '')::text, confirmed_flush_lsn::text, COALESCE(wal_status, ''),
	COALESCE(active_pid, 0),
	COALESCE((CASE WHEN pg_catalog.pg_is_in_recovery() THEN pg_catalog.pg_last_wal_replay_lsn()
		ELSE pg_catalog.pg_current_wal_lsn() END - confirmed_flush_lsn)::int8, 0)
FROM pg_catalog.pg_replication_slots WHERE slot_name = $1`

func (c *Catalog) Slot(ctx context.Context, name string) (pipeline.SlotStatus, error) {
	var st pipeline.SlotStatus
	err := c.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		var confirmed *string
		err := conn.QueryRow(ctx, slotSQL, name).Scan(&st.Plugin, &confirmed, &st.WALStatus, &st.ActivePID,
			&st.LagBytes)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pgsql: read slot %s: %w", name, err)
		}
		st.Exists = true
		if confirmed != nil {
			if st.ConfirmedFlush, err = lsn.Parse(*confirmed); err != nil {
				return fmt.Errorf("pgsql: slot %s confirmed_flush_lsn: %w", name, err)
			}
		}
		return nil
	})
	if err != nil {
		return pipeline.SlotStatus{}, err
	}
	return st, nil
}

const publicationTablesSQL = `SELECT ` + tableColumnsSQL + `
FROM (SELECT DISTINCT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = ANY($1)) pt
JOIN pg_catalog.pg_namespace n ON n.nspname = pt.schemaname
JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
ORDER BY n.nspname, c.relname`

func (c *Catalog) PublicationTables(ctx context.Context, publications []string) ([]pipeline.TableInfo, error) {
	tables, err := c.PublicationTableDetails(ctx, publications)
	if err != nil {
		return nil, err
	}
	out := make([]pipeline.TableInfo, len(tables))
	for i := range tables {
		out[i] = tables[i].Info()
	}
	return out, nil
}

func (c *Catalog) PublicationTableDetails(ctx context.Context, publications []string) ([]Table, error) {
	var tables []Table
	err := c.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, publicationTablesSQL, publications)
		if err != nil {
			return fmt.Errorf("pgsql: list publication tables: %w", err)
		}
		tables, err = scanTables(rows)
		return err
	})
	return tables, err
}

func (c *Catalog) CurrentTime(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := c.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		if err := conn.QueryRow(ctx, `SELECT pg_catalog.clock_timestamp()`).Scan(&t); err != nil {
			return fmt.Errorf("pgsql: read clock_timestamp: %w", err)
		}
		return nil
	})
	return t, err
}

const keyColumnsSQL = `SELECT ` + primaryKeyColumnsSQL + `
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2`

func (c *Catalog) KeyColumns(ctx context.Context, schemaName, table string) ([]string, error) {
	var cols []string
	err := c.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		err := conn.QueryRow(ctx, keyColumnsSQL, schemaName, table).Scan(&cols)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pgsql: read primary key of %s.%s: %w", schemaName, table, err)
		}
		return nil
	})
	return cols, err
}

type Usage struct {
	Slots   int
	Senders int
}

func (c *Catalog) Usage(ctx context.Context) (Usage, error) {
	var u Usage
	err := c.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM pg_catalog.pg_replication_slots)::int,
			(SELECT count(*) FROM pg_catalog.pg_stat_replication)::int`).Scan(&u.Slots, &u.Senders)
		if err != nil {
			return fmt.Errorf("pgsql: read replication usage: %w", err)
		}
		return nil
	})
	return u, err
}

var _ pipeline.Catalog = (*Catalog)(nil)
