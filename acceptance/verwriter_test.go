// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/internal/testutil/m2"
	"github.com/thnhdatle/wake/pkg/event"
)

// verTable is a table "<schema>.<name>" (id bigint PRIMARY KEY, ver bigint
// NOT NULL, payload text) written by newVerTx.
type verTable struct {
	Name string // "<schema>.<table>"
	// Keys is the key space; worker w owns the ids k with k % conns == w.
	Keys int64
	// PayloadBytes is the approximate payload size (a multiple of 32).
	PayloadBytes int
	// DeletePct of the transactions on an existing key delete it.
	DeletePct int
	// Seeded ids 1..Seeded exist with ver 1 before the load starts.
	Seeded int64
}

// verTableDDL creates the table of t (its schema must exist).
func verTableDDL(t verTable) string {
	return fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text);",
		identifier(t.Name))
}

// identifier quotes "<schema>.<table>".
func identifier(name string) string {
	return pgx.Identifier(strings.Split(name, ".")).Sanitize()
}

// verWriter is one worker's view of its keys: whether each exists and its
// last ver, so it records every change (M3) without reading.
type verWriter struct {
	exists map[int64]bool
	ver    map[int64]int64
}

// newVerTx returns a load body for conns workers: each transaction picks one
// of the worker's keys in tables[rng] and inserts it (absent), deletes it
// (DeletePct) or updates it with ver + 1. ver grows per key across deletes,
// so every recorded change is distinct.
func newVerTx(conns int, tables ...verTable) stack.TxFunc {
	writers := make([][]*verWriter, len(tables))
	for ti := range tables {
		writers[ti] = make([]*verWriter, conns)
		for w := range conns {
			writers[ti][w] = &verWriter{exists: map[int64]bool{}, ver: map[int64]int64{}}
		}
	}
	return func(ctx context.Context, conn *pgx.Conn, worker int, rng *rand.Rand) ([]stack.Change, error) {
		ti := rng.IntN(len(tables))
		tb, w := tables[ti], writers[ti][worker]
		key := int64(worker) + int64(conns)*rng.Int64N(max(tb.Keys/int64(conns), 1)) + 1
		if _, touched := w.ver[key]; !touched && key <= tb.Seeded {
			w.exists[key], w.ver[key] = true, 1
		}
		next := w.ver[key] + 1
		ch := stack.Change{Table: tb.Name, Key: stack.IDKey(key), Ver: next}
		ident := identifier(tb.Name)
		reps := max(tb.PayloadBytes/32, 1)
		var err error
		switch {
		case !w.exists[key]:
			ch.Op = stack.OpCreate
			_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, ver, payload)
				VALUES ($1, $2::bigint, repeat(md5($2::bigint::text), %d))`, ident, reps), key, next)
		case rng.IntN(100) < tb.DeletePct:
			ch.Op, ch.Ver = stack.OpDelete, w.ver[key]
			_, err = conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", ident), key)
		default:
			ch.Op = stack.OpUpdate
			_, err = conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET ver = $2::bigint,
				payload = repeat(md5($2::bigint::text), %d) WHERE id = $1`, ident, reps), key, next)
		}
		if err != nil {
			return nil, err //nolint:wrapcheck // reported in the load summary
		}
		w.exists[key] = ch.Op != stack.OpDelete
		if ch.Op != stack.OpDelete {
			w.ver[key] = next
		}
		return []stack.Change{ch}, nil
	}
}

// pgTableRows reads a table as the sink should hold it: to_jsonb of each row
// matches the type mapping of its columns (bigint and integer as numbers,
// text as strings); the key is the id column.
func pgTableRows(ctx context.Context, pg *stack.PG, name string) (m2.Table, error) {
	var rows []event.Row
	err := pg.With(ctx, func(c *pgx.Conn) error {
		q, err := c.Query(ctx, "SELECT to_jsonb(t)::text FROM "+identifier(name)+" t")
		if err != nil {
			return err //nolint:wrapcheck // wrapped below
		}
		var s string
		_, err = pgx.ForEachRow(q, []any{&s}, func() error {
			var r event.Row
			if uerr := json.Unmarshal([]byte(s), &r); uerr != nil {
				return uerr //nolint:wrapcheck // wrapped below
			}
			rows = append(rows, r)
			return nil
		})
		return err //nolint:wrapcheck // wrapped below
	})
	if err != nil {
		return nil, fmt.Errorf("select %s: %w", name, err)
	}
	return m2.FromRows([]string{"id"}, rows)
}
