// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	tidOID             = 27
	defaultRowsPerPage = 50
	maxRangePages      = 1 << 20
)

type leaf struct {
	oid       uint32
	schema    string
	name      string
	blocks    int64
	reltuples float64
	relpages  int64
}

const leafColumnsSQL = `SELECT c.oid, n.nspname::text, c.relname::text,
	pg_catalog.pg_relation_size(c.oid) / pg_catalog.current_setting('block_size')::int8,
	c.reltuples::float8, c.relpages::int8
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace `

const (
	tableLeafSQL      = leafColumnsSQL + `WHERE c.oid = $1::oid`
	partitionLeafsSQL = leafColumnsSQL + `WHERE c.relkind = 'r' AND c.oid IN
	(SELECT relid FROM pg_catalog.pg_partition_tree($1::oid::regclass) WHERE isleaf) ORDER BY c.oid`
)

func loadLeaves(ctx context.Context, tx pgx.Tx, m *tableMeta) ([]leaf, error) {
	q := tableLeafSQL
	if m.partitioned {
		q = partitionLeafsSQL
	}
	rows, err := tx.Query(ctx, q, m.oid)
	if err != nil {
		return nil, fmt.Errorf("pgsql: list heaps of %s.%s: %w", m.schema, m.name, err)
	}
	defer rows.Close()
	var out []leaf
	for rows.Next() {
		var l leaf
		if err := rows.Scan(&l.oid, &l.schema, &l.name, &l.blocks, &l.reltuples, &l.relpages); err != nil {
			return nil, fmt.Errorf("pgsql: scan heap of %s.%s: %w", m.schema, m.name, err)
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgsql: list heaps of %s.%s: %w", m.schema, m.name, err)
	}
	return out, nil
}

func rangePages(l *leaf, want int) uint32 {
	perPage := float64(defaultRowsPerPage)
	if l.relpages > 0 && l.reltuples > 0 {
		perPage = max(l.reltuples/float64(l.relpages), 1)
	}
	pages := math.Ceil(float64(want)/perPage) + 1
	return uint32(min(pages, maxRangePages))
}

func ctidSQL(m *tableMeta, l *leaf) string {
	var b strings.Builder
	b.WriteString("SELECT ctid, ")
	writeColumns(&b, m.cols)
	b.WriteString(" FROM ONLY ")
	b.WriteString(pgx.Identifier{l.schema, l.name}.Sanitize())
	b.WriteString(" WHERE ctid > $1 AND ctid < $2 ORDER BY ctid LIMIT $3")
	return b.String()
}

type ctidReader struct {
	tx      pgx.Tx
	meta    *tableMeta
	rend    *renderer
	limit   int
	chunk   pipeline.Chunk
	read    int
	lastRel uint32
	lastTID tid
}

func readCTID(ctx context.Context, tx pgx.Tx, m *tableMeta, req *pipeline.ChunkRequest) (pipeline.Chunk, error) {
	var startRel uint32
	var from tid
	if req.After != nil {
		var err error
		if startRel, from, err = decodeCTIDPosition(req.After); err != nil {
			return pipeline.Chunk{}, err
		}
	}
	leaves, err := loadLeaves(ctx, tx, m)
	if err != nil {
		return pipeline.Chunk{}, err
	}
	rend, err := newRenderer(m, 1)
	if err != nil {
		return pipeline.Chunk{}, err
	}
	cr := &ctidReader{tx: tx, meta: m, rend: rend, limit: req.Limit, chunk: pipeline.Chunk{Last: req.After}}
	for i := range leaves {
		l := &leaves[i]
		if l.oid < startRel {
			continue
		}
		cur := tid{}
		if l.oid == startRel {
			cur = from
		}
		full, lerr := cr.readLeaf(ctx, l, cur)
		if lerr != nil {
			return pipeline.Chunk{}, lerr
		}
		if full {
			break
		}
	}
	cr.chunk.Done = cr.read <= cr.limit
	if len(cr.chunk.Rows) > 0 {
		if cr.chunk.Last, err = encodeCTIDPosition(cr.lastRel, cr.lastTID); err != nil {
			return pipeline.Chunk{}, err
		}
	}
	return cr.chunk, nil
}

func (cr *ctidReader) readLeaf(ctx context.Context, l *leaf, cur tid) (bool, error) {
	sql := ctidSQL(cr.meta, l)
	for int64(cur.page) < l.blocks {
		want := cr.limit + 1 - cr.read
		end := uint64(cur.page) + uint64(rangePages(l, want))
		end = min(end, math.MaxUint32)
		upper := tid{page: uint32(end)}
		params := [][]byte{[]byte(cur.String()), []byte(upper.String()), []byte(strconv.Itoa(want))}
		rr := cr.tx.Conn().PgConn().ExecParams(ctx, sql, params, []uint32{tidOID, tidOID, int8OID}, nil, nil)
		for rr.NextRow() {
			if err := cr.add(l.oid, rr.Values()); err != nil {
				_, _ = rr.Close() //nolint:errcheck // reporting the row error
				return false, err
			}
		}
		if _, err := rr.Close(); err != nil {
			return false, fmt.Errorf("pgsql: read chunk of %s.%s: %w", cr.meta.schema, cr.meta.name, err)
		}
		if cr.read > cr.limit {
			return true, nil
		}
		if end >= math.MaxUint32 {
			break
		}
		cur = upper
	}
	return false, nil
}

func (cr *ctidReader) add(rel uint32, values [][]byte) error {
	if cr.read++; cr.read > cr.limit {
		return nil
	}
	if len(values) == 0 || values[0] == nil {
		return fmt.Errorf("pgsql: %s.%s: row without ctid", cr.meta.schema, cr.meta.name)
	}
	t, err := parseTID(string(values[0]))
	if err != nil {
		return fmt.Errorf("pgsql: %s.%s: %w", cr.meta.schema, cr.meta.name, err)
	}
	row, err := cr.rend.row(values)
	if err != nil {
		return err
	}
	cr.chunk.Rows = append(cr.chunk.Rows, row)
	cr.lastRel, cr.lastTID = rel, t
	return nil
}
