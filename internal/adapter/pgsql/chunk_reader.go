// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/watermark"
)

// int8OID is the type OID of bigint parameters.
const int8OID = 20

// snapshotNameRE matches the names pg_export_snapshot and
// CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT produce ("%08X-%08X-%d"); it
// makes the name safe to embed in SET TRANSACTION SNAPSHOT, which takes no
// parameters.
var snapshotNameRE = regexp.MustCompile(`^[0-9A-F]{1,16}-[0-9A-F]{1,16}(-[0-9]{1,10})?$`)

// ChunkReader reads table chunks. It implements
// pipeline.ChunkReader and is safe for concurrent use; each ReadChunk and
// each session uses its own pooled connection.
type ChunkReader struct {
	pool *Pool
	o    options
}

// NewChunkReader validates the dependencies.
func NewChunkReader(pool *Pool, opts ...Option) (*ChunkReader, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrInvalidArgument)
	}
	o, err := newOptions(opts)
	if err != nil {
		return nil, err
	}
	return &ChunkReader{pool: pool, o: o}, nil
}

func validateRequest(req *pipeline.ChunkRequest) error {
	if req.Schema == "" || req.Table == "" {
		return fmt.Errorf("%w: chunk request without table", ErrInvalidArgument)
	}
	if req.Limit < 1 || req.Limit > MaxChunkRows {
		return fmt.Errorf("%w: chunk limit %d outside 1..%d", ErrInvalidArgument, req.Limit, MaxChunkRows)
	}
	return nil
}

// ReadChunk implements pipeline.ChunkReader: in one READ ONLY
// REPEATABLE READ transaction it reads pg_current_snapshot(), then the
// chunk, then pg_xact_status of every xid in the snapshot's xip list.
func (r *ChunkReader) ReadChunk(ctx context.Context, req pipeline.ChunkRequest) (pipeline.Chunk, error) {
	if err := validateRequest(&req); err != nil {
		return pipeline.Chunk{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, r.o.chunkTimeout)
	defer cancel()
	var chunk pipeline.Chunk
	err := r.pool.withConn(ctx, func(conn *pgx.Conn) error {
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return fmt.Errorf("pgsql: begin chunk transaction: %w", err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit
		var snapText string
		if err = tx.QueryRow(ctx, `SELECT pg_catalog.pg_current_snapshot()::text`).Scan(&snapText); err != nil {
			return fmt.Errorf("pgsql: read snapshot: %w", err)
		}
		snap, err := watermark.ParseSnapshot(snapText)
		if err != nil {
			return fmt.Errorf("pgsql: parse snapshot: %w", err)
		}
		if chunk, err = readChunk(ctx, tx, &req); err != nil {
			return err
		}
		if chunk.CommittedXIP, err = committedXIP(ctx, tx, snap.XIP); err != nil {
			return err
		}
		chunk.Snapshot = pipeline.TxSnapshot{XMin: snap.Xmin, XMax: snap.Xmax, XIP: snap.XIP}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("pgsql: end chunk transaction: %w", err)
		}
		return nil
	})
	if err != nil {
		return pipeline.Chunk{}, err
	}
	return chunk, nil
}

const committedXIPSQL = `SELECT x FROM unnest($1::text[]) AS t(x)
WHERE pg_catalog.pg_xact_status(x::xid8) = 'committed'`

// committedXIP returns the xids of xip that pg_xact_status reports
// committed, strictly increasing.
func committedXIP(ctx context.Context, tx pgx.Tx, xip []uint64) ([]uint64, error) {
	if len(xip) == 0 {
		return nil, nil
	}
	texts := make([]string, len(xip))
	for i, x := range xip {
		texts[i] = strconv.FormatUint(x, 10)
	}
	rows, err := tx.Query(ctx, committedXIPSQL, texts)
	if err != nil {
		return nil, fmt.Errorf("pgsql: read xact status: %w", err)
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return nil, fmt.Errorf("pgsql: scan xact status: %w", err)
		}
		x, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("pgsql: xid %q: %w", text, err)
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgsql: read xact status: %w", err)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// readChunk reads one chunk in the caller's transaction: by keyset
// pagination on the primary key (else the replica identity index), or by
// ctid ranges for tables without one.
func readChunk(ctx context.Context, tx pgx.Tx, req *pipeline.ChunkRequest) (pipeline.Chunk, error) {
	meta, err := loadTableMeta(ctx, tx, req.Schema, req.Table)
	if err != nil {
		return pipeline.Chunk{}, err
	}
	if key := meta.pageKey(); key != nil {
		return readKeyset(ctx, tx, meta, key, req)
	}
	return readCTID(ctx, tx, meta, req)
}

// keysetSQL builds
//
//	SELECT <cols> FROM [ONLY] <table> [WHERE (<key>) > ($1, …)] ORDER BY <key> LIMIT $n
//
// ONLY excludes inheritance children of plain tables; a partitioned table is
// read through its root.
func keysetSQL(m *tableMeta, key []string, after bool) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	writeColumns(&b, m.cols)
	b.WriteString(" FROM ")
	if !m.partitioned {
		b.WriteString("ONLY ")
	}
	b.WriteString(m.sanitize())
	keyList := quoteNames(key)
	n := 1
	if after {
		b.WriteString(" WHERE (" + keyList + ") > (")
		for i := range key {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("$" + strconv.Itoa(n))
			n++
		}
		b.WriteString(")")
	}
	b.WriteString(" ORDER BY " + keyList + " LIMIT $" + strconv.Itoa(n))
	return b.String()
}

func writeColumns(b *strings.Builder, cols []column) {
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgx.Identifier{c.name}.Sanitize())
	}
}

func quoteNames(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = pgx.Identifier{n}.Sanitize()
	}
	return strings.Join(q, ", ")
}

// readKeyset reads up to req.Limit rows after the position by key; it reads
// one extra row to tell whether the table has more.
func readKeyset(ctx context.Context, tx pgx.Tx, m *tableMeta, key []string, req *pipeline.ChunkRequest,
) (pipeline.Chunk, error) {
	keyPos := make([]int, len(key))
	var params [][]byte
	var oids []uint32
	for i, name := range key {
		if keyPos[i] = columnIndex(m.cols, name); keyPos[i] < 0 {
			return pipeline.Chunk{}, fmt.Errorf("%w: key column %q of %s.%s is not published (generated?)",
				ErrInvalidArgument, name, m.schema, m.name)
		}
	}
	if req.After != nil {
		values, err := decodeKeyPosition(req.After, len(key))
		if err != nil {
			return pipeline.Chunk{}, err
		}
		for i, v := range values {
			params = append(params, []byte(v))
			oids = append(oids, m.cols[keyPos[i]].typeOID)
		}
	}
	params = append(params, []byte(strconv.Itoa(req.Limit+1)))
	oids = append(oids, int8OID)
	rend, err := newRenderer(m, 0)
	if err != nil {
		return pipeline.Chunk{}, err
	}
	chunk := pipeline.Chunk{Last: req.After}
	var last []string
	read := 0
	rr := tx.Conn().PgConn().ExecParams(ctx, keysetSQL(m, key, req.After != nil), params, oids, nil, nil)
	for rr.NextRow() {
		if read++; read > req.Limit {
			continue // the extra row: more rows follow
		}
		values := rr.Values()
		row, rerr := rend.row(values)
		if rerr != nil {
			_, _ = rr.Close() //nolint:errcheck // reporting the render error
			return pipeline.Chunk{}, rerr
		}
		chunk.Rows = append(chunk.Rows, row)
		last = last[:0]
		for _, p := range keyPos {
			last = append(last, string(values[p]))
		}
	}
	if _, err = rr.Close(); err != nil {
		return pipeline.Chunk{}, fmt.Errorf("pgsql: read chunk of %s.%s: %w", m.schema, m.name, err)
	}
	chunk.Done = read <= req.Limit
	if len(chunk.Rows) > 0 {
		if chunk.Last, err = encodeKeyPosition(last); err != nil {
			return pipeline.Chunk{}, err
		}
	}
	return chunk, nil
}

// OpenExported implements pipeline.ChunkReader: it opens a READ ONLY
// REPEATABLE READ transaction on its own connection and imports the
// exported snapshot with SET TRANSACTION SNAPSHOT. The session holds the
// connection until Close.
func (r *ChunkReader) OpenExported(ctx context.Context, snapshotName string) (pipeline.ChunkSession, error) {
	if !snapshotNameRE.MatchString(snapshotName) {
		return nil, fmt.Errorf("%w: snapshot name %q", ErrInvalidArgument, snapshotName)
	}
	ctx, cancel := context.WithTimeout(ctx, r.o.queryTimeout)
	defer cancel()
	conn, err := r.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		r.pool.release(ctx, conn)
		return nil, fmt.Errorf("pgsql: begin snapshot transaction: %w", err)
	}
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+snapshotName+"'"); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // reporting the import error
		r.pool.release(ctx, conn)
		return nil, fmt.Errorf("pgsql: import snapshot %s: %w", snapshotName, err)
	}
	return &Session{r: r, conn: conn, tx: tx}, nil
}

// Compile-time check of the port ChunkReader implements.
var _ pipeline.ChunkReader = (*ChunkReader)(nil)
