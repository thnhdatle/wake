// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgsql_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func checkSnapshot(t *testing.T, c *pipeline.Chunk) {
	t.Helper()
	s := c.Snapshot
	require.NotZero(t, s.XMin)
	require.LessOrEqual(t, s.XMin, s.XMax)
	for i, x := range s.XIP {
		require.GreaterOrEqual(t, x, s.XMin)
		require.Less(t, x, s.XMax)
		if i > 0 {
			require.Greater(t, x, s.XIP[i-1])
		}
	}
	for i, x := range c.CommittedXIP {
		require.Contains(t, s.XIP, x)
		if i > 0 {
			require.Greater(t, x, c.CommittedXIP[i-1])
		}
	}
}

func TestChunkReader_ReadChunk_KeysetPaginatesUnderConcurrentWrites(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY, v int NOT NULL);
		INSERT INTO items SELECT g, 0 FROM generate_series(1, 1000) g`)
	reader := newReader(t, newPool(t, db.DSN))

	writeCtx, stopWriting := context.WithCancel(ctx)
	conn := db.Conn(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: deterministic workload
		for i := 0; writeCtx.Err() == nil; i++ {
			id := 1 + rng.IntN(1000)
			extra := 2000 + i
			if _, err := conn.Exec(writeCtx, `UPDATE items SET v = v + 1 WHERE id = $1`, id); err != nil {
				return
			}
			if _, err := conn.Exec(writeCtx, `INSERT INTO items VALUES ($1, 0)`, extra); err != nil {
				return
			}
			if _, err := conn.Exec(writeCtx, `DELETE FROM items WHERE id = $1`, extra-1); err != nil {
				return
			}
		}
	})
	ids := map[int]int{}
	last := 0
	var chunks []pipeline.Chunk
	req := pipeline.ChunkRequest{Schema: "public", Table: "items", Limit: 97}
	for {
		c, err := reader.ReadChunk(ctx, req)
		require.NoError(t, err)
		checkSnapshot(t, &c)
		chunks = append(chunks, c)
		for _, r := range c.Rows {
			var key struct{ ID int }
			require.NoError(t, json.Unmarshal([]byte(rowJSON(r.Key)), &key))
			require.Greater(t, key.ID, last, "keys increase within and across chunks")
			last = key.ID
			ids[key.ID]++
		}
		if c.Done {
			break
		}
		req.After = c.Last
	}
	stopWriting()
	wg.Wait()
	for id := 1; id <= 1000; id++ {
		require.Equal(t, 1, ids[id], "id %d read exactly once", id)
	}
	assert.Greater(t, len(chunks), 10)
	assert.JSONEq(t, `["97"]`, string(chunks[0].Last))
}

func TestChunkReader_ReadChunk_CommittedXIPReportsCommitInvisibleToChunk(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY); INSERT INTO items VALUES (1), (2)`)
	reader := newReader(t, newPool(t, db.DSN))

	// T holds an ACCESS EXCLUSIVE lock, so the chunk SELECT waits after
	// pg_current_snapshot() listed T in xip; T commits while it waits.
	writer := db.Conn(t)
	tx, err := writer.Begin(ctx)
	require.NoError(t, err)
	var xid string
	require.NoError(t, tx.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xid))
	_, err = tx.Exec(ctx, `LOCK TABLE items IN ACCESS EXCLUSIVE MODE; INSERT INTO items VALUES (3)`)
	require.NoError(t, err)
	// A later transaction completes, so that xmax passes T's xid and T is
	// listed in xip (a running xid at or above xmax is not listed).
	db.Exec(t, `SELECT pg_current_xact_id()`)

	type result struct {
		c   pipeline.Chunk
		err error
	}
	done := make(chan result, 1)
	go func() {
		c, rerr := reader.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "items", Limit: 10})
		done <- result{c, rerr}
	}()
	admin := db.Conn(t)
	require.Eventually(t, func() bool {
		var waiting int
		require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks
			WHERE relation = 'items'::regclass AND NOT granted`).Scan(&waiting))
		return waiting > 0
	}, waitTimeout, 10*time.Millisecond)
	require.NoError(t, tx.Commit(ctx))
	res := <-done
	require.NoError(t, res.err)
	checkSnapshot(t, &res.c)
	x, err := strconv.ParseUint(xid, 10, 64)
	require.NoError(t, err)
	assert.Contains(t, res.c.Snapshot.XIP, x)
	// Transactions of concurrently running tests (other databases share
	// the xid space) may be listed too.
	assert.Contains(t, res.c.CommittedXIP, x)
	require.Len(t, res.c.Rows, 2, "the committed insert is invisible to the chunk's snapshot")
	assert.True(t, res.c.Done)
}

func TestChunkReader_ReadChunk_CompositeKeysRoundTripTextPositions(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `
		CREATE TABLE comp (name text COLLATE "C", at timestamptz, b bytea, v int,
			PRIMARY KEY (name, at, b));
		INSERT INTO comp VALUES
			('a b', '2026-01-01 00:00:00.123456+00', '\x00', 1), ('a b', '2026-01-01 00:00:00.123456+00', '\x01', 2),
			('a"b', '2026-01-01 00:00:00+05', '\x00', 3), ('é☃', '-infinity', '\xff', 4),
			('it''s', 'infinity', '\x00ff', 5), ('z', '1999-12-31 23:59:59.999999+00', '', 6);
		CREATE TABLE ri (x int NOT NULL, y text NOT NULL, z text);
		CREATE UNIQUE INDEX ri_yx ON ri (y, x);
		ALTER TABLE ri REPLICA IDENTITY USING INDEX ri_yx;
		INSERT INTO ri SELECT g, 'k' || (g % 7), 'z' || g FROM generate_series(1, 50) g;`)
	reader := newReader(t, newPool(t, db.DSN))
	read := func(req pipeline.ChunkRequest) (pipeline.Chunk, error) { return reader.ReadChunk(ctx, req) }

	rows, chunks := readAll(t, read, "public", "comp", 2)
	vs := make([]int, 0, len(rows))
	for _, r := range rows {
		var v struct{ V int }
		require.NoError(t, json.Unmarshal([]byte(r), &v))
		vs = append(vs, v.V)
	}
	assert.Equal(t, []int{1, 2, 3, 5, 6, 4}, vs, "ordered by (name, at, b) in the C collation")
	assert.JSONEq(t, `{"name":"a b","at":"2026-01-01T00:00:00.123456+00:00","b":"AA=="}`,
		rowJSON(chunks[0].Rows[0].Key))

	rows, chunks = readAll(t, read, "public", "ri", 8)
	assert.Len(t, rows, 50)
	assert.Equal(t, []string{"x", "y"}, []string{chunks[0].Rows[0].Key[0].Name, chunks[0].Rows[0].Key[1].Name},
		"identity index columns in column order")
	assert.JSONEq(t, `{"x":7,"y":"k0"}`, rowJSON(chunks[0].Rows[0].Key))
	assert.JSONEq(t, `["k1","1"]`, string(chunks[0].Last), "paged by (y, x): seven k0 rows, then (k1, 1)")
}

func TestChunkReader_ReadChunk_CTIDRangesForTablesWithoutKey(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `
		CREATE TABLE full_nopk (a int, b text);
		ALTER TABLE full_nopk REPLICA IDENTITY FULL;
		INSERT INTO full_nopk SELECT g, repeat('x', g % 50) FROM generate_series(1, 3000) g;
		DELETE FROM full_nopk WHERE a % 3 = 0 OR a BETWEEN 1000 AND 2000;
		CREATE TABLE keyless (a int, b text);
		INSERT INTO keyless SELECT g, 'k' FROM generate_series(1, 10) g;
		CREATE TABLE empty_nopk (a int);
		CREATE TABLE parted (a int, b text) PARTITION BY LIST (b);
		CREATE TABLE parted_x PARTITION OF parted FOR VALUES IN ('x');
		CREATE TABLE parted_y PARTITION OF parted FOR VALUES IN ('y');
		CREATE TABLE parted_z PARTITION OF parted FOR VALUES IN ('z');
		INSERT INTO parted SELECT g, CASE WHEN g % 2 = 0 THEN 'x' ELSE 'z' END FROM generate_series(1, 501) g;`)
	db.Exec(t, `VACUUM ANALYZE full_nopk`)
	reader := newReader(t, newPool(t, db.DSN))
	read := func(req pipeline.ChunkRequest) (pipeline.Chunk, error) { return reader.ReadChunk(ctx, req) }

	rows, chunks := readAll(t, read, "public", "full_nopk", 100)
	var want []string
	for a := 1; a <= 3000; a++ {
		if a%3 != 0 && (a < 1000 || a > 2000) {
			b, _ := json.Marshal(stringsRepeat("x", a%50))
			want = append(want, fmt.Sprintf(`{"a":%d,"b":%s}`, a, b))
		}
	}
	assert.ElementsMatch(t, want, rows)
	assert.Equal(t, rowJSON(chunks[0].Rows[0].After), rowJSON(chunks[0].Rows[0].Key), "FULL without key: every column")
	var pos map[string]any
	require.NoError(t, json.Unmarshal(chunks[0].Last, &pos))
	assert.Contains(t, pos, "rel")
	assert.Contains(t, pos, "ctid")

	rows, chunks = readAll(t, read, "public", "keyless", 3)
	assert.Len(t, rows, 10)
	assert.Nil(t, chunks[0].Rows[0].Key, "keyless rows have no key")

	rows, chunks = readAll(t, read, "public", "empty_nopk", 3)
	assert.Empty(t, rows)
	require.Len(t, chunks, 1)
	assert.Nil(t, chunks[0].Last)

	rows, _ = readAll(t, read, "public", "parted", 64)
	assert.Len(t, rows, 501)
	assert.Len(t, uniq(rows), 501)

	_, err := reader.ReadChunk(ctx, pipeline.ChunkRequest{
		Schema: "public", Table: "keyless", Limit: 3,
		After: json.RawMessage(`["1"]`),
	})
	require.ErrorIs(t, err, pgsql.ErrBadPosition)
}

func stringsRepeat(s string, n int) string { return strings.Repeat(s, n) }

func uniq(xs []string) []string {
	s := slices.Clone(xs)
	slices.Sort(s)
	return slices.Compact(s)
}

func TestChunkReader_ReadChunk_RejectsBadRequests(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY)`)
	reader := newReader(t, newPool(t, db.DSN))
	_, err := reader.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "items"})
	require.ErrorIs(t, err, pgsql.ErrInvalidArgument)
	_, err = reader.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "nope", Limit: 1})
	require.ErrorIs(t, err, pgsql.ErrTableNotFound)
	_, err = reader.ReadChunk(ctx, pipeline.ChunkRequest{
		Schema: "public", Table: "items", Limit: 1,
		After: json.RawMessage(`{"rel":1,"ctid":"(0,1)"}`),
	})
	require.ErrorIs(t, err, pgsql.ErrBadPosition)
}

func TestChunkReader_OpenExported_ReadsTheExportedSnapshot(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY, v text);
		INSERT INTO items SELECT g, 'old' FROM generate_series(1, 25) g;
		CREATE TABLE nokey (a int); INSERT INTO nokey SELECT generate_series(1, 5);`)
	repl := db.Replication(t)
	name, _ := repl.CreateSlot(t, "wake_exported")
	db.Exec(t, `UPDATE items SET v = 'new'; INSERT INTO items VALUES (26, 'new'); DELETE FROM items WHERE id = 1;
		INSERT INTO nokey VALUES (6)`)

	pool := newPool(t, db.DSN, pgsql.WithMaxConns(1))
	reader := newReader(t, pool)
	sess, err := reader.OpenExported(ctx, name)
	require.NoError(t, err)
	read := func(req pipeline.ChunkRequest) (pipeline.Chunk, error) { return sess.ReadChunk(ctx, req) }
	rows, chunks := readAll(t, read, "public", "items", 10)
	require.Len(t, rows, 25)
	for i, r := range rows {
		assert.JSONEq(t, fmt.Sprintf(`{"id":%d,"v":"old"}`, i+1), r)
	}
	assert.Empty(t, chunks[0].Snapshot.XIP)
	assert.Zero(t, chunks[0].Snapshot.XMin)
	rows, _ = readAll(t, read, "public", "nokey", 2)
	assert.Len(t, rows, 5)

	// The session holds the pool's only connection.
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = reader.ReadChunk(short, pipeline.ChunkRequest{Schema: "public", Table: "items", Limit: 1})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, sess.Close(ctx))
	require.ErrorIs(t, sess.Close(ctx), pgsql.ErrSessionClosed)
	_, err = sess.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "items", Limit: 1})
	require.ErrorIs(t, err, pgsql.ErrSessionClosed)
	c, err := reader.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "items", Limit: 100})
	require.NoError(t, err)
	assert.Len(t, c.Rows, 25, "after Close the connection serves other reads")

	_, err = reader.OpenExported(ctx, "'; DROP TABLE items; --")
	require.ErrorIs(t, err, pgsql.ErrInvalidArgument)
	_, err = reader.OpenExported(ctx, "00000003-0000FFFF-1")
	require.Error(t, err)
	_, err = reader.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "items", Limit: 100})
	require.NoError(t, err, "a failed import releases its connection")
}

// typesDDL is the type matrix of scripts/record_pgoutput.sh.
const typesDDL = `
CREATE SCHEMA rec;
CREATE TYPE rec.mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE rec.types (
  id bigint PRIMARY KEY,
  c_int2 int2, c_int4 int4, c_int8 int8, c_oid oid,
  c_numeric numeric(20, 4), c_numeric_nan numeric, c_float4 float4, c_float8 float8,
  c_bool bool, c_text text, c_varchar varchar(20), c_bpchar char(5), c_name name, c_char "char",
  c_uuid uuid, c_bytea bytea, c_json json, c_jsonb jsonb,
  c_date date, c_time time, c_timetz timetz, c_timestamp timestamp, c_timestamptz timestamptz,
  c_interval interval, c_mood rec.mood,
  c_int4_arr int4[], c_text_arr text[], c_int8_arr2 int8[][],
  c_inet inet, c_cidr cidr, c_macaddr macaddr, c_money money,
  c_int4range int4range, c_numrange numrange, c_tstzrange tstzrange, c_daterange daterange,
  c_xml xml, c_point point
);
CREATE PUBLICATION pub FOR TABLE rec.types;`

const typesRows = `
INSERT INTO rec.types VALUES (
  1, -32768, 2147483647, 9223372036854775807, 4294967295,
  1234567890123456.1234, 'NaN', 'Infinity', -1.5e-300,
  true, 'he said "hi" <&> é ☃', 'varchar', 'ab', 'pg_name', 'x',
  'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\xdeadbeef00', '{"b": 1, "a": [1, 2]}', '{"b": 1, "a": [1, 2]}',
  '2026-10-18', '12:34:56.789', '12:34:56+05:30', '2026-10-18 12:34:56.123456', '2026-10-18 12:34:56.5+00',
  '1 year 2 mons -3 days 04:05:06.5', 'happy',
  '{1,NULL,3}', '{"a,b","c\"d","e\\f",NULL,"","NULL"}', '{{1,2},{3,4}}',
  '192.168.0.1/24', '10.0.0.0/8', '08:00:2b:01:02:03', '12.34',
  '[1,10)', '(,5.5]', '[2026-01-01 00:00:00+00,infinity)', 'empty',
  '<a>1</a>', '(1.5,2)'
);
INSERT INTO rec.types (id) VALUES (2);
INSERT INTO rec.types (id, c_timestamptz, c_date, c_interval, c_float8) VALUES
  (3, 'infinity', '-infinity', '-00:00:01', '-Infinity');`

// typesRow1 is the after image of row 1 in
// internal/app/stream/testdata/golden/reader/types.hex.default.golden.
const typesRow1 = `{"id":1,"c_int2":-32768,"c_int4":2147483647,"c_int8":9223372036854775807,"c_oid":4294967295,` +
	`"c_numeric":"1234567890123456.1234","c_numeric_nan":"NaN","c_float4":"Infinity","c_float8":-1.5e-300,` +
	`"c_bool":true,"c_text":"he said \"hi\" <&> é ☃","c_varchar":"varchar","c_bpchar":"ab   ","c_name":"pg_name",` +
	`"c_char":"x","c_uuid":"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","c_bytea":"3q2+7wA=","c_json":{"b":1,"a":[1,2]},` +
	`"c_jsonb":{"a":[1,2],"b":1},"c_date":"2026-10-18","c_time":"12:34:56.789","c_timetz":"12:34:56+05:30",` +
	`"c_timestamp":"2026-10-18T12:34:56.123456","c_timestamptz":"2026-10-18T12:34:56.5+00:00",` +
	`"c_interval":"P1Y2M-3DT4H5M6.5S","c_mood":"happy","c_int4_arr":[1,null,3],` +
	`"c_text_arr":["a,b","c\"d","e\\f",null,"","NULL"],"c_int8_arr2":[[1,2],[3,4]],"c_inet":"192.168.0.1/24",` +
	`"c_cidr":"10.0.0.0/8","c_macaddr":"08:00:2b:01:02:03","c_money":"$12.34",` +
	`"c_int4range":{"lower":1,"upper":10,"lower_inc":true,"upper_inc":false},` +
	`"c_numrange":{"lower":null,"upper":"5.5","lower_inc":false,"upper_inc":true},` +
	`"c_tstzrange":{"lower":"2026-01-01T00:00:00+00:00","upper":"infinity","lower_inc":true,"upper_inc":false},` +
	`"c_daterange":{"empty":true},"c_xml":"<a>1</a>","c_point":"(1.5,2)"}`

func TestChunkReader_ReadChunk_TypeMatrixMatchesStream(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, typesDDL)
	repl := db.Replication(t)
	_, cp := repl.CreateSlot(t, "wake_types")
	db.Exec(t, typesRows)
	repl.Start(t, "wake_types", cp, "pub")

	reader := newReader(t, newPool(t, db.DSN))
	read := func(req pipeline.ChunkRequest) (pipeline.Chunk, error) { return reader.ReadChunk(ctx, req) }
	rows, chunks := readAll(t, read, "rec", "types", 2)
	require.Len(t, rows, 3)
	assert.Equal(t, typesRow1, rows[0], "snapshot rows render like the recorded stream")
	assert.JSONEq(t, `{"id":1}`, rowJSON(chunks[0].Rows[0].Key))
	for i := range 3 {
		img, _ := repl.NextRow(t, waitTimeout)
		assert.Equal(t, rowJSON(img.After), rows[i], "row %d: chunk and stream render identically", i+1)
	}
}

func TestPool_ValidatesOptions(t *testing.T) {
	t.Parallel()
	_, err := pgsql.NewPool("postgres://u@localhost/db", pgsql.WithMaxConns(0))
	require.ErrorIs(t, err, pgsql.ErrInvalidArgument)
	_, err = pgsql.NewPool("::bad::")
	require.ErrorIs(t, err, pgsql.ErrInvalidArgument)

	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	p := newPool(t, db.DSN, pgsql.WithApplicationName("wake-test"), pgsql.WithMaxIdle(time.Minute),
		pgsql.WithConnectTimeout(10*time.Second))
	require.NoError(t, p.Ping(ctx))
	assert.Equal(t, "postgres", p.User())
	cat := newCatalog(t, p)
	_, err = cat.CurrentTime(ctx)
	require.NoError(t, err)
	conn := db.Conn(t)
	var settings string
	require.NoError(t, conn.QueryRow(ctx, `SELECT string_agg(application_name, ',') FROM pg_stat_activity
		WHERE application_name = 'wake-test'`).Scan(&settings))
	assert.Equal(t, "wake-test", settings)
	p.Close(ctx)
	require.ErrorIs(t, p.Ping(ctx), pgsql.ErrPoolClosed)
}
