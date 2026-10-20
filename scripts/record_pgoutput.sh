#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Records raw pgoutput messages from the compose Postgres into testdata/pgoutput/.
# Each scenario runs against a fresh slot; the messages are read with
# pg_logical_slot_peek_binary_changes (one row per protocol message) and written
# as "<lsn> <xid> <hex>" lines in WAL order to testdata/pgoutput/<scenario>.hex.
# Usage: scripts/record_pgoutput.sh   (starts and stops only the postgres service)
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
. scripts/lib.sh

need docker
COMPOSE=(docker compose -f deploy/docker-compose.yml)
OUT=testdata/pgoutput
SLOT=wake_record
PUB=wake_record

"${COMPOSE[@]}" up -d --wait postgres
trap '"${COMPOSE[@]}" down -v' EXIT

psql_app() {
	"${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -X -q -U postgres -d app "$@"
}

psql_app <<'SQL'
DROP PUBLICATION IF EXISTS wake_record;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'wake_record') THEN
    PERFORM pg_drop_replication_slot('wake_record');
  END IF;
END $$;
DROP SCHEMA IF EXISTS rec CASCADE;
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
CREATE TABLE rec.docs (id int PRIMARY KEY, title text, body text);
ALTER TABLE rec.docs ALTER COLUMN body SET STORAGE EXTERNAL;
CREATE TABLE rec.docs_full (id int PRIMARY KEY, title text, body text);
ALTER TABLE rec.docs_full ALTER COLUMN body SET STORAGE EXTERNAL;
ALTER TABLE rec.docs_full REPLICA IDENTITY FULL;
CREATE TABLE rec.nopk_full (a int, b text);
ALTER TABLE rec.nopk_full REPLICA IDENTITY FULL;
CREATE TABLE rec.uniq_idx (a int NOT NULL, b int NOT NULL, c text);
CREATE UNIQUE INDEX uniq_idx_ab ON rec.uniq_idx (a, b);
ALTER TABLE rec.uniq_idx REPLICA IDENTITY USING INDEX uniq_idx_ab;
CREATE TABLE rec.keyless (a int, b text);
CREATE TABLE rec.evolve (id int PRIMARY KEY, a int);
CREATE PUBLICATION wake_record FOR TABLE rec.types, rec.docs, rec.docs_full, rec.nopk_full, rec.uniq_idx, rec.evolve;
CREATE PUBLICATION wake_record_insert FOR TABLE rec.keyless WITH (publish = 'insert');
SQL

# scenario <name> < sql: fresh slot, run the SQL read from stdin, dump the messages.
scenario() {
	local name=$1
	psql_app -c "SELECT pg_create_logical_replication_slot('$SLOT', 'pgoutput')" </dev/null >/dev/null
	psql_app
	psql_app -A -t -F ' ' -c "SELECT lsn, xid, encode(data, 'hex') FROM pg_logical_slot_peek_binary_changes(
		'$SLOT', NULL, NULL, 'proto_version', '1', 'publication_names', '$PUB,wake_record_insert',
		'messages', 'true')" </dev/null >"$OUT/$name.hex"
	psql_app -c "SELECT pg_drop_replication_slot('$SLOT')" </dev/null >/dev/null
	echo "record_pgoutput: $OUT/$name.hex ($(wc -l <"$OUT/$name.hex" | tr -d ' ') messages)"
}

mkdir -p "$OUT"

scenario types <<'SQL'
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
  (3, 'infinity', '-infinity', '-00:00:01', '-Infinity');
SQL

scenario toast <<'SQL'
INSERT INTO rec.docs VALUES (1, 'a', repeat('x', 10000));
UPDATE rec.docs SET title = 'b' WHERE id = 1;
INSERT INTO rec.docs_full VALUES (1, 'a', repeat('y', 10000));
UPDATE rec.docs_full SET title = 'b' WHERE id = 1;
SQL

scenario identity <<'SQL'
INSERT INTO rec.docs VALUES (2, 't', 'small');
UPDATE rec.docs SET id = 3 WHERE id = 2;
DELETE FROM rec.docs WHERE id = 3;
INSERT INTO rec.nopk_full VALUES (1, 'one');
UPDATE rec.nopk_full SET b = 'uno' WHERE a = 1;
DELETE FROM rec.nopk_full WHERE a = 1;
INSERT INTO rec.uniq_idx VALUES (1, 2, 'c');
UPDATE rec.uniq_idx SET c = 'd' WHERE a = 1;
UPDATE rec.uniq_idx SET b = 3 WHERE a = 1;
DELETE FROM rec.uniq_idx WHERE a = 1;
INSERT INTO rec.keyless VALUES (1, 'k');
SQL

scenario multi <<'SQL'
BEGIN;
INSERT INTO rec.evolve VALUES (1, 1);
INSERT INTO rec.evolve VALUES (2, 2);
UPDATE rec.evolve SET a = 3 WHERE id = 1;
SELECT pg_logical_emit_message(true, 'app.audit', 'in-tx');
COMMIT;
SELECT pg_logical_emit_message(false, 'app.audit', 'outside');
SELECT pg_logical_emit_message(true, 'wake.heartbeat', '{}');
TRUNCATE rec.evolve;
SQL

scenario schema <<'SQL'
INSERT INTO rec.evolve VALUES (10, 1);
ALTER TABLE rec.evolve ADD COLUMN b text DEFAULT 'd';
INSERT INTO rec.evolve VALUES (11, 2);
ALTER TABLE rec.evolve DROP COLUMN a;
INSERT INTO rec.evolve VALUES (12);
ALTER TABLE rec.evolve ALTER COLUMN b TYPE varchar(10);
UPDATE rec.evolve SET b = 'x' WHERE id = 12;
ALTER TABLE rec.evolve RENAME COLUMN b TO c;
DELETE FROM rec.evolve WHERE id = 12;
SQL
