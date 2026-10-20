# Getting started

This guide takes you from nothing to a Wake instance streaming a real Postgres
database to Kafka. It covers installation, what Postgres needs, a role with the
fewest privileges that work, the first run and how to check that everything is
healthy. To try Wake in five minutes on your laptop, use
[quickstart.md](quickstart.md) first.

## What you need

- Postgres 14, 15, 16 or 17 with `wal_level=logical`. Wake's continuous
  integration runs against the latest minor release of each of these majors;
  [operations.md](operations.md) states the version policy.
- A destination: Kafka, NATS JetStream, an HTTPS webhook receiver, or stdout for
  a first look ([sinks.md](sinks.md)).
- A place to keep two small things across restarts: the state file and the spill
  directory (see [Persistent storage](#persistent-storage)).
- Exactly one running Wake process per source. Postgres allows one consumer per
  replication slot; a second instance fails with `slot_in_use`.

## Install

Pick one.

**Container image.** Images are published as `ghcr.io/thnhdatle/wake:v<version>`
(for example `v0.1.0`; the `<major>.<minor>` tag follows the latest patch release).
The image is distroless, runs as user 65532, exposes the admin port 9093 and starts
`wake run`:

```bash
docker run --rm ghcr.io/thnhdatle/wake:v0.1.0 version
```

**Release binary.** Download `wake_<version>_<os>_<arch>.tar.gz` for Linux or
macOS (amd64, arm64) from the
[releases page](https://github.com/thnhdatle/wake/releases), check it against
`checksums.txt` and put `wake` on your `PATH`. The binary is static and has no
runtime dependencies.

**Go install.** `go install github.com/thnhdatle/wake/cmd/wake@v0.1.0` (its
`wake version` prints `dev`; the release archives and images carry the version).

**From source.** With the Go toolchain named in `go.mod`:

```bash
git clone https://github.com/thnhdatle/wake.git
cd wake
make build            # writes bin/wake
bin/wake version
```

Every command takes `--config <file>` (or the environment variable
`WAKE_CONFIG`). The commands are listed in [operations.md](operations.md#commands).

## Prepare Postgres

### Server settings

Wake needs, on the source server:

| Setting | Value | Why |
|---|---|---|
| `wal_level` | `logical` (restart) | pgoutput needs logical decoding |
| `max_replication_slots` | at least one free slot | Wake creates the slot `wake_<source.name>` |
| `max_wal_senders` | at least one free sender | the replication connection is a WAL sender |
| `max_slot_wal_keep_size` | a finite size (recommended) | bounds the WAL a stopped Wake can pin; see [Slot sizing](operations.md#slot-sizing-and-wal-retention) |
| `wal_sender_timeout` | default 60 s | `stream.status_interval` (default 10 s) must stay below half of it |

`wake check` verifies all of these and prints the fix for each failure, with the
error codes listed in [errors.md](errors.md).

### Per provider

The replication privilege is verified by opening a replication connection and
running `IDENTIFY_SYSTEM`. If that fails, Wake reports `replication_privilege`
with the fix for your provider.

| Provider | What to do |
|---|---|
| Self-hosted | Set `wal_level = logical` and restart. `ALTER ROLE wake REPLICATION;`. Allow the role in `pg_hba.conf`; logical replication connects to the database itself, so an entry for that database usually suffices |
| Amazon RDS and Aurora | Set `rds.logical_replication = 1` in the parameter group and reboot. `GRANT rds_replication TO wake;` |
| Google Cloud SQL | Set the flag `cloudsql.logical_decoding = on` (restart). `ALTER ROLE wake REPLICATION;` |
| Azure Database for PostgreSQL | Set the server parameter `wal_level = logical` and restart. `ALTER ROLE wake REPLICATION;` |

Managed services differ in who may own tables and publications. See
[Managed Postgres notes](operations.md#managed-postgres-notes).

### Replica identity and keys

Every captured table should have a primary key. Wake uses the primary key as the
event `key` (and as the Kafka message key, which gives per-key ordering). For a
table without a primary key you can use a unique index through
`ALTER TABLE ... REPLICA IDENTITY USING INDEX ...`, or `REPLICA IDENTITY FULL`.
Tables with none of these ("keyless" tables) are refused at startup
(`table_keyless`) unless you opt in with `tables.skip_unkeyed` or
`tables.emit_unkeyed`; read [operations.md](operations.md#keyless-tables) before
choosing.

With the default replica identity, `before` images of updates and deletes hold
only the key columns. If consumers need the full old row (for example to
invalidate a cache entry derived from a non-key column), set
`REPLICA IDENTITY FULL` on that table.

## A role with the least privileges (recommended)

Wake has two publication modes. In `existing` mode Wake reads a publication that
a DBA created and never alters it, so the Wake role needs no ownership and no
`CREATE` privilege. This is the recommended production setup.

Run as a superuser or the table owner:

```sql
-- the role Wake connects as
CREATE ROLE wake LOGIN REPLICATION PASSWORD 'change-me';
-- RDS and Aurora: GRANT rds_replication TO wake;   (instead of the REPLICATION attribute)

GRANT CONNECT ON DATABASE app TO wake;
GRANT USAGE ON SCHEMA public TO wake;
GRANT SELECT ON public.users, public.orders TO wake;   -- needed to snapshot existing rows

-- the publication Wake reads; partitioned tables are captured through their root
CREATE PUBLICATION wake_app_pub FOR TABLE public.users, public.orders
  WITH (publish_via_partition_root = true);
```

Notes:

- `SELECT` is used for the initial snapshot, for snapshots of tables added later
  and for `wake snapshot table`. Without it those snapshots fail.
- `publish_via_partition_root = true` makes events of a partitioned table carry
  the parent's name. Without it they carry leaf partition names, and Wake logs
  `publication_partition_root` as a warning.
- When you add a table, add it to the publication (`ALTER PUBLICATION ... ADD
  TABLE`) and grant `SELECT`. Wake notices the new table at the next
  `tables.refresh_interval` and snapshots it.
- Wake never needs `CREATE` on the database, ownership of tables or of the
  publication in this mode. It does create the replication slot, which needs the
  replication privilege.
- Any role that can connect to the database can ask Wake for a re-snapshot (see
  [Snapshot requests](operations.md#snapshot-requests-can-be-abused)). Keep
  `CONNECT` on the database restricted.

The matching configuration:

```yaml
source:
  name: app
  dsn_env: WAKE_SOURCE_DSN
  publication:
    mode: existing
    name: wake_app_pub
```

### Managed mode and the quickstart

In `managed` mode (the default) Wake creates and maintains the publication
`wake_<source.name>` from the `tables.include` and `tables.exclude` globs. That
requires `CREATE` on the database (to create the publication) and ownership of
every matched table, which Wake verifies at startup and in `wake check`
(`publication_privilege`). It is
convenient to start with, and the quickstart uses the Postgres superuser in this
mode. Use `existing` mode when you do not want Wake to hold those privileges.

## Configure

A minimal configuration for a Kafka sink:

```yaml
source:
  name: app
  dsn_env: WAKE_SOURCE_DSN
  publication:
    mode: existing
    name: wake_app_pub
state:
  path: /var/lib/wake/wake-state.json
stream:
  spill_dir: /var/lib/wake/spill
sinks:
  - name: kafka-main
    type: kafka
    kafka:
      brokers: ["kafka-1:9092", "kafka-2:9092"]
      topic_prefix: cdc
```

Secrets never go into the file. The DSN is read from the environment variable
named by `source.dsn_env`:

```bash
export WAKE_SOURCE_DSN='postgres://wake:change-me@db.internal:5432/app?sslmode=verify-full'
```

Every key is described in [configuration.md](configuration.md). Check the file:

```bash
wake config validate --config wake.yaml
```

The command lists every problem at once and exits 2 when there is one.

### Persistent storage

Wake writes two things to disk:

- the state file (`state.path`): which slot and Postgres system it belongs to,
  schema versions and snapshot progress ([operations.md](operations.md#state-file));
- spill files (`stream.spill_dir`) for transactions larger than
  `stream.max_tx_bytes` (default 256 MiB).

Put both on a volume that survives restarts. Losing the state file while the slot
exists makes the next start refuse with `state_mismatch`. Size the spill volume
for your largest expected transaction. Spill files are deleted at every start.

## First run

Run the pre-flight checks. They change nothing:

```bash
wake check --config wake.yaml
```

Each line is `OK`, `WARN` or `FAIL` with a check name and, for problems, an error
code that [errors.md](errors.md) explains. Fix every `FAIL`, then start the
service:

```bash
wake run --config wake.yaml
```

On the first start (no slot yet) Wake, in this order:

1. runs the pre-flight checks and reads the server settings;
2. creates the slot `wake_<source.name>` together with an exported snapshot;
3. copies the existing rows of every captured table as `op: "r"` events
   (`snapshot.mode: initial`, the default) while holding back stream events;
4. releases the stream and keeps running.

Large tables take a while to copy; the stream is delivered only after the copy of
all tables has been read, and a crash during the copy resumes where it stopped
([operations.md](operations.md#snapshots)). Use `snapshot.mode: never` to skip the
initial copy and start from the slot's consistent point.

Wake logs JSON lines to standard error; set `WAKE_LOG_FORMAT=text` for text. A
healthy start logs `wake started`.

## Verify

```bash
wake status --config wake.yaml
curl -s http://127.0.0.1:9093/readyz
```

`wake status` prints the status document of the running instance:

- `slot`: name, `wal_status`, `confirmed_flush_lsn` and `lag_bytes`;
- `confirmed_lsn`: the position Wake has confirmed to Postgres, the end of the
  contiguous prefix of transactions that every sink acknowledged;
- `snapshot`: runs, per-table progress and the queue of tables waiting for a
  snapshot;
- `sinks`: state of each sink (`healthy`, `retrying` or `fatal`) with the error
  code and reason when it is not healthy;
- `schemas`: the schema version of every table.

If the admin endpoint is unreachable, `wake status` says so and reads the state
file and `pg_replication_slots` directly. `/readyz` returns 200 once the
replication connection streams and no sink is blocked by a non-retriable error,
and 503 with a list of failing checks otherwise.

Make a change in Postgres and look at your destination. For Kafka the topic is
`<kafka.topic_prefix>.<schema>.<table>`; the event format is described in
[consuming-events.md](consuming-events.md).

## Stopping and restarting

Stop Wake with SIGTERM (or Ctrl-C). It stops reading, lets the sinks flush
(Kafka for at most ten seconds) and exits 0. Restarting, also after a crash, is
always safe: Wake resumes from the slot's confirmed position and re-delivers what
was not yet acknowledged ([delivery-guarantees.md](delivery-guarantees.md)).
`wake run` exits 1 when the replication connection is lost; run it under a
supervisor (systemd, Docker restart policy, a Kubernetes StatefulSet) that
restarts it.

Send SIGHUP to re-read `tables.include`, `tables.exclude` and `tables.name_map`
and re-evaluate the captured tables immediately. Other configuration changes need
a restart.

## Next steps

- [configuration.md](configuration.md): every key, defaults and validation.
- [sinks.md](sinks.md): Kafka, NATS, webhook (with signatures) and stdout.
- [operations.md](operations.md): deployment, alerts, slot sizing, runbooks.
- [consuming-events.md](consuming-events.md): writing a correct consumer.
- [type-mapping.md](type-mapping.md): how each Postgres type appears in the JSON.
- [troubleshooting.md](troubleshooting.md): symptom, cause, fix.
