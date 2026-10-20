# Getting started

Set up Wake to stream a real Postgres database to Kafka. To try it on your laptop first, see
[quickstart.md](quickstart.md).

## What you need

- Postgres 14, 15, 16 or 17 with `wal_level=logical`.
- A destination: Kafka, NATS JetStream, an HTTPS webhook or stdout ([sinks.md](sinks.md)).
- Persistent storage for the state file and spill directory ([below](#persistent-storage)).
- Exactly one Wake process per source. A second one fails with `slot_in_use`.

## Install

Pick one.

```bash
docker run --rm ghcr.io/thnhdatle/wake:v0.1.0 version   # container image
go install github.com/thnhdatle/wake/cmd/wake@v0.1.0    # Go install (version prints dev)
```

Release binaries (`wake_<version>_<os>_<arch>.tar.gz`) are on the
[releases page](https://github.com/thnhdatle/wake/releases); check them against `checksums.txt`.
From source:

```bash
git clone https://github.com/thnhdatle/wake.git
cd wake
make build            # writes bin/wake
bin/wake version
```

All commands: [operations.md](operations.md#commands).

## Prepare Postgres

### Server settings

| Setting | Value | Why |
|---|---|---|
| `wal_level` | `logical` (restart) | logical decoding |
| `max_replication_slots` | at least one free | Wake creates slot `wake_<source.name>` |
| `max_wal_senders` | at least one free | the replication connection |
| `max_slot_wal_keep_size` | a finite size (recommended) | bounds pinned WAL; see [Slot sizing](operations.md#slot-sizing-and-wal-retention) |
| `wal_sender_timeout` | default 60 s | must exceed twice `stream.status_interval` (default 10 s) |

`wake check` verifies these and prints a fix per failure ([errors.md](errors.md)).

### Per provider

| Provider | What to do |
|---|---|
| Self-hosted | Set `wal_level = logical` and restart. `ALTER ROLE wake REPLICATION;`. Allow the role for the database in `pg_hba.conf` |
| Amazon RDS and Aurora | Set `rds.logical_replication = 1` in the parameter group and reboot. `GRANT rds_replication TO wake;` |
| Google Cloud SQL | Set the flag `cloudsql.logical_decoding = on` (restart). `ALTER ROLE wake REPLICATION;` |
| Azure Database for PostgreSQL | Set the server parameter `wal_level = logical` and restart. `ALTER ROLE wake REPLICATION;` |

See also [Managed Postgres notes](operations.md#managed-postgres-notes).

### Replica identity and keys

Give every captured table a primary key, or `REPLICA IDENTITY USING INDEX`. Keyless tables
fail with `table_keyless` ([keyless tables](operations.md#keyless-tables)). Set
`REPLICA IDENTITY FULL` if consumers need the whole old row in `before`.

## A role with the least privileges (recommended)

In `existing` mode Wake reads a DBA-created publication and needs no ownership or `CREATE`.
Run as a superuser or the table owner:

```sql
CREATE ROLE wake LOGIN REPLICATION PASSWORD 'change-me';
-- RDS and Aurora: GRANT rds_replication TO wake;   (instead of REPLICATION)

GRANT CONNECT ON DATABASE app TO wake;
GRANT USAGE ON SCHEMA public TO wake;
GRANT SELECT ON public.users, public.orders TO wake;   -- needed for snapshots

CREATE PUBLICATION wake_app_pub FOR TABLE public.users, public.orders
  WITH (publish_via_partition_root = true);           -- partitioned tables under the root name
```

- To add a table: `ALTER PUBLICATION ... ADD TABLE` and grant `SELECT`.
- **Any role that can connect can trigger a re-snapshot.** Keep `CONNECT` restricted
  ([snapshot requests](operations.md#snapshot-requests-can-be-abused)).

```yaml
source:
  name: app
  dsn_env: WAKE_SOURCE_DSN
  publication:
    mode: existing        # read the DBA's publication
    name: wake_app_pub
```

### Managed mode and the quickstart

`managed` mode (the default) creates `wake_<source.name>` from `tables.include`. It needs
`CREATE` and ownership of every matched table.

## Configure

```yaml
source:
  name: app
  dsn_env: WAKE_SOURCE_DSN                  # env var holding the DSN
  publication:
    mode: existing
    name: wake_app_pub
state:
  path: /var/lib/wake/wake-state.json       # persistent
stream:
  spill_dir: /var/lib/wake/spill            # persistent
sinks:
  - name: kafka-main
    type: kafka
    kafka:
      brokers: ["kafka-1:9092", "kafka-2:9092"]
      topic_prefix: cdc
```

```bash
export WAKE_SOURCE_DSN='postgres://wake:change-me@db.internal:5432/app?sslmode=verify-full'
wake config validate --config wake.yaml   # lists every problem, exits 2 on any
```

Secrets never go in the file. Every key is in [configuration.md](configuration.md).

### Persistent storage

Put `state.path` ([state file](operations.md#state-file)) and `stream.spill_dir` on a volume
that survives restarts. A lost state file blocks the next start (`state_mismatch`). Size the
volume for your largest transaction.

## First run

```bash
wake check --config wake.yaml   # pre-flight checks, changes nothing
wake run --config wake.yaml     # start
```

Fix every `FAIL` ([errors.md](errors.md)). The first start copies existing rows as `op: "r"`
events, then streams changes ([snapshots](operations.md#snapshots)). Set `snapshot.mode: never`
to skip the copy. A healthy start logs `wake started`; set `WAKE_LOG_FORMAT=text` for text logs.

## Verify

```bash
wake status --config wake.yaml           # slot, confirmed_lsn, snapshot, sinks, schemas
curl -s http://127.0.0.1:9093/readyz     # 200 when streaming and no sink blocked, else 503
```

Make a change and check the destination. Kafka topics are
`<kafka.topic_prefix>.<schema>.<table>` ([event format](consuming-events.md)).

## Stopping and restarting

- SIGTERM or Ctrl-C: flushes sinks (Kafka at most ten seconds) and exits 0.
- Restart, even after a crash, is safe. Unacknowledged events are re-delivered
  ([delivery-guarantees.md](delivery-guarantees.md)).
- `wake run` exits 1 when replication is lost. Run it under a supervisor that restarts it.
- SIGHUP re-reads `tables.include`, `tables.exclude` and `tables.name_map`. Other changes need
  a restart.

## Next steps

- [configuration.md](configuration.md): every key.
- [sinks.md](sinks.md): Kafka, NATS, webhook and stdout.
- [operations.md](operations.md): deployment, alerts, runbooks.
- [consuming-events.md](consuming-events.md): writing a correct consumer.
- [type-mapping.md](type-mapping.md): Postgres types in JSON.
- [troubleshooting.md](troubleshooting.md): symptom, cause, fix.
