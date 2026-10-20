# Error codes

Every error Wake reports to an operator carries a stable `snake_case` code.
Codes are part of the public API: they appear

- in command output as `wake: [<code>] <message>`,
- in logs as the `error` attribute of startup checks and sink state changes,
- in `wake check` findings as `[<code>]`,
- in admin HTTP responses inside the error envelope
  `{"error": {"code", "message", "request_id"}}` and in the `checks` of
  `/readyz`.

| Code | HTTP status | Where |
|---|---|---|
| [`config_invalid`](#config_invalid) | - | all commands, exit code 2 |
| [`confirmation_required`](#confirmation_required) | - | `wake slot drop`, `wake publication drop`, exit code 2 |
| [`state_mismatch`](#state_mismatch) | - | `wake run` startup |
| [`slot_missing`](#slot_missing) | - | `wake run` startup |
| [`slot_invalidated`](#slot_invalidated) | - | `wake run`, `wake check` |
| [`slot_in_use`](#slot_in_use) | - | `wake run`, `wake check`, `wake slot drop` |
| [`state_corrupt`](#state_corrupt) | - | `wake run`, `wake status` |
| [`replication_privilege`](#replication_privilege) | - | `wake run`, `wake check` |
| [`no_free_slot`](#no_free_slot) | - | `wake run`, `wake check` |
| [`no_free_wal_sender`](#no_free_wal_sender) | - | `wake run`, `wake check` |
| [`wal_level_not_logical`](#wal_level_not_logical) | - | `wake run`, `wake check` |
| [`postgres_version_unsupported`](#postgres_version_unsupported) | - | `wake run`, `wake check` |
| [`status_interval_invalid`](#status_interval_invalid) | - | `wake run`, `wake check` |
| [`wal_retention_unlimited`](#wal_retention_unlimited) (warning) | - | `wake run` log, `wake check` |
| [`publication_privilege`](#publication_privilege) | - | `wake run`, `wake check` (managed mode) |
| [`publication_conflict`](#publication_conflict) | - | `wake run`, `wake check` (managed mode) |
| [`publication_missing`](#publication_missing) | - | `wake run`, `wake check` (existing mode) |
| [`publication_partition_root`](#publication_partition_root) (warning) | - | `wake run` log, `wake check` (existing mode) |
| [`table_keyless`](#table_keyless) | - | `wake run`, `wake check`, refresh log |
| [`identifier_invalid`](#identifier_invalid) | - | `wake run`, `wake check`, refresh log |
| [`snapshot_chunk_attempts`](#snapshot_chunk_attempts) | - | `wake run` |
| [`event_too_large`](#event_too_large) | 503 (`/readyz` check) | sinks |
| [`sink_fatal`](#sink_fatal) | 503 (`/readyz` check) | sinks |
| [`replication_not_streaming`](#replication_not_streaming) | 503 (`/readyz` check) | `wake run` admin |
| [`not_ready`](#not_ready) | 503 | `GET /readyz` |
| [`status_unavailable`](#status_unavailable) | 503 | `GET /v1/status` |
| [`internal`](#internal) | 500 | admin endpoints |
| [`template_requires_full_identity`](#template_requires_full_identity) | 503 (`/readyz` of `wake cache-invalidator`) | cache invalidator, `wake config validate`, exit code 2 at startup |
| [`template_unknown_column`](#template_unknown_column) | - | `wake cache-invalidator`, `wake config validate`, exit code 2 |
| [`template_unknown_table`](#template_unknown_table) | - | `wake cache-invalidator`, `wake config validate`, exit code 2 |
| [`template_invalid`](#template_invalid) | - | `wake cache-invalidator`, `wake config validate`, exit code 2 |
| [`mapping_invalid`](#mapping_invalid) | - | `wake cache-invalidator`, `wake config validate`, exit code 2 |
| [`invalid_config`](#invalid_config) | - | `pkg/cache/cdcsource` option and source errors; reported as `config_invalid` by the commands |
| [`source_not_running`](#source_not_running) | 503 (`source` check of `/readyz`) | `pkg/cache/cdcsource` source health check |
| [`redis_unavailable`](#redis_unavailable) | 503 (`redis` check of `/readyz` of `wake cache-invalidator`) | cache invalidator |
| [`source_unavailable`](#source_unavailable) | 503 (`source` check of `/readyz` of `wake cache-invalidator`) | cache invalidator |

## Exit codes

| Exit code | Meaning |
|---|---|
| 0 | Success; `wake run` stopped by SIGTERM/SIGINT after a clean shutdown |
| 1 | Runtime failure, failed pre-flight check, or an error reported by Postgres or a sink. `wake run` exits 1 when the replication connection is lost or a component fails; the supervisor restarts it (delivery is at-least-once, so a restart is safe) |
| 2 | Configuration error (`config_invalid`) or a destructive command without `--yes` (`confirmation_required`) |

## Configuration

### config_invalid

The configuration file, a `WAKE_*` environment variable or a flag is
invalid, the DSN variable named by `source.dsn_env` is unset, a secret
variable (`webhook.secret_env`, `kafka.sasl.*_env`) is unset, or a TLS file
cannot be read. `wake config validate` lists every problem at once. Fix the
listed keys; exit code 2.

### confirmation_required

`wake slot drop` and `wake publication drop` are destructive and refuse to
run without `--yes`. Re-run with `--yes` once you are sure (decommission
runbook in `operations.md`); exit code 2.

## State file and slot

### state_mismatch

The slot `wake_<source.name>` exists, but the state file (`state.path`) is
missing or belongs to another slot or Postgres system (for example a volume
from another environment, or a restored database with a new system
identifier). Wake refuses to start because schema versions and snapshot
progress would not match the slot. Mount the right state file, or start
once with `wake run --reinit-state`: schema versions are reseeded so they
never decrease, snapshot progress is discarded (unfinished runs are logged;
rerun `wake snapshot table`) and streaming resumes from the slot.

### slot_missing

The state file holds a consistent point, but the slot no longer exists (it
was dropped, or the database was restored or failed over without the slot).
Changes since the slot disappeared are lost. Start with
`wake run --reinit-state`, which creates the slot as on first start (initial
snapshot per `snapshot.mode`); consumers must rebuild their state
(`operations.md`). A state file without a consistent point (slot creation
never completed) does not raise this error: Wake creates the slot as on
first start.

### slot_invalidated

The slot's `wal_status` is `lost`: Postgres removed WAL the slot still
needed (`max_slot_wal_keep_size` exceeded while Wake was stopped or too
slow). Events between the confirmed LSN and now are gone. Drop the slot with
`wake slot drop --yes` and start with `wake run --reinit-state`; consumers
must rebuild. Size `max_slot_wal_keep_size` and alert on
`wake_slot_lag_bytes` (`operations.md`).

### slot_in_use

Another process (named by its PID) consumes the slot; Postgres allows one
consumer per slot. Stop the other Wake instance (only one replica may run).
After a crash the old walsender can hold the slot until Postgres notices the
broken connection (up to `wal_sender_timeout`); the supervisor's restart
succeeds once it is gone. `wake slot drop` reports it for an active slot.

### state_corrupt

The state file is not valid state JSON or was written by a newer Wake. The
file is left untouched for inspection. Restore it from a backup, or move it
away and start with `--reinit-state` (see `state_mismatch`).

## Source server

### replication_privilege

Opening a replication connection and running `IDENTIFY_SYSTEM` failed: the
role lacks the replication privilege or `pg_hba.conf` rejects replication
connections. Fix per provider:

- self-hosted: `ALTER ROLE <role> REPLICATION;` and a `pg_hba.conf` entry
  for the `replication` pseudo-database (logical replication connects to the
  database itself, so the database entry usually suffices);
- Amazon RDS / Aurora: `GRANT rds_replication TO <role>;`, set
  `rds.logical_replication = 1` in the parameter group and reboot;
- Google Cloud SQL: set the flag `cloudsql.logical_decoding = on` and
  `ALTER ROLE <role> REPLICATION;`;
- Azure Database for PostgreSQL: set `wal_level = logical`, restart the
  server and `ALTER ROLE <role> REPLICATION;`.

### no_free_slot

The slot must be created but every `max_replication_slots` slot exists.
Drop an unused slot (`SELECT slot_name, active FROM pg_replication_slots`)
or raise `max_replication_slots` (restart).

### no_free_wal_sender

Every `max_wal_senders` connection is in use. Stop another replication
client or raise `max_wal_senders` (restart).

### wal_level_not_logical

`wal_level` is not `logical`. Set it and restart Postgres (RDS/Aurora:
`rds.logical_replication = 1` and reboot; Cloud SQL:
`cloudsql.logical_decoding = on`; Azure: `wal_level = logical` and restart).

### postgres_version_unsupported

The server is older than Postgres 14. Upgrade; `operations.md` lists the
minimum minor versions.

### status_interval_invalid

`stream.status_interval` is not below half of the server's
`wal_sender_timeout`, so the server could drop the connection while reading
is paused for backpressure. Lower `stream.status_interval`
or raise `wal_sender_timeout`. The check is skipped when the timeout is 0.

### wal_retention_unlimited

Warning: `max_slot_wal_keep_size` is `-1`, so a stopped or stuck Wake
retains WAL until the disk fills. `wake check` prints the current slot lag
and the sizing guidance of `operations.md`. Set a limit that covers your
longest expected outage, and alert on `wake_slot_lag_bytes` and
`absent(wake_slot_lag_bytes)`.

## Publications and tables

### publication_privilege

Managed mode cannot create or alter its publications: the role lacks
`CREATE` on the database (to create `wake_<name>`), does not own an existing
managed publication, or does not own every matched table. Grant the
privileges, change the owner, or use `existing` mode with a publication a
DBA created (least privilege, `getting-started.md`).

### publication_conflict

A publication named `wake_<name>` (or `wake_<name>_insert`) exists but
managed mode cannot manage it, for example it is `FOR ALL TABLES`. Drop or
rename it, or switch to `existing` mode and set `source.publication.name`.

### publication_missing

In `existing` mode, `source.publication.name` does not exist. Create it,
for example `CREATE PUBLICATION wake FOR TABLE … WITH
(publish_via_partition_root = true);`, or fix the name.

### publication_partition_root

Warning (existing mode): the publication holds partitioned tables with
`publish_via_partition_root = false`, so events carry the leaf partition
names instead of the parent's. Recreate the publication with
`publish_via_partition_root = true` if consumers expect the parent name.

### table_keyless

A captured table has no primary key and no replica-identity index (replica
identity `NOTHING`, or `DEFAULT` without a primary key). Managed mode never
adds it to `wake_<name>` (the application's UPDATE/DELETE would fail). At
startup Wake fails unless one of:

- add a primary key or `ALTER TABLE … REPLICA IDENTITY USING INDEX …`;
- `tables.emit_unkeyed: true`: its inserts are emitted with `key: null`
  (managed mode publishes it in `wake_<name>_insert` with
  `publish = 'insert'`; UPDATE/DELETE are not captured);
- `tables.skip_unkeyed: true`: the table is skipped (no events).

Tables detected at runtime are skipped and logged with this code.

### identifier_invalid

A schema or table name is not a valid event identifier (`[A-Za-z0-9_-]+`), its Kafka
topic `<topic_prefix>.<schema>.<table>` would exceed 249 characters, or two
captured tables map to the same name. Map the table in `tables.name_map`
(`"<schema>.<table>": "<schema>.<table>"`) to a valid, shorter, unique name,
or exclude it. A table detected at runtime is skipped until mapped; after
the mapping (SIGHUP or restart) it is treated as newly detected and
snapshotted. Changes before the mapping are not delivered.

### snapshot_chunk_attempts

A snapshot chunk failed 50 times in a row (read or watermark errors, or
repeated visibility retries), so `wake run` stopped. The log names the table
and the last error; fix the cause (permissions on the table, statement
timeouts, a table rewrite) and restart: the run resumes from its last
acknowledged position.

## Sinks and readiness

Sink errors never drop events
([delivery guarantees](delivery-guarantees.md)): a sink retries every error
without limit at a capped, jittered backoff and Wake stops reading while it
cannot deliver. The two codes below mark non-retriable errors: they set
`/readyz` to 503 (check `sink` with `state: fatal`) and fire the
`sink_fatal` alert (`wake_alert_active{rule="sink_fatal"}`, POST to
`lag.alert_webhook`). The confirmed LSN does not pass the blocked event.

### event_too_large

An event (key, value and headers) exceeds the sink's `max_event_bytes`. The
reason names the table, key and LSN. Raise `max_event_bytes` (and the
destination's own limit: Kafka `message.max.bytes`/`max.message.bytes`,
NATS `max_payload`), or exclude the column or table; Wake's own
configuration takes effect on restart, after which the event is delivered.

### sink_fatal

Any other non-retriable sink error: a record above the topic's
`max.message.bytes` or the NATS server's `max_payload`, authentication or
authorization failure, certificate error, unknown topic with
`kafka.auto_create_topics: false`, unknown NATS stream with
`nats.create_stream: false`, a webhook poison batch (non-2xx other than
408/429/5xx) with `webhook.dead_letter_dir` empty, or a failed dead-letter
write. Fix the cause at the destination (raise the limit, fix credentials
or ACLs, create the topic or stream, fix the receiver); delivery resumes
without a restart.

### replication_not_streaming

`/readyz` check `replication` failing: START_REPLICATION has not been issued
yet (startup: pre-flight checks, publication and slot setup, which can wait
for long-running transactions when the slot is created), the stream reader
stopped, or the replication connection showed no activity (messages
received or standby status updates accepted) for three
`stream.status_interval`s. A lost connection makes `wake run` exit 1; the
supervisor restarts it.

### not_ready

`GET /readyz` answers 503 with this code when any check fails: `config`,
`replication` (`replication_not_streaming`) or a `sink` with a
non-retriable error (`event_too_large`, `sink_fatal`). The message lists
the failing checks; `checks` carries every check with its `code` and
`reason`. A sink that is only retrying (broker down, 5xx) keeps the service
ready: backpressure is the designed behavior, and `lag.max_ack_age` alerts
when it lasts.

### status_unavailable

`GET /v1/status` answers 503 while Wake is starting (before the pipeline is
wired). `wake status` then falls back to the state file and
`pg_replication_slots` and says so.

### internal

An admin handler failed unexpectedly (500). The log holds the stack trace;
please report it.

## Cache invalidator

### template_requires_full_identity

A `cache_invalidator.templates` entry references a non-key column of a
table whose replica identity is not `FULL`, so `u`/`d` events lack that
column in `before` and the old key cannot be invalidated. Reported at
startup, and by `wake config validate`, when the environment variable named by
`cache_invalidator.validate_dsn_env` is set (exit code 2; the check is skipped
while the variable is unset), and at runtime on the first such event (error
log, metric, `/readyz` 503 of `wake cache-invalidator`); processing continues
with the images available. Run `ALTER TABLE … REPLICA IDENTITY FULL` or
reference key columns only.

### template_unknown_column

A `cache_invalidator.templates` entry references a column the table does not
publish (a misspelled name, a dropped column, or a generated column, which
logical replication does not carry). Found by the `validate_dsn_env` check at
startup and in `wake config validate`; exit code 2. Fix the template or the
table.

### template_unknown_table

A table named in `cache_invalidator.templates` does not exist in the database
the `validate_dsn_env` check connected to (after applying `tables.name_map` in
reverse). Found at startup and in `wake config validate`; exit code 2. Fix the
table name or create the table.

### template_invalid

A key template of `cache_invalidator.templates` does not parse: an empty
template, an unclosed or empty `{placeholder}`, white space around a column
name, or a stray `}` (write `}}` for a literal brace). The message names the template and
the byte offset. Exit code 2.

### mapping_invalid

A `cache_invalidator.templates` key is not a `<schema>.<table>` name, a table
has no key template, or the mapping has no tables at all. Exit code 2.

### invalid_config

`pkg/cache/cdcsource` rejected an option or a source configuration (for
example an empty broker list or a metrics registration conflict). Library
callers match on it; `wake cache-invalidator` reports it as
[`config_invalid`](#config_invalid), exit code 2.

### source_not_running

The health check of a cdcsource event source (Kafka or NATS) found it not
consuming: before `Consume` started or after it returned. Inside
`wake cache-invalidator` this surfaces as
[`source_unavailable`](#source_unavailable).

### redis_unavailable

The `redis` check of `GET /readyz` of `wake cache-invalidator` failed: Redis
does not answer a `PING` (down, wrong address, TLS or password error). The
invalidator keeps running: failed deletes are deferred and retried, and the
`pkg/cache` instances apply them (or bump the namespace epoch) once Redis is
back. Restore Redis connectivity; `reason` carries the client error.

### source_unavailable

The `source` check of `GET /readyz` of `wake cache-invalidator` failed: the
Kafka or NATS source is not consuming or its broker does not answer (broker
down, wrong address, missing topic or stream, authentication). The invalidator
keeps retrying; invalidations stop meanwhile, so cached values stay until
their TTL. Restore the broker and check `reason`.
