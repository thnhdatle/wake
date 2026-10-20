# Error codes

Look up an error code and its fix. Codes are stable `snake_case` and appear in command output
(`wake: [<code>] <message>`), logs, `wake check` findings and admin responses
(`{"error": {"code", "message", "request_id"}}`, `/readyz` `checks`).

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
| 0 | success or clean shutdown |
| 1 | runtime or pre-flight failure, lost replication; safe to restart |
| 2 | config error (`config_invalid`) or missing `--yes` (`confirmation_required`) |

## Configuration

### config_invalid

Invalid config, unset DSN or secret variable, or unreadable TLS file. Run `wake config validate`.

### confirmation_required

Re-run with `--yes` once you are sure ([decommission](operations.md#runbooks)).

## State file and slot

### state_mismatch

The state file is missing or belongs to another slot. Mount the right file, or run
`wake run --reinit-state` once and re-snapshot tables that were mid-snapshot.

### slot_missing

The slot is gone. **Changes since then are lost.** Run `wake run --reinit-state` and rebuild
consumers ([runbook](operations.md#when-the-slot-or-the-state-file-is-lost)).

### slot_invalidated

`max_slot_wal_keep_size` was exceeded. **Events since the confirmed LSN are gone.** Run
`wake slot drop --yes`, then `wake run --reinit-state`, and rebuild consumers.

### slot_in_use

Another process (PID shown) holds the slot. Stop it. After a crash, wait up to
`wal_sender_timeout`.

### state_corrupt

Invalid state file, or one from a newer Wake. Restore a backup, or move it away and use
`--reinit-state`.

## Source server

### replication_privilege

The role cannot open a replication connection:

- self-hosted: `ALTER ROLE <role> REPLICATION;` and allow it in `pg_hba.conf`;
- Amazon RDS / Aurora: `GRANT rds_replication TO <role>;`, `rds.logical_replication = 1`, reboot;
- Google Cloud SQL: `cloudsql.logical_decoding = on` and `ALTER ROLE <role> REPLICATION;`;
- Azure Database for PostgreSQL: `wal_level = logical`, restart, `ALTER ROLE <role> REPLICATION;`.

### no_free_slot

Drop an unused slot (`SELECT slot_name, active FROM pg_replication_slots`) or raise
`max_replication_slots` (restart).

### no_free_wal_sender

Stop another replication client or raise `max_wal_senders` (restart).

### wal_level_not_logical

Set `wal_level = logical` and restart (RDS/Aurora: `rds.logical_replication = 1`; Cloud SQL:
`cloudsql.logical_decoding = on`).

### postgres_version_unsupported

Upgrade to Postgres 14 or later.

### status_interval_invalid

Keep `stream.status_interval` below half of `wal_sender_timeout`. Skipped when the timeout is 0.

### wal_retention_unlimited

Warning: `max_slot_wal_keep_size` is `-1`. Set a
[limit](operations.md#slot-sizing-and-wal-retention) and alert on `absent(wake_slot_lag_bytes)`.

## Publications and tables

### publication_privilege

Managed mode lacks `CREATE` or ownership. Grant them, or use `existing` mode
([least privileges](getting-started.md#a-role-with-the-least-privileges-recommended)).

### publication_conflict

`wake_<name>` or `wake_<name>_insert` cannot be managed (for example `FOR ALL TABLES`). Drop or
rename it, or use `existing` mode.

### publication_missing

`source.publication.name` does not exist. Fix the name or create it:

```sql
CREATE PUBLICATION wake FOR TABLE … WITH (publish_via_partition_root = true);
```

### publication_partition_root

Warning: events carry leaf partition names. Recreate the publication with
`publish_via_partition_root = true`.

### table_keyless

Add a primary key or `ALTER TABLE … REPLICA IDENTITY USING INDEX …`, or set
`tables.emit_unkeyed: true` (inserts only) or `tables.skip_unkeyed: true`.

### identifier_invalid

Name not `[A-Za-z0-9_-]+`, topic over 249 characters, or a name clash. Map it in
`tables.name_map` or exclude it. **Earlier changes are not delivered.**

### snapshot_chunk_attempts

A snapshot chunk failed 50 times. Fix the logged cause (permissions, timeouts, a rewrite) and
restart; the snapshot resumes.

## Sinks and readiness

These block the sink: `/readyz` returns 503 and `sink_fatal` fires. Nothing is dropped.

### event_too_large

Raise `max_event_bytes` and the destination limit (Kafka `message.max.bytes`/`max.message.bytes`,
NATS `max_payload`), or exclude the column or table. Then restart Wake.

### sink_fatal

The destination rejects: size limit, auth or certificate, missing topic or stream, webhook
poison batch, failed dead-letter write. Fix it; delivery resumes without a restart.

### replication_not_streaming

Wake is starting, stopped reading, or saw no activity for three `stream.status_interval`s.

### not_ready

A `config`, `replication` or `sink` check failed; see `checks`. A retrying sink stays ready.

### status_unavailable

Wake is starting. `wake status` falls back to the state file.

### internal

An admin handler failed. Report it with the logged stack trace.

## Cache invalidator

### template_requires_full_identity

A template uses a non-key column of a table without full identity. Run
`ALTER TABLE … REPLICA IDENTITY FULL` or use key columns. At runtime `/readyz` returns 503 until
restart.

### template_unknown_column

The column is not published (misspelled, dropped or generated). Fix the template or table.

### template_unknown_table

The table does not exist. Fix the name or create it.

### template_invalid

The template does not parse (empty, bad `{placeholder}`, spaces, stray `}`; use `}}`).

### mapping_invalid

A key is not `<schema>.<table>`, a table has no template, or there are no tables.

### invalid_config

A `pkg/cache/cdcsource` option is invalid. Commands report [`config_invalid`](#config_invalid).

### source_not_running

A cdcsource source is not consuming. The invalidator reports
[`source_unavailable`](#source_unavailable).

### redis_unavailable

Redis does not answer `PING`. Deletes are retried. Restore Redis.

### source_unavailable

Kafka or NATS is not consuming. **Cached values stay until their TTL.** Restore the broker.
