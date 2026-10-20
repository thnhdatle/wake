# Operations

Deploy, monitor, upgrade and retire Wake, and recover from a lost slot or state file.

- [Commands](#commands)
- [Deploying](#deploying)
- [Managed Postgres notes](#managed-postgres-notes)
- [Slot sizing and WAL retention](#slot-sizing-and-wal-retention)
- [Snapshots](#snapshots)
- [Keyless tables](#keyless-tables)
- [Changes before a name mapping](#changes-before-a-name-mapping)
- [State file](#state-file)
- [When the slot or the state file is lost](#when-the-slot-or-the-state-file-is-lost)
- [Upgrades](#upgrades)
- [Metrics and alerts](#metrics-and-alerts)
- [Runbooks](#runbooks)

## Commands

All commands take `--config <file>` or `WAKE_CONFIG`.

| Command | What it does |
|---|---|
| `wake run [--reinit-state]` | runs the service; exits 0 on SIGTERM/SIGINT, 1 on failure. `--reinit-state`: [below](#when-the-slot-or-the-state-file-is-lost) |
| `wake check` | pre-flight checks, changes nothing; exits 1 on a failure |
| `wake status [--addr host:port]` | prints `GET /v1/status`; falls back to the state file |
| `wake snapshot table <schema.table>` | requests a table snapshot |
| `wake slot drop --yes` | drops slot `wake_<source.name>`; without `--yes` exits 2 |
| `wake publication drop --yes` | drops managed publications `wake_<name>` and `wake_<name>_insert` |
| `wake config validate` | lists every config problem; exits 2 on errors |
| `wake config print [--redact]` | prints effective config; `--redact` masks URL passwords |
| `wake cache-invalidator` | runs the [cache invalidator](cache.md) |
| `wake healthcheck [--addr host:port]` | exits 0 when `/readyz` returns 200 |
| `wake version` | prints version information |

Exit codes: 0 success, 1 failure, 2 config error or missing `--yes`
([errors.md](errors.md#exit-codes)).

### Admin endpoints

`admin.listen` (default `127.0.0.1:9093`):

| Path | Meaning |
|---|---|
| `/healthz` | process is alive |
| `/readyz` | 200 when streaming and no sink is blocked; else 503 with failing `checks` |
| `/metrics` | Prometheus metrics |
| `/v1/status` | slot, confirmed LSN, lag, snapshots, sinks, schema versions |
| `/v1/version` | build information |

API: [api/openapi.yaml](../../api/openapi.yaml). **The endpoints have no authentication.**
Bind them to a private interface or protect them with a network policy.

## Deploying

Run exactly one Wake per source, under a supervisor that restarts it. A restart is safe.

### Docker Compose

[deploy/docker-compose.yml](../../deploy/docker-compose.yml) is the [quickstart](quickstart.md)
stack. State and spill live on the `wake-data` volume. For long-lived use, add
`restart: unless-stopped` and replace the development credentials. **Never run
`docker compose down -v`**: the state file belongs to the slot.

### Kubernetes

```bash
kubectl apply -k deploy/kubernetes   # after creating the wake-source Secret
```

[deploy/kubernetes](../../deploy/kubernetes) runs one StatefulSet replica with a 1 Gi volume.

- Use `/healthz` for liveness and `/readyz` for readiness, never `/readyz` for liveness.
- Restrict port 9093 with a NetworkPolicy.
- Memory: about `stream.max_tx_bytes` (default 256 MiB) plus 64 MiB per Kafka or NATS sink,
  plus headroom.
- Run `wake check` from a one-off pod before the first rollout.

## Managed Postgres notes

Provider settings are in [getting-started.md](getting-started.md#per-provider) and
[`replication_privilege`](errors.md#replication_privilege).

- **RDS and Aurora**: set `max_slot_wal_keep_size`; the default is unlimited. Assume the slot
  does not survive a failover.
- **`managed` mode** needs table ownership (or `GRANT app_owner TO wake;`). If migrations run as
  another role, use `existing` mode
  ([least privileges](getting-started.md#a-role-with-the-least-privileges-recommended)).

**Failover slots (Postgres 17)**: Wake does not set the `failover` flag. To have a standby sync
the slot, configure slot sync and create the slot yourself before the first start:

```sql
SELECT pg_create_logical_replication_slot('wake_app', 'pgoutput', false, false, true);
```

Then run `wake run --reinit-state` once and `wake snapshot table` for each table. This is
untested; rehearse it.

## Slot sizing and WAL retention

The slot keeps all unconfirmed WAL. **If Wake stops or a sink is down, WAL grows until the
primary's disk fills and the primary stops.**

```text
max_slot_wal_keep_size  >=  peak WAL bytes per second  x  longest tolerated outage in seconds  x  1.5
```

- Past the limit the slot is invalidated and consumers must rebuild.
- Set `lag.warn_bytes` near a quarter of the limit and `lag.critical_bytes` near three quarters.
  The defaults (1 GiB, 10 GiB) assume a limit well above 10 GiB.
- A stopped Wake has no metrics: alert on `absent(wake_slot_lag_bytes)`.

`slot.wal_status: unreserved` means WAL is about to be removed: act now.

## Snapshots

A snapshot sends existing rows as `op: "r"` events: on first start, for added tables and on
request.

```bash
wake snapshot table public.users   # re-send one table
```

- Treat `r` events as upserts by `seq`. **They are not deduplicated by id.**
- Keyless tables: avoid `VACUUM FULL`, `CLUSTER` or rewriting `ALTER TABLE` during their
  snapshot, or rows may be skipped or repeated.
- Rows in a partition attached during a snapshot are missed. Re-run `wake snapshot table`.

### Snapshot requests can be abused

**Any role with `CONNECT` can trigger a re-snapshot** and flood your sinks. Run
`REVOKE CONNECT ON DATABASE app FROM PUBLIC;`, grant it explicitly, and alert on
`wake_snapshot_requests_total`.

## Keyless tables

| Option | Behaviour |
|---|---|
| neither set (default) | `table_keyless`; startup fails |
| `tables.skip_unkeyed: true` | skipped, no events |
| `tables.emit_unkeyed: true` | **inserts only**, `key: null` |

**Their updates and deletes are not captured.** In `existing` mode publish them with
`publish = 'insert'`.

## Changes before a name mapping

Names must match `[A-Za-z0-9_-]+`. Map others in `tables.name_map` and send `SIGHUP`; the table
is then snapshotted. **Earlier changes are not delivered**, and rows deleted meanwhile are never
announced.

## State file

**Back up `state.path`** regularly and before upgrades; copying while Wake runs is safe.

## When the slot or the state file is lost

| Situation | Error | Meaning |
|---|---|---|
| slot exists, state file missing or foreign | `state_mismatch` | no events lost; snapshot progress unknown |
| state file exists, slot does not | `slot_missing` | **changes since the loss are gone** |
| slot `wal_status` is `lost` | `slot_invalidated` | **events since the confirmed position are gone** |

`wake run --reinit-state`:

- on `state_mismatch`: adopts the slot. Rerun `wake snapshot table` for tables mid-snapshot.
- on `slot_missing`: creates a new slot. **Consumers must rebuild.**

To rebuild after `slot_missing` or `slot_invalidated`:

1. Stop the consumers.
2. Discard derived state: tables, indexes, caches, Kafka topics or the JetStream stream.
3. For `slot_invalidated`: `wake slot drop --yes`.
4. Start `wake run --reinit-state` with `snapshot.mode: initial`.
5. Consumers read from the beginning as upserts ordered by `seq`.

## Upgrades

### Wake

1. Read the CHANGELOG.
2. Back up the state file.
3. Stop with SIGTERM, then start the new version with the same config and volume.
4. Check `wake status` and `/readyz`.

A downgrade needs the backup.

### Postgres

Supported: 14, 15, 16 and 17. Minor upgrades need nothing. Major upgrades, losing nothing:

1. Stop the writers.
2. Wait for `lag_bytes` near 0 and no sink retrying.
3. Stop Wake and upgrade Postgres.
4. Set `snapshot.mode: never` and start `wake run --reinit-state`.
5. Restart the writers.

If you cannot stop the writers, plan a consumer rebuild.

## Metrics and alerts

### Metrics

`table` labels are capped at 50 values; the rest count as `other`.

**Slot and lag**

| Metric | Type | Meaning |
|---|---|---|
| `wake_slot_lag_bytes` | gauge | WAL bytes behind `confirmed_flush_lsn` |
| `wake_slot_wal_status` | gauge | `reserved` 0, `extended` 1, `unreserved` 2, `lost` 3, unknown -1 |
| `wake_oldest_unacked_age_seconds` | gauge | oldest unacknowledged event age |
| `wake_lag_poll_errors_total` | counter | failed slot polls |
| `wake_alert_active{rule}` | gauge | 1 while a rule fires |
| `wake_end_to_end_latency_seconds{sink}` | histogram | commit to acknowledgement |

**Sinks**

| Metric | Type | Meaning |
|---|---|---|
| `wake_sink_events_sent_total{sink}` | counter | events sent, re-sends included |
| `wake_sink_bytes_sent_total{sink}` | counter | bytes sent |
| `wake_sink_events_acked_total{sink}` | counter | events acknowledged |
| `wake_sink_retries_total{sink,reason}` | counter | retries by reason. Kafka: `network`, `broker`, `unknown_topic`, `message_too_large`, `event_too_large`, `auth`, `rejected`, `other`. NATS: `network`, `server`, `unknown_stream`, `message_too_large`, `event_too_large`, `auth`, `rejected`, `other`. Webhook: `server_error`, `rate_limited`, `timeout`, `network`, `poison`, `dead_letter`, `internal` |
| `wake_sink_dead_lettered_total{sink}` | counter | webhook dead letters |
| `wake_sink_inflight{sink}` | gauge | unacknowledged events |
| `wake_sink_send_duration_seconds{sink}` | histogram | one delivery attempt |

**Stream and snapshots**

| Metric | Type | Meaning |
|---|---|---|
| `wake_spilled_transactions_total` | counter | transactions spilled |
| `wake_spilled_bytes_total` | counter | bytes spilled |
| `wake_spilled_transaction_size_bytes` | histogram | spilled transaction size |
| `wake_unknown_types_total{type}` | counter | [unmapped types](type-mapping.md) |
| `wake_unknown_messages_total{type}` | counter | unknown pgoutput messages |
| `wake_truncate_skipped_total{table}` | counter | truncates dropped |
| `wake_tables_skipped_total{table,reason}` | counter | tables not captured (`not_captured`, `identifier_invalid`, `table_keyless`) |
| `wake_snapshot_rows_total{table}` | counter | snapshot rows acknowledged |
| `wake_snapshot_requests_total{table}` | counter | snapshot requests |
| `wake_snapshot_chunk_retries_total{table}` | counter | snapshot chunks re-read |
| `wake_build_info{version,commit,go_version}` | gauge | always 1 |

Cache metrics: [cache.md](cache.md#metrics).

### Alerts

On each rule change Wake sets `wake_alert_active{rule}` and POSTs to `lag.alert_webhook`:

```json
{"rule":"slot_lag_warn","state":"firing","value":9437184,"threshold":8388608,"ts":"2026-10-18T12:00:00Z"}
```

| Rule | Fires while | Threshold |
|---|---|---|
| `slot_lag_warn` | slot lag above threshold | `lag.warn_bytes` |
| `slot_lag_critical` | slot lag above threshold | `lag.critical_bytes` |
| `max_ack_age` | oldest unacknowledged event older than threshold | `lag.max_ack_age` |
| `sink_fatal` | a sink is blocked | 0 |

### Prometheus rules

Use [deploy/prometheus/alerts.yml](../../deploy/prometheus/alerts.yml). Match its thresholds
(1 GiB, 10 GiB, 300 s) to your `lag.*` settings. Keep `WakeDown` and `WakeMetricsAbsent`.

## Runbooks

### Decommission: `wake slot drop`

**Never just stop Wake**: the slot keeps pinning WAL until the disk is full.

```bash
wake slot drop --yes --config wake.yaml          # after stopping Wake; undelivered changes are lost
wake publication drop --yes --config wake.yaml   # managed mode only
```

Then drop an `existing` publication yourself, delete the state file and volume, and revoke the
role.

### A sink is blocked

`wake status` shows `state: fatal` with a `code`. Fix it
([errors.md](errors.md#sinks-and-readiness)); delivery resumes.

### Slot lag is growing

1. `wake status`: is a sink `retrying`? Fix the destination.
2. Otherwise look for long transactions in `pg_stat_activity`.
3. Near `max_slot_wal_keep_size`: add disk, or accept invalidation and rebuild.
