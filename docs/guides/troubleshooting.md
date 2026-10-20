# Troubleshooting

Find your symptom, then apply the fix. Codes are explained in [errors.md](errors.md).

```bash
wake status --config wake.yaml          # slot, confirmed LSN, lag, sinks, snapshots
wake check --config wake.yaml           # pre-flight checks with error codes
```

Also read the `wake run` logs (stderr; `WAKE_LOG_FORMAT=text` for text).

- [Wake does not start](#wake-does-not-start)
- [Wake runs but is not ready or not delivering](#wake-runs-but-is-not-ready-or-not-delivering)
- [Events are missing or look wrong](#events-are-missing-or-look-wrong)
- [Duplicates and ordering](#duplicates-and-ordering)
- [Snapshots](#snapshots)
- [Disk and WAL](#disk-and-wal)
- [Sinks](#sinks)
- [Cache and invalidator](#cache-and-invalidator)

## Wake does not start

| Symptom | Cause | Fix |
|---|---|---|
| Exit code 2, `[config_invalid]` | invalid config | `wake config validate` |
| `[replication_privilege]` | no replication privilege | [fix per provider](errors.md#replication_privilege) |
| `[wal_level_not_logical]` | `wal_level` is not `logical` | [fix](errors.md#wal_level_not_logical) |
| `[no_free_slot]` | all `max_replication_slots` used | [fix](errors.md#no_free_slot) |
| `[no_free_wal_sender]` | all `max_wal_senders` used | [fix](errors.md#no_free_wal_sender) |
| `[slot_in_use]` naming a PID | another Wake, or a crashed one's connection | [fix](errors.md#slot_in_use) |
| `[state_mismatch]` | state file missing or foreign | [fix](errors.md#state_mismatch) |
| `[slot_missing]` | slot gone; **consumers must rebuild** | [fix](errors.md#slot_missing) |
| `[slot_invalidated]` | `wal_status = lost`; **consumers must rebuild** | [fix](errors.md#slot_invalidated) |
| `[state_corrupt]` | invalid or newer state file. Never edit it by hand | [fix](errors.md#state_corrupt) |
| `[status_interval_invalid]` | interval too close to `wal_sender_timeout` | [fix](errors.md#status_interval_invalid) |
| `[postgres_version_unsupported]` | Postgres older than 14 | upgrade |
| `[publication_privilege]` (managed mode) | missing `CREATE` or ownership | [fix](errors.md#publication_privilege) |
| `[publication_conflict]` | `wake_<name>` cannot be managed | [fix](errors.md#publication_conflict) |
| `[publication_missing]` (existing mode) | publication missing | [fix](errors.md#publication_missing) |
| `[table_keyless]` | table has no key | [fix](errors.md#table_keyless) |
| `[identifier_invalid]` | invalid or clashing name | [fix](errors.md#identifier_invalid) |
| Stuck before `wake started` | slot creation waits for open transactions | end them (`pg_stat_activity`) |
| Warning `wal_retention_unlimited` | no WAL limit | [set one](operations.md#slot-sizing-and-wal-retention) |
| Warning `publication_partition_root` | leaf partition names | [fix](errors.md#publication_partition_root) |

## Wake runs but is not ready or not delivering

| Symptom | Cause | Fix |
|---|---|---|
| `/readyz` 503, `replication_not_streaming` | starting or not streaming | read the logs |
| `/readyz` 503, `event_too_large` | event too big | [fix](errors.md#event_too_large) |
| `/readyz` 503, `sink_fatal` | destination rejects | [fix](errors.md#sink_fatal) |
| Sink `retrying`, lag grows, `/readyz` 200 | destination down; nothing lost | restore it; see `wake_sink_retries_total` |
| "admin endpoint unreachable" | Wake down, or loopback in a container | `--addr`, or `admin.listen: 0.0.0.0:9093` |
| Restart loop with `slot_in_use` | two instances | run one per source |
| Alert webhook never arrives | unset or receiver fails | check receiver and logs |

## Events are missing or look wrong

| Symptom | Cause | Fix |
|---|---|---|
| No events for a table | not captured or not yet refreshed | `wake check`; `wake_tables_skipped_total`; `SIGHUP` |
| New table's existing rows missing | `snapshot.added_tables: false`, queued, or no `SELECT` | grant `SELECT`; `wake snapshot table` |
| Only inserts arrive | keyless table (`emit_unkeyed`) | add a primary key |
| Leaf partition names in events | `publish_via_partition_root = false` | recreate with `true` |
| Table skipped, `identifier_invalid` | invalid name | [map it](operations.md#changes-before-a-name-mapping) |
| `before` holds only the key | default replica identity | `REPLICA IDENTITY FULL` |
| Column in `unchanged_columns` | unchanged TOAST value | [keep the old value](consuming-events.md#unchanged-columns) |
| Large integer slightly wrong | `int8` parsed as float | [parse exactly](consuming-events.md#integers-above-253) |
| No schema event after DDL | sent at the next write | write to the table |
| Versions jumped to ~1.7 billion | `--reinit-state` | expected |
| `varchar(n)` change, lists look equal | [known limitation](schema-changes.md#limitations) | rely on `schema_version` |
| Kafka records with empty value | tombstones | skip, or `kafka.tombstones: false` |
| Events with `"key": null` | keyless table or truncate | skip and count |

## Duplicates and ordering

| Symptom | Cause | Fix |
|---|---|---|
| Same `id` twice | at-least-once | dedupe by `id` or upsert by `seq` |
| NATS duplicates after long outage | longer than `nats.duplicate_window` (2 m) | dedupe in consumer; raise the window |
| `r` events for known rows | snapshot ran again | upsert by `seq` |
| Update in another partition | key changed | [compare `seq`](consuming-events.md#key-changing-updates) |
| Webhook order broken | dead-lettered batches | replay files by `seq` |
| `lsn` string sort wrong | hex, variable length | `event.Compare` |

## Snapshots

| Symptom | Cause | Fix |
|---|---|---|
| Initial snapshot slow | large tables | raise `parallel_tables`, `chunk_rows` |
| Exit with `snapshot_chunk_attempts` | chunk failed 50 times | [fix](errors.md#snapshot_chunk_attempts) |
| `wake_snapshot_chunk_retries_total` grows | concurrent writes | normal under load |
| Flood of `op: "r"` events | re-snapshot | [restrict `CONNECT`](operations.md#snapshot-requests-can-be-abused) |
| Keyless table rows skipped or doubled | rewrite during snapshot | re-run `wake snapshot table` |
| Attached partition rows missing | attached during snapshot | `wake snapshot table <parent>` |

## Disk and WAL

| Symptom | Cause | Fix |
|---|---|---|
| WAL grows, high `wake_slot_lag_bytes` | sink down or long transaction | [runbooks](operations.md#runbooks) |
| WAL grows, no Wake metrics | Wake stopped | start it or `wake slot drop --yes` |
| Wake volume full | spill or dead letters | resize; move `dead_letter_dir` |
| WAL grows when idle | other databases write | check Wake runs, `heartbeat.interval` |

## Sinks

| Symptom | Cause | Fix |
|---|---|---|
| Kafka: unknown topic | auto-create off | create topics incl. `<prefix>._schema` |
| Kafka: auth errors | credentials, ACLs or CA | check `sasl.*_env`, ACLs, `tls.ca_file` |
| NATS: stream missing | `create_stream: false` or subject clash | create it on `<prefix>.>` |
| Webhook: fatal after 4xx | receiver rejects | fix the receiver |
| Webhook: bad signature | parsed body, wrong secret, clock skew, proxy | [verify raw bytes](sinks.md#verifying-signatures) |
| Webhook: duplicate batches | retries | dedupe by `id` or `Wake-Delivery-Id` |

## Cache and invalidator

| Symptom | Cause | Fix |
|---|---|---|
| Stale cache values | replica loader, mismatched namespace or channel, wrong templates | load from primary; match `namespace`, `bus_channel` |
| Stale after `TRUNCATE` | not invalidated | TTL or delete keys |
| Stale after a template column changed | no `REPLICA IDENTITY FULL` | set it, or use key columns |
| Invalidator 503, `redis` | Redis down | restore it |
| Invalidator 503, `source` | Kafka or NATS down | check brokers, credentials |
| Invalidator 503, `templates` | needs full identity | set it; restart invalidator |
| `Delete` returns `cache.ErrDeferred` | Redis down; queued | retry until `nil` |
| Hit ratio drops after recovery | L1 flushed | [expected](cache.md#redis-outages) |
