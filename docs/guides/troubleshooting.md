# Troubleshooting

Find the symptom, check the cause, apply the fix. Every error code is explained
in [errors.md](errors.md). Start with two commands that answer most questions:

```bash
wake status --config wake.yaml          # slot, confirmed LSN, lag, sinks, snapshots
wake check --config wake.yaml           # pre-flight checks with error codes
```

and the logs of `wake run` (JSON lines on standard error; `WAKE_LOG_FORMAT=text`
for text).

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
| Exit code 2, `wake: [config_invalid] ...` | A key is invalid, an unknown key (typo), an unset `source.dsn_env` variable or secret variable, or an unreadable TLS file | Run `wake config validate`; it lists every problem with its key ([configuration.md](configuration.md)) |
| `[replication_privilege]` | The role lacks the replication privilege, or `pg_hba.conf` rejects replication connections | Fix per provider: `ALTER ROLE ... REPLICATION`, `GRANT rds_replication`, provider flags ([getting-started.md](getting-started.md#per-provider)) |
| `[wal_level_not_logical]` | `wal_level` is not `logical` | Set it and restart Postgres; RDS `rds.logical_replication = 1`, Cloud SQL `cloudsql.logical_decoding = on` |
| `[no_free_slot]` | Every `max_replication_slots` slot exists | Drop an unused slot (`SELECT slot_name, active FROM pg_replication_slots`) or raise the setting |
| `[no_free_wal_sender]` | Every `max_wal_senders` connection is in use | Stop another replication client or raise the setting |
| `[slot_in_use]` naming a PID | Another Wake instance consumes the slot, or a crashed Wake's walsender is still attached | Stop the other instance. After a crash, wait up to `wal_sender_timeout` for Postgres to drop the old connection; the supervisor's restart then succeeds |
| `[state_mismatch]` | The slot exists but the state file is missing or belongs to another slot or Postgres system (a lost or foreign volume, a restored database) | Mount the right file, or start once with `--reinit-state` ([operations.md](operations.md#when-the-slot-or-the-state-file-is-lost)) |
| `[slot_missing]` | The state file exists but the slot is gone (dropped, restore, failover) | Changes since then are lost: `--reinit-state` creates a new slot, and **consumers must rebuild** |
| `[slot_invalidated]` | `wal_status = lost`: Postgres removed WAL the slot needed (`max_slot_wal_keep_size`) | `wake slot drop --yes`, then `--reinit-state`; **consumers must rebuild**. Size the limit and alert on lag |
| `[state_corrupt]` | The state file is not valid, or was written by a newer Wake | Restore a backup, or move it away and use `--reinit-state`. Never edit it by hand |
| `[status_interval_invalid]` | `stream.status_interval` is not below half of `wal_sender_timeout` | Lower the interval (default 10 s) or raise the timeout |
| `[postgres_version_unsupported]` | Postgres older than 14 | Upgrade Postgres |
| `[publication_privilege]` (managed mode) | The role lacks `CREATE` on the database or does not own a matched table or the publication | Grant the privileges, or switch to `existing` mode ([getting-started.md](getting-started.md#a-role-with-the-least-privileges-recommended)) |
| `[publication_conflict]` | A publication named `wake_<name>` exists and cannot be managed (for example `FOR ALL TABLES`) | Drop or rename it, or use `existing` mode |
| `[publication_missing]` (existing mode) | `source.publication.name` does not exist | Create the publication or fix the name |
| `[table_keyless]` | A captured table has no primary key and no replica-identity index | Add a key, or choose `tables.emit_unkeyed` or `tables.skip_unkeyed` ([operations.md](operations.md#keyless-tables)) |
| `[identifier_invalid]` | A schema or table name is not `[A-Za-z0-9_-]+`, a topic would exceed 249 characters, or two tables map to one name | Map it in `tables.name_map` or exclude it |
| `wake run` seems stuck before `wake started` | Creating the slot waits for transactions that are open on the source | Find and end long-running transactions (`pg_stat_activity`); `/readyz` reports `replication_not_streaming` meanwhile |
| Warning `wal_retention_unlimited` | `max_slot_wal_keep_size = -1` | Set a limit ([operations.md](operations.md#slot-sizing-and-wal-retention)) |
| Warning `publication_partition_root` | A partitioned table is published with `publish_via_partition_root = false`; events carry leaf partition names | Recreate the publication with `publish_via_partition_root = true` |

## Wake runs but is not ready or not delivering

| Symptom | Cause | Fix |
|---|---|---|
| `/readyz` 503, check `replication` failing (`replication_not_streaming`) | Start-up not finished, the reader stopped, or no activity for three `stream.status_interval`s | Read the logs; a lost connection makes Wake exit 1 and the supervisor restarts it |
| `/readyz` 503, check `sink` with `state: fatal` and code `event_too_large` | An event is larger than the sink's `max_event_bytes` or the destination's limit | The `reason` names table, key and LSN. Raise `max_event_bytes` and the destination limit (Kafka `message.max.bytes` and the topic's `max.message.bytes`, NATS `max_payload`), or exclude the column or table. A change in Wake's own configuration needs a restart |
| `/readyz` 503, sink fatal with code `sink_fatal` | Authentication or authorization failure, certificate error, unknown topic with `auto_create_topics: false`, missing NATS stream with `create_stream: false`, a message above the broker limit, a webhook poison batch with `dead_letter_dir` empty | Fix it at the destination (credentials, ACLs, create the topic or stream, fix the receiver); delivery resumes without a restart. See [sinks.md](sinks.md) |
| `wake status` shows a sink `retrying`, lag grows, `/readyz` still 200 | The destination is unreachable or answering 5xx or 429. Wake retries forever and stops reading; nothing is lost | Restore the destination. Check `wake_sink_retries_total{sink,reason}`. The `max_ack_age` alert fires after `lag.max_ack_age` |
| `wake status`: "admin endpoint unreachable" | Wake is not running, or `admin.listen` is `127.0.0.1:9093` inside a container so the port is not reachable | Use `--addr`, or set `admin.listen: 0.0.0.0:9093` in containers |
| Container restarts forever with `slot_in_use` | Two instances (an old and a new pod, or a second Compose project) | Run one instance per source; use a StatefulSet with one replica |
| Alert webhook never arrives | `lag.alert_webhook` unset, or the receiver answers non-2xx or is slow | Wake retries the notification at each poll; check the receiver and the Wake logs |

## Events are missing or look wrong

| Symptom | Cause | Fix |
|---|---|---|
| No events for a table | The table is not in the publication or matched by `tables.include`, is excluded, is keyless and skipped, or has an invalid name. Newly created tables are found only at the next `tables.refresh_interval` | `wake check`; look at `wake_tables_skipped_total{table,reason}`; in `existing` mode add the table to the publication; send `SIGHUP` to refresh at once |
| A new table appears but its existing rows are not delivered | `snapshot.added_tables: false`, or the snapshot is queued behind other runs, or the role lacks `SELECT` | `wake status` (`snapshot.queue`); grant `SELECT`; run `wake snapshot table <schema.table>` |
| Inserts of a table arrive but updates and deletes do not | The table is keyless and published insert-only (`emit_unkeyed`); UPDATE and DELETE of such tables are not captured | Add a primary key ([operations.md](operations.md#keyless-tables)) |
| Events of a partitioned table carry leaf partition names | Existing publication with `publish_via_partition_root = false` | Recreate it with `publish_via_partition_root = true` |
| A table is skipped with `identifier_invalid` in the log | Its name has characters outside `[A-Za-z0-9_-]` | Add it to `tables.name_map`, send `SIGHUP`. Changes before the mapping are not delivered ([operations.md](operations.md#changes-before-a-name-mapping)) |
| `before` of updates and deletes holds only the key columns | The table has the default replica identity | `ALTER TABLE ... REPLICA IDENTITY FULL` if you need the full old row |
| An update lacks a column in `after` and lists it in `unchanged_columns` | Postgres did not resend an unchanged large (TOAST) value | Keep your previous value of that column (`event.MergeUnchanged`), or use `REPLICA IDENTITY FULL`, which makes Wake copy the value ([consuming-events.md](consuming-events.md#unchanged-columns)) |
| A large integer is wrong in the consumer (ends in zeros, off by a few) | The consumer parsed an `int8` above 2^53 as a floating-point number | Parse integers exactly: Go `json.Number`, JavaScript `BigInt` ([consuming-events.md](consuming-events.md#integers-above-253)) |
| No schema-change event after a DDL | Postgres sends the new table definition only before the next write to the table | Write to the table; the event is emitted just before that write's event |
| Schema versions jumped to values around 1.7 billion | A `--reinit-state` seeded versions with the Unix time | Expected; versions only need to increase ([schema-changes.md](schema-changes.md)) |
| A `varchar(n)` change bumps the version but both column lists look equal | Known limitation: types are listed without modifiers | Rely on `schema_version` ([schema-changes.md](schema-changes.md#limitations)) |
| Kafka records with an empty value | Tombstones written after each delete (`kafka.tombstones`) | Skip records with a null value; set `kafka.tombstones: false` if unwanted |
| Events with `"key": null` | Keyless table under `tables.emit_unkeyed`, or a truncate (`t`) event | Consumers that need a key skip and count them |

## Duplicates and ordering

| Symptom | Cause | Fix |
|---|---|---|
| The same event `id` arrives twice | Delivery is at-least-once: after a crash or restart events after the last confirmed position are sent again | Deduplicate by `id`, or apply idempotent upserts ordered by `seq` ([delivery-guarantees.md](delivery-guarantees.md)) |
| Duplicates on NATS after a long outage | The JetStream duplicate window (`nats.duplicate_window`, default 2 m) bounds deduplication by `Nats-Msg-Id` | Deduplicate in the consumer; raise the window |
| `r` events for rows you already have | A snapshot ran again (added table, resume after a crash, `wake snapshot table`); snapshot replays are not deduplicated by id | Upsert by key ordered by `seq`; a stream event with a greater `seq` supersedes an `r` |
| An update arrives in another partition than earlier events of the same row | The primary key was changed: the event is partitioned by the new key | Compare `seq`; apply a `u` whose `before` key differs from `key` as delete of the old key plus upsert of the new ([consuming-events.md](consuming-events.md#key-changing-updates)) |
| Order seems broken for some webhook keys | Dead-lettered batches (`webhook.dead_letter_dir`) break per-key order for those keys | Inspect the dead-letter files, replay them manually, and order by `seq` |
| Lexicographic sort of `lsn` strings gives wrong order | LSNs are hexadecimal `X/X` with a variable number of digits | Compare LSNs numerically (`event.Compare`) |

## Snapshots

| Symptom | Cause | Fix |
|---|---|---|
| The initial snapshot is slow | Large tables; `snapshot.parallel_tables` bounds parallelism | Raise `parallel_tables` and `chunk_rows` within the source's capacity; watch `wake_snapshot_rows_total` |
| `wake run` exits with `snapshot_chunk_attempts` | A chunk failed 50 times in a row (permissions on the table, statement timeouts, a table rewrite, or constant visibility retries) | The log names the table and last error; fix it and restart, the run resumes from the last acknowledged position |
| `wake_snapshot_chunk_retries_total` grows | Chunks are discarded and re-read because of concurrent transactions on the table | Some retries are normal under heavy write load. Retries without progress end in `snapshot_chunk_attempts` after 50 consecutive failures of one chunk |
| A flood of `op: "r"` events | A re-snapshot: someone ran `wake snapshot table`, a table was re-added, or an initial snapshot restarted after `--reinit-state` | Check `wake_snapshot_requests_total{table}`; restrict `CONNECT` ([operations.md](operations.md#snapshot-requests-can-be-abused)) |
| Rows of a table with no key were skipped or sent twice by a snapshot | The table was rewritten between chunks (`VACUUM FULL`, `CLUSTER`, a rewriting `ALTER TABLE`); such tables are paged by `ctid` | Avoid rewrites during the snapshot and run `wake snapshot table` again ([operations](operations.md#snapshots)) |
| Rows of a newly attached partition are missing | The partition was pre-filled before `ATTACH PARTITION` and the snapshot had already passed | `wake snapshot table <parent>` |

## Disk and WAL

| Symptom | Cause | Fix |
|---|---|---|
| The source's WAL volume grows, `wake_slot_lag_bytes` is high | A sink is down or slow, Wake is stopped, or a long transaction pins the slot | [operations.md](operations.md#runbooks); set `max_slot_wal_keep_size` |
| WAL grows and there are no Wake metrics | Wake is stopped; the slot keeps pinning WAL | Start Wake or `wake slot drop --yes` if decommissioned; alert on `absent(wake_slot_lag_bytes)` |
| The Wake volume fills up | Spill files of a very large transaction, or a state or dead-letter directory on the same volume | Size the volume for the largest transaction; spill files are removed at start; move `dead_letter_dir` elsewhere |
| WAL grows on an idle database | Other databases of the cluster write WAL while the captured tables are idle | Check `heartbeat.interval` and that Wake runs; the heartbeat advances the confirmed position |

## Sinks

| Symptom | Cause | Fix |
|---|---|---|
| Kafka: sink fatal, unknown topic | `kafka.auto_create_topics: false` and the topic does not exist | Create `<prefix>.<schema>.<table>` (and `<prefix>._schema`), or enable auto-creation |
| Kafka: authentication or authorization errors | Wrong SASL credentials or ACLs; certificate not trusted | Check `sasl.*_env` variables, topic ACLs and `tls.ca_file`; the sink resumes when fixed |
| NATS: sink fatal, stream missing | `create_stream: false` and the stream does not exist, or another stream captures the subject | Create the stream on `<prefix>.>`, or enable `create_stream` |
| Webhook: sink fatal after repeated 4xx | The receiver rejects the batch (401, 403, 404, 400, 422); `dead_letter_dir` is empty | Fix the receiver (credentials, URL, payload handling); the batch is retried until accepted. Set `dead_letter_dir` only if you accept breaking per-key order for rejected batches |
| Webhook: signature verification fails | The body was parsed before hashing, the wrong secret, a clock skew above the tolerance, or a proxy changed the body | Verify the raw bytes with the secret named by `secret_env` ([sinks.md](sinks.md#verifying-signatures)) |
| Webhook: duplicate batches | A retry after a timeout or a 5xx that actually reached you | Deduplicate by event `id` or by `Wake-Delivery-Id` |

## Cache and invalidator

| Symptom | Cause | Fix |
|---|---|---|
| A cache serves stale values | The loader reads from a replica (a lagging replica can repopulate a stale value), or the namespace or bus channel of the application and the invalidator differ, or the key templates do not render the keys the application uses | Load from the primary; compare `cache_invalidator.namespace` and `bus_channel` with the application's `cache.New` namespace and `WithInvalidationBus`; check `wake_cdcsource_keys_deleted_total` ([cache.md](cache.md)) |
| Stale values after `TRUNCATE` | `t` events are not invalidated | Rely on TTL, or delete keys explicitly |
| Stale values after a column used in a template changed | The table does not have `REPLICA IDENTITY FULL`, so the old key cannot be rendered (`template_requires_full_identity`) | `ALTER TABLE ... REPLICA IDENTITY FULL` or reference key columns only |
| Invalidator `/readyz` 503, check `redis` | Redis is unreachable | Restore Redis; the invalidator retries |
| Invalidator `/readyz` 503, check `source` | Kafka or NATS unreachable, or the consumer is not running | Check brokers, credentials, the consumer group or durable |
| Invalidator `/readyz` 503, check `templates`, code `template_requires_full_identity` | A template references a non-key column of a table without `REPLICA IDENTITY FULL`; seen at start-up or at the first update or delete | Set `REPLICA IDENTITY FULL` and restart the invalidator (the condition stays raised until restart) |
| `Delete` returns `cache.ErrDeferred` | Redis is unreachable; the key was removed locally and queued for replay | Retry until `nil` (the invalidator does), restore Redis; other instances may serve their L1 for up to `WithDegradedL1TTL` |
| Cache hit ratio drops after Redis recovers | An epoch change flushed every L1 (the deferred-invalidation set overflowed, or the bus reconnected) | Expected; see `wake_cache_epoch_changes_total` ([cache.md](cache.md#redis-outages)) |
