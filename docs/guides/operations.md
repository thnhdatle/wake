# Operations

How to deploy, monitor, upgrade and decommission Wake, and what to do when
something is lost. Read [delivery-guarantees.md](delivery-guarantees.md) for what
Wake promises and [troubleshooting.md](troubleshooting.md) when something is
wrong now.

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

All commands accept `--config <file>` (or `WAKE_CONFIG`).

| Command | What it does |
|---|---|
| `wake run [--reinit-state]` | Runs the service: pre-flight checks, slot and publication setup, snapshot, streaming, admin server. Exits 0 after SIGTERM or SIGINT, 1 when a component fails or the replication connection is lost. `--reinit-state` is explained in [When the slot or the state file is lost](#when-the-slot-or-the-state-file-is-lost) |
| `wake check` | Runs the pre-flight checks without changing anything: server version, `wal_level`, replication privilege, free slots and WAL senders, publications, `CREATE` and table ownership (managed) or `publish_via_partition_root` (existing), keyless tables, identifiers, topic lengths, WAL retention. Prints `OK`, `WARN` or `FAIL` per check with the error code; exits 1 on a failure |
| `wake status [--addr host:port]` | Prints `GET /v1/status` of the running instance. If the admin endpoint is unreachable it says so and reads the state file and `pg_replication_slots` directly |
| `wake snapshot table <schema.table>` | Requests a snapshot of one table; see [Snapshots](#snapshots) |
| `wake slot drop --yes` | Drops the replication slot `wake_<source.name>`. Without `--yes` it exits 2 (`confirmation_required`) |
| `wake publication drop --yes` | Drops the managed publications `wake_<name>` and `wake_<name>_insert`. Never touches an `existing`-mode publication |
| `wake config validate` | Validates the configuration, listing every problem; exits 2 on errors. With a configured `cache_invalidator` section and `validate_dsn_env` set, it also checks the key templates against the database |
| `wake config print [--redact]` | Prints the effective configuration (file, environment, defaults). Never prints secret values; `--redact` also masks passwords in URLs |
| `wake cache-invalidator` | Runs the CDC-driven cache invalidator ([cache.md](cache.md)) |
| `wake healthcheck [--addr host:port]` | Exits 0 when `/readyz` returns 200; used by container health checks |
| `wake version` | Prints version information |

Exit codes: 0 success, 1 runtime failure or failed pre-flight check, 2
configuration error or a destructive command without `--yes`
([errors.md](errors.md#exit-codes)).

### Admin endpoints

`admin.listen` (default `127.0.0.1:9093`) serves:

| Path | Meaning |
|---|---|
| `/healthz` | The process is alive |
| `/readyz` | 200 when the configuration is loaded, the replication connection is streaming (with activity within three `stream.status_interval`s) and no sink is blocked by a non-retriable error. Otherwise 503 with `checks` naming each failing condition. A sink that is merely retrying (broker down, 5xx) keeps the service ready: backpressure is the designed behaviour, and the `max_ack_age` alert covers it |
| `/metrics` | Prometheus metrics |
| `/v1/status` | Slot, confirmed LSN, lag, snapshot runs and progress, sinks, schema versions |
| `/v1/version` | Build information |

The OpenAPI description is [api/openapi.yaml](../../api/openapi.yaml). The
endpoints have no authentication; bind them to a private interface or protect
them with a network policy.

## Deploying

Run exactly one Wake process per source. A second one fails with
`slot_in_use` while the first holds the slot, but two instances must never be
started on purpose: Wake has no leader election in v0.1. Run Wake under a
supervisor that restarts it when it exits; a restart is always safe.

### Docker Compose

[deploy/docker-compose.yml](../../deploy/docker-compose.yml) is the local stack
used by the [quickstart](quickstart.md): Postgres 17, Kafka, NATS, Redis and
Wake. The Wake service

- runs `run --config /etc/wake/config.yaml` with a read-only root file system;
- gets the DSN through `WAKE_SOURCE_DSN` and logs as text (`WAKE_LOG_FORMAT=text`);
- mounts the named volume `wake-data` at `/var/lib/wake` for the state file
  (`state.path: /var/lib/wake/wake-state.json`) and the spill directory
  (`stream.spill_dir: /var/lib/wake/spill`), as
  [deploy/config.compose.yaml](../../deploy/config.compose.yaml) configures;
- listens on `0.0.0.0:9093` inside the container and publishes it on the
  loopback interface of the host;
- uses `wake healthcheck` as its health check.

For a long-lived Compose deployment add `restart: unless-stopped` and replace the
development credentials. Keep the volume across `docker compose down` (do not
pass `-v`): the state file belongs to the slot in Postgres.

### Kubernetes

The manifests are in [deploy/kubernetes](../../deploy/kubernetes) (StatefulSet,
ConfigMap, Services, example Secret, kustomization); apply them with
`kubectl apply -k deploy/kubernetes` after creating the `wake-source` Secret.
Size the PersistentVolumeClaim (default 1 Gi) for the largest transaction you
expect, because transactions above `stream.max_tx_bytes` spill to it.

Wake runs as a single-replica StatefulSet with a PersistentVolumeClaim for the
state file and the spill directory. A StatefulSet, not a Deployment, because
there is exactly one slot consumer and its state must survive rescheduling; a
rolling update stops the old pod before the new one starts, so two consumers
never overlap. The reference manifest:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: wake-config
data:
  config.yaml: |
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
          brokers: ["kafka:9092"]
          topic_prefix: cdc
    admin:
      listen: 0.0.0.0:9093
---
apiVersion: v1
kind: Service
metadata:
  name: wake
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: wake
  ports:
    - name: admin
      port: 9093
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: wake
spec:
  serviceName: wake
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: wake
  template:
    metadata:
      labels:
        app.kubernetes.io/name: wake
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9093"
    spec:
      terminationGracePeriodSeconds: 60
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: wake
          image: ghcr.io/thnhdatle/wake:v0.1.0
          args: ["run", "--config", "/etc/wake/config.yaml"]
          env:
            - name: WAKE_SOURCE_DSN
              valueFrom:
                secretKeyRef:
                  name: wake-source
                  key: dsn
          ports:
            - name: admin
              containerPort: 9093
          livenessProbe:
            httpGet: {path: /healthz, port: admin}
            periodSeconds: 10
          readinessProbe:
            httpGet: {path: /readyz, port: admin}
            periodSeconds: 10
          resources:
            requests: {cpu: 250m, memory: 512Mi}
            limits: {memory: 1Gi}
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities: {drop: ["ALL"]}
          volumeMounts:
            - {name: data, mountPath: /var/lib/wake}
            - {name: config, mountPath: /etc/wake, readOnly: true}
      volumes:
        - name: config
          configMap:
            name: wake-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
```

Notes:

- The volume holds `state.path` and `stream.spill_dir`. Size it for the largest
  transaction you expect (default 1 Gi); spill files are deleted at every start.
  If the PersistentVolume is lost, read [When the slot or the state file is
  lost](#when-the-slot-or-the-state-file-is-lost).
- Use `/healthz` for liveness and `/readyz` for readiness. Do not use `/readyz`
  as a liveness probe: a blocked sink (for example an oversized event) is fixed at
  the destination, and restarting Wake does not help.
- The grace period covers the flush of the sinks on shutdown (Kafka waits up to
  ten seconds).
- Restrict access to port 9093 with a NetworkPolicy; the admin endpoints are not
  authenticated.
- Memory: plan for about `stream.max_tx_bytes` (default 256 MiB) for the
  transaction being assembled, plus the sink buffers (up to 64 MiB for each Kafka
  or NATS sink), plus runtime overhead; leave headroom above that for the
  container limit, or lower `stream.max_tx_bytes` and rely on spill to disk.
- Run `wake check` from a one-off pod or a workstation before the first rollout.

## Managed Postgres notes

The replication privilege is verified by opening a replication connection and
running `IDENTIFY_SYSTEM`; a failure is reported as `replication_privilege` with
the fix for your provider ([errors.md](errors.md#replication_privilege)).

**Amazon RDS and Aurora.**

- Set `rds.logical_replication = 1` in the (cluster) parameter group and reboot.
  This sets `wal_level = logical`.
- Grant the role `rds_replication` (`GRANT rds_replication TO wake;`). The
  `REPLICATION` attribute is not used on RDS.
- Raise `max_replication_slots` and `max_wal_senders` if they are exhausted
  (`no_free_slot`, `no_free_wal_sender`).
- Set `max_slot_wal_keep_size` in the parameter group; the default is unlimited,
  and an unlimited slot can fill the instance's storage when Wake is down. See
  [Slot sizing](#slot-sizing-and-wal-retention).
- In `managed` mode the role must own every matched table or be a member of the
  owning role (`GRANT app_owner TO wake;`), and needs `CREATE` on the database.
  If your tables are owned by another role and you do not want to grant
  membership, use `existing` mode with a DBA-created publication.
- Do not expect a logical slot to survive a failover to another instance unless
  your engine version documents that it does; assume the
  [lost-slot](#when-the-slot-or-the-state-file-is-lost) runbook applies.

**Google Cloud SQL.** Set the flag `cloudsql.logical_decoding = on` (this
restarts the instance) and give the role the replication attribute
(`ALTER ROLE wake REPLICATION;`). Maintenance restarts drop the replication
connection; Wake exits and its supervisor restarts it.

**Azure Database for PostgreSQL.** Set the server parameter `wal_level` to
`logical`, restart the server and `ALTER ROLE wake REPLICATION;`.

**Managed publications and ownership.** `managed` mode creates and alters
`wake_<name>` itself. It needs `CREATE` on the database, ownership (or
membership in the owning role) of every matched table, and ownership of
publications it finds with its own name; `wake check` verifies this
(`publication_privilege`). On platforms where your application's migrations run
as a different role than Wake, `existing` mode avoids all of it
([getting-started.md](getting-started.md#a-role-with-the-least-privileges-recommended)).

**Failover slots (Postgres 17).** A logical slot normally exists only on the
primary. After a failover to a physical standby the slot is gone, and `wake run`
refuses to start with `slot_missing`. Postgres 17 can synchronise slots flagged
`failover` to a standby (`sync_replication_slots = on` and `hot_standby_feedback
= on` on the standby, `primary_slot_name` and a `dbname` in its `primary_conninfo`,
and `synchronized_standby_slots` on the primary). Wake itself creates its slot
without the `failover` flag, so it is not synchronised by default. If you need
this, create the slot yourself before the first start:

```sql
SELECT pg_create_logical_replication_slot('wake_app', 'pgoutput', false, false, true);
```

Wake then finds a slot but no state file and refuses with `state_mismatch`;
starting once with `wake run --reinit-state` adopts the slot at its confirmed
position. Because that start performs no initial snapshot, snapshot the existing
rows afterwards with `wake snapshot table <schema.table>` for each table. This
procedure is not part of Wake's automated tests; rehearse it on a copy. After a
promotion the standby's copy of the slot may be behind the position Wake last
confirmed; Wake then re-delivers events (delivery is at-least-once), it does not
lose them. The physical standby shares the primary's system identifier, so the
state file still matches.

## Slot sizing and WAL retention

A replication slot makes Postgres keep every WAL segment the slot has not yet
confirmed. While Wake runs and its sinks keep up, that is a few segments. When a
sink is down, Wake is stopped, or a transaction is stuck, the retained WAL grows
until the disk of the primary fills, and a full WAL disk stops the primary. Two
settings bound it:

- `max_slot_wal_keep_size` on Postgres: the most WAL a slot may pin. When the slot
  falls further behind, Postgres invalidates it (`wal_status = lost`) and Wake
  cannot continue: `slot_invalidated`, and consumers must rebuild. Unlimited (`-1`,
  the default) means the disk is the limit; Wake logs `wal_retention_unlimited` at
  startup and `wake check` prints the value and the current slot lag.
- Wake's lag alerts (`lag.warn_bytes`, `lag.critical_bytes`, `lag.max_ack_age`),
  which tell you before that happens.

**Sizing.** Choose the longest outage you want to survive without rebuilding
consumers, estimate the WAL your database writes at its busiest, and multiply:

```text
max_slot_wal_keep_size  >=  peak WAL bytes per second  x  longest tolerated outage in seconds  x  1.5
```

Measure the WAL rate from `pg_stat_wal.wal_bytes` (or by sampling
`pg_current_wal_lsn()` and `pg_wal_lsn_diff`) over a busy period. The result must
fit in the free space of the WAL volume with room to spare for checkpoints and
other consumers. Then set the alerts below it, for example `lag.warn_bytes` at a
quarter of the limit and `lag.critical_bytes` at three quarters. The defaults
(1 GiB and 10 GiB) assume a limit well above 10 GiB.

Things that pin WAL beyond sink outages:

- **Long-running transactions** on the source: Postgres must keep the WAL back to
  the oldest transaction that was running when the slot last advanced.
  `wake_slot_lag_bytes` is measured from `confirmed_flush_lsn`, so also watch
  `pg_replication_slots.restart_lsn` and `safe_wal_size`.
- **Idle captured tables**: the confirmed position advances only with traffic.
  Wake emits a `wake.heartbeat` message when no captured transaction was read for
  `heartbeat.interval` (1 minute), which lets the position advance and WAL of
  other databases in the cluster be recycled.
- **A stopped Wake**: the slot keeps pinning WAL while nothing consumes it, and
  Wake's own metrics are gone. Alert on `absent(wake_slot_lag_bytes)` and on
  `up{job="wake"} == 0`, and use the Postgres-side rule below, which works
  without Wake.

`wake status` shows `slot.wal_status` (`reserved`, `extended`, `unreserved`,
`lost`) and `slot.lag_bytes`. A status of `unreserved` means some WAL the slot needs will be removed at the next
checkpoint: fix the cause immediately.

## Snapshots

A snapshot copies the existing rows of a table to the sinks as `op: "r"` events.
There are three kinds:

- **Initial snapshot.** On the first start (when Wake creates the slot) with
  `snapshot.mode: initial`. All captured tables are copied at the exact position
  of the slot's creation, by up to `snapshot.parallel_tables` parallel workers in
  chunks of `snapshot.chunk_rows` rows. Stream events are held back until the copy
  has been read; reading pauses once `stream.max_tx_bytes` of events are held.
- **Table snapshots.** For tables added to a running pipeline
  (`snapshot.added_tables: true`), for a resumed initial snapshot, and for
  `wake snapshot table`. Wake copies one table at a time, chunk by chunk,
  interleaved with the live stream by watermarks, without pausing it or breaking
  per-key order.
- **Resume.** A crash during the initial snapshot keeps the slot; on restart the
  stream is released at once and every unfinished table continues, one at a time,
  from its last acknowledged key. Rows are never lost; some chunks may be sent
  again.

Progress is in `wake status` (`snapshot.runs`, `snapshot.queue`) and the metric
`wake_snapshot_rows_total{table}` (rows acknowledged by every sink). To request a
snapshot of one table, for example after consumers lost their copy:

```bash
wake snapshot table public.users
```

The command connects with the source DSN and writes a transactional logical
message into the WAL; the running Wake (or the next one to start) reads it from
the stream and queues the table. Requests for tables that are not captured are
logged and ignored. Runs execute one after the other, each with a new `run_id`.

Consumers must treat `r` events as upserts ordered by `seq`: a snapshot replays
current rows and is **not deduplicated by id** (a re-executed chunk gets new ids).
See [consuming-events.md](consuming-events.md). The snapshot reads the table with
the Wake role's `SELECT` privilege.

Snapshot limits to know about:

- Tables with neither a primary key nor a replica-identity index (`REPLICA
  IDENTITY FULL` without a key, or keyless tables) are paged by `ctid`. A table
  rewrite between two chunks (`VACUUM FULL`, `CLUSTER`, a rewriting `ALTER TABLE`)
  can make a snapshot skip or repeat rows. Avoid rewrites while such a table is
  being snapshotted.
- Rows that already sit in a partition attached to a partitioned table while a
  snapshot of that table is running are missed by it. Re-run
  `wake snapshot table` after attaching a pre-filled partition.

### Snapshot requests can be abused

`wake snapshot table` needs nothing but access to the database: it calls
`pg_logical_emit_message`, which any role that can connect may call. A role with
`CONNECT` can therefore trigger a re-snapshot of any captured table and flood your
sinks and consumers with `r` events (and the cache invalidator, if
`invalidate_on_snapshot` is on). Mitigate it:

- restrict `CONNECT` on the database to the roles that need it
  (`REVOKE CONNECT ON DATABASE app FROM PUBLIC;` and grant it explicitly);
- alert on the rate of `wake_snapshot_requests_total{table}` (requests for
  uncaptured tables are counted under `table="_other"`), see the alert rules
  below.

## Keyless tables

A keyless table has replica identity `NOTHING`, or `DEFAULT` without a primary
key. Postgres rejects the application's own UPDATE and DELETE on such a table when
the publication publishes them, so Wake never adds a keyless table to its managed
publication `wake_<name>`. What happens instead depends on the options:

| Option | Behaviour |
|---|---|
| neither set (default) | Keyless tables are logged at error (`table_keyless`) and listed by `wake check`; startup fails. At runtime refreshes, they are skipped |
| `tables.skip_unkeyed: true` | Keyless tables are skipped: no events |
| `tables.emit_unkeyed: true` | Their **inserts** are emitted with `key: null`, in both modes |

With `emit_unkeyed` in `managed` mode, matched keyless tables go into a second
managed publication, `wake_<name>_insert`, that publishes inserts only
(`publish = 'insert'`), so the application's UPDATE and DELETE keep working. A
table moves between the two publications at the next refresh when it gains or
loses a key. `wake publication drop` removes both. In `existing` mode, keyless
tables already in your publication are accepted when Postgres allows them there,
which it does only if the publication does not publish their updates and deletes
(for example `publish = 'insert'`).

**UPDATE and DELETE of insert-only tables are not captured.** Only inserts of
those tables produce events, and those events have `key: null`, are not ordered
per key (they are partitioned by `id`) and cannot be upserted by key. Give the
table a primary key if you need its updates and deletes.

## Changes before a name mapping

Schema, table and prefix names must match `[A-Za-z0-9_-]+`. A table whose name
does not is rejected at startup (`identifier_invalid`), or, when it appears while
Wake runs, skipped with an error log. After you map it in `tables.name_map` and
send `SIGHUP` (or restart), Wake treats it as a newly detected table: it is
snapshotted (`snapshot.added_tables`) and streamed from then on. **Changes made
before the mapping took effect are not delivered as events**; the snapshot
delivers the rows as they are at that moment, which includes the effect of those
changes except for rows deleted in the meantime, whose deletion is never
announced.

## State file

`state.path` holds what the slot does not: the slot name and the Postgres system
identifier the file belongs to, the consistent point of the initial snapshot,
the schema version, replica identity and columns of every table, the snapshot
runs with their queue and per-table progress, and the run counter that keeps
`run_id` stable. It is JSON, written atomically (temporary file, `fsync`,
rename), so a crash leaves either the old or the new file. It is small and
rewritten on progress, schema and queue changes.

At every start Wake checks that the file and the slot belong together and refuses
to run when they do not (`state_mismatch`, `slot_missing`, `state_corrupt`).

**Backup.** Copying the file while Wake runs is safe. Back it up regularly (a
volume snapshot, or a periodic copy to object storage) and always before a Wake
upgrade. A restored copy is correct as long as it belongs to the same slot; an
old copy repeats some work (snapshot progress is older, so chunks are sent
again) and, if tables changed shape since the copy was taken, schema versions
may be re-issued with numbers consumers have already seen. When in doubt prefer
`--reinit-state` over restoring an old copy: it reseeds versions so that they
never decrease ([schema-changes.md](schema-changes.md)).

## When the slot or the state file is lost

The slot (in Postgres) and the state file (on Wake's volume) are two halves of one
position. When they disagree Wake stops instead of guessing.

| Situation | Error | Meaning |
|---|---|---|
| The slot exists, the state file is missing or belongs to another slot or system | `state_mismatch` | A lost or foreign volume, or a restored database with a new system identifier. Streaming can resume from the slot, but snapshot progress and schema versions are unknown |
| The state file exists, the slot does not | `slot_missing` | The slot was dropped, or the database was restored or failed over without it. **Changes since the slot disappeared are gone** |
| The slot's `wal_status` is `lost` | `slot_invalidated` | Postgres removed WAL the slot still needed. **Events between the confirmed position and now are gone** |

`wake run --reinit-state` overrides the first two:

- **`state_mismatch`**: Wake adopts the existing slot; streaming continues from the
  slot's confirmed position. Schema versions are reseeded to
  `max(stored version, current Unix time in seconds)` so they never decrease
  (consumers will see versions that look like timestamps,
  [schema-changes.md](schema-changes.md)). Snapshot progress is discarded and
  unfinished runs are logged; rerun `wake snapshot table` for the tables that were
  mid-snapshot. No events are lost.
- **`slot_missing`**: Wake creates a new slot as on first start, with an initial
  snapshot according to `snapshot.mode`. **Changes between the loss of the old slot
  and the new consistent point, deletes included, are never delivered.** A `d`
  event that never comes leaves the deleted row in every downstream store.
  **Consumers must rebuild their state.**

To rebuild after `slot_missing` or `slot_invalidated`:

1. Stop or pause the consumers, or point them at a fresh destination.
2. Discard the derived state: truncate the downstream tables and indexes, flush
   the caches, and recreate the Kafka topics or JetStream stream (compacted or
   retained history would otherwise keep rows that were deleted in the gap).
3. For `slot_invalidated`, drop the dead slot first: `wake slot drop --yes`.
4. Start Wake with `--reinit-state` and `snapshot.mode: initial`; it emits every
   current row as an `r` event followed by the live stream.
5. Let the consumers read from the beginning, applying events as idempotent
   upserts ordered by `seq` ([consuming-events.md](consuming-events.md)).

A state file written before the slot was created, without a consistent point
(slot creation never completed), is not an error: Wake creates the slot as on
first start.

## Upgrades

### Wake

1. Read the CHANGELOG for the target version.
2. Back up the state file.
3. Stop Wake with SIGTERM and wait for the clean exit; then start the new
   version with the same configuration and volume (on Kubernetes, change the
   image of the StatefulSet).
4. Check `wake status` and `/readyz`.

A restart re-delivers the events that were not yet acknowledged, so consumers must
already be idempotent. A newer Wake reads the state file of an older one; an older
Wake refuses a file written by a newer one (`state_corrupt`), so a downgrade
needs the backup taken in step 2.

### Postgres

**Supported versions.** Postgres 14, 15, 16 and 17. Wake's CI runs the
integration suite against the latest minor release of each of these majors at the
time of the Wake release. The only version gate in the code is the major:
`wake run` and `wake check` refuse a server older than 14 with
`postgres_version_unsupported`. Run the current minor of your major; fixes to
logical decoding are delivered in minor releases.

**Minor upgrades** (restart of the same major). The replication connection drops,
Wake exits with status 1 and its supervisor restarts it once Postgres is back.
Nothing is lost.

**Major upgrades.** The new cluster has a new system identifier, and `pg_upgrade`
carries logical slots over only when upgrading from Postgres 17 or later. So
`wake run` stops at the first start on the new cluster, with `slot_missing` (no
slot) or `state_mismatch` (slot carried over, new system identifier), and both are
resolved with `--reinit-state`. A drain procedure that loses nothing:

1. Stop the writers (or take the application offline).
2. Wait until `wake status` shows `lag_bytes` near 0 and `confirmed_lsn` equal to
   the current WAL position, and no sink is retrying.
3. Stop Wake and upgrade Postgres.
4. Set `snapshot.mode: never`, because nothing changed since the last delivered
   event, and start `wake run --reinit-state`. Wake creates the slot on the new
   cluster (or adopts the carried-over one) and streams from there; schema
   versions are reseeded.
5. Restart the writers.

If you cannot stop the writers, plan for the consumer rebuild described above.

## Metrics and alerts

### Metrics

`/metrics` serves these metrics, besides the Go runtime and process collectors.
Labels derived from user data (`table`) are capped at 50 distinct values; further
values are counted under `other`.

**Slot and lag**

| Metric | Type | Meaning |
|---|---|---|
| `wake_slot_lag_bytes` | gauge | WAL bytes between `pg_current_wal_lsn()` and the slot's `confirmed_flush_lsn` |
| `wake_slot_wal_status` | gauge | `reserved` 0, `extended` 1, `unreserved` 2, `lost` 3, unknown -1 (before the first poll) |
| `wake_oldest_unacked_age_seconds` | gauge | Age of the oldest event Wake has read and not yet seen acknowledged by every sink it was routed to; 0 when none is pending |
| `wake_lag_poll_errors_total` | counter | Failed polls of the slot (the lag gauges are stale while this grows) |
| `wake_alert_active{rule}` | gauge | 1 while the alert rule is firing: `slot_lag_warn`, `slot_lag_critical`, `max_ack_age`, `sink_fatal` |
| `wake_end_to_end_latency_seconds{sink}` | histogram | Commit time (`ts_ms`) to sink acknowledgement, on the host clock |

**Sinks**

| Metric | Type | Meaning |
|---|---|---|
| `wake_sink_events_sent_total{sink}` | counter | Events handed to the destination; events re-sent after an error count again |
| `wake_sink_bytes_sent_total{sink}` | counter | Bytes handed to the destination, same rule |
| `wake_sink_events_acked_total{sink}` | counter | Events the destination acknowledged |
| `wake_sink_retries_total{sink,reason}` | counter | Delivery attempts repeated after an error, by reason. Kafka: `network`, `broker`, `unknown_topic`, `message_too_large`, `event_too_large`, `auth`, `rejected`, `other`. NATS: `network`, `server`, `unknown_stream`, `message_too_large`, `event_too_large`, `auth`, `rejected`, `other`. Webhook: `server_error`, `rate_limited`, `timeout`, `network`, `poison`, `dead_letter`, `internal` |
| `wake_sink_dead_lettered_total{sink}` | counter | Events written to the webhook dead-letter directory (and acknowledged without delivery) |
| `wake_sink_inflight{sink}` | gauge | Events sent and not yet acknowledged |
| `wake_sink_send_duration_seconds{sink}` | histogram | Duration of one delivery attempt |

**Stream and snapshots**

| Metric | Type | Meaning |
|---|---|---|
| `wake_spilled_transactions_total` | counter | Transactions moved to a spill file |
| `wake_spilled_bytes_total` | counter | Bytes moved to spill files |
| `wake_spilled_transaction_size_bytes` | histogram | Size of each spilled transaction |
| `wake_unknown_types_total{type}` | counter | Column types outside the type mapping, counted once per type ([type-mapping.md](type-mapping.md)) |
| `wake_unknown_messages_total{type}` | counter | pgoutput message types outside protocol version 1, counted once per type and skipped |
| `wake_truncate_skipped_total{table}` | counter | Truncates not emitted because `events.emit_truncate` is off |
| `wake_tables_skipped_total{table,reason}` | counter | Published tables that are not captured, once per table and reason (`not_captured`, `identifier_invalid`, `table_keyless`) |
| `wake_snapshot_rows_total{table}` | counter | Snapshot rows that every sink acknowledged |
| `wake_snapshot_requests_total{table}` | counter | Snapshot requests; `table="_other"` for uncaptured tables |
| `wake_snapshot_chunk_retries_total{table}` | counter | Snapshot chunks discarded and read again |
| `wake_build_info{version,commit,go_version}` | gauge | Always 1 |

The cache library and the invalidator add `wake_cache_*` and `wake_cdcsource_*`
metrics, listed in [cache.md](cache.md#metrics).

### Alerts

Wake evaluates four rules every `lag.interval`. When a rule changes state it logs,
sets `wake_alert_active{rule}` and, if `lag.alert_webhook` is set, POSTs once per
change:

```json
{"rule":"slot_lag_warn","state":"firing","value":9437184,"threshold":8388608,"ts":"2026-10-18T12:00:00Z"}
```

`state` is `firing` or `resolved`. A notification that fails is retried at the
next poll; the receiver sees each state change once and never two notifications
with the same state in a row.

| Rule | Fires while | Threshold |
|---|---|---|
| `slot_lag_warn` | slot lag in bytes is above the threshold | `lag.warn_bytes` |
| `slot_lag_critical` | slot lag in bytes is above the threshold | `lag.critical_bytes` |
| `max_ack_age` | the oldest unacknowledged event is older than the threshold (seconds) | `lag.max_ack_age` |
| `sink_fatal` | any sink is blocked by a non-retriable error (`event_too_large`, `sink_fatal`); the value is the number of such sinks | 0 |

`max_ack_age` is the time budget for the unbounded retries of the sinks: a sink
retries retriable errors forever, so this alert tells you that it has been doing
so for too long. A long initial snapshot does not trip it.

### Prometheus rules

`deploy/prometheus/alerts.yml` carries these rules (plus a few for the cache
library and the invalidator, and a chunk-retry rule for snapshots); the core
ones are reproduced here:

```yaml
groups:
  - name: wake
    rules:
      # The lag expressions repeat the defaults of lag.warn_bytes (1 GiB),
      # lag.critical_bytes (10 GiB) and lag.max_ack_age (300 s): keep them in
      # step with your Wake configuration.
      - alert: WakeSlotLagWarn
        expr: wake_slot_lag_bytes > 1073741824
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: Replication slot lag above 1 GiB (lag.warn_bytes)
      - alert: WakeSlotLagCritical
        expr: wake_slot_lag_bytes > 10737418240
        labels: {severity: critical}
        annotations:
          summary: Replication slot lag above 10 GiB (lag.critical_bytes); WAL is piling up
      - alert: WakeAckAgeHigh
        expr: wake_oldest_unacked_age_seconds > 300
        labels: {severity: warning}
        annotations:
          summary: Events are waiting for a sink longer than 300 s (lag.max_ack_age)
      - alert: WakeSinkFatal
        expr: wake_alert_active{rule="sink_fatal"} == 1
        labels: {severity: critical}
        annotations:
          summary: A sink is blocked by a non-retriable error; delivery is stopped
      - alert: WakeSlotWalStatus
        expr: wake_slot_wal_status >= 2
        labels: {severity: critical}
        annotations:
          summary: Slot WAL status is unreserved or lost
      - alert: WakeLagPollFailing
        expr: increase(wake_lag_poll_errors_total[10m]) > 0
        labels: {severity: warning}
        annotations:
          summary: Wake cannot read the slot status; lag metrics are stale
      - alert: WakeDown
        expr: up{job="wake"} == 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: Wake is not reachable; a stopped Wake keeps the slot pinning WAL
      - alert: WakeMetricsAbsent
        expr: absent(wake_slot_lag_bytes)
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: wake_slot_lag_bytes is absent; Wake is down or not scraped
      - alert: WakeSnapshotRequests
        expr: sum(rate(wake_snapshot_requests_total[5m])) > 0.05
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: Unusual number of snapshot requests; check who can connect to the database
  - name: wake-postgres-optional
    rules:
      - alert: WakeSlotLagWithoutWake
        # Needs postgres_exporter; works when Wake itself is down. Metric and label
        # names depend on the exporter version and collectors: adjust to yours.
        expr: pg_replication_slots_pg_wal_lsn_diff{slot_name=~"wake_.*"} > 10 * 1024 * 1024 * 1024
        for: 10m
        labels: {severity: critical}
        annotations:
          summary: Slot retains more than 10 GiB of WAL (measured by Postgres, not by Wake)
```

The file also has `WakeAlertActive`, which reports any other `wake_alert_active`
rule, so a rule added later does not go unnoticed.

Adjust the thresholds to your `lag.*` settings, and the last rule to your
`max_slot_wal_keep_size`. A stopped Wake silences its own metrics while the slot
keeps pinning WAL, which is why `WakeDown`, `WakeMetricsAbsent` and the
Postgres-side rule exist.

## Runbooks

### Decommission: `wake slot drop`

Never just stop Wake and forget it: the slot stays in Postgres and keeps pinning
WAL until the disk is full. To retire a pipeline:

1. Stop Wake (SIGTERM, or scale the StatefulSet to 0). If consumers still need the
   last events, wait until `wake status` shows no lag first.
2. Drop the slot:

   ```bash
   wake slot drop --yes --config wake.yaml
   ```

   This releases the WAL the slot retained; changes not yet delivered are lost.
   It reports `slot_in_use` (with the PID) while a process is still attached;
   wait for the old walsender to disappear (up to `wal_sender_timeout`) and retry.
   Without `--yes` the command exits 2.
3. In `managed` mode drop the publications: `wake publication drop --yes`. An
   `existing`-mode publication is yours; drop it with `DROP PUBLICATION`.
4. Check `SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'wake_app';`
   returns no row.
5. Delete the state file and the volume, revoke the role, and remove the alert
   rules for the source.

The state file refers to a slot that no longer exists: if you later reuse the
same `source.name` with the old file, `wake run` refuses with `slot_missing`.
Delete the file (a fresh start then takes an initial snapshot) or pass
`--reinit-state` knowing what it implies.

### A sink is blocked

`/readyz` returns 503, `wake_alert_active{rule="sink_fatal"}` is 1 and `wake status`
shows the sink with `state: fatal`, a `code` (`event_too_large` or `sink_fatal`)
and a `reason`. Wake has stopped reading and keeps retrying; the confirmed
position does not pass the blocked event, so WAL accumulates. Fix the cause named in
the reason (see [errors.md](errors.md#sinks-and-readiness) and
[sinks.md](sinks.md)); delivery resumes without a restart, except for fixes in
Wake's own configuration (`max_event_bytes`, excluding a column or table), which
need a restart.

### Slot lag is growing

1. `wake status`: is a sink `retrying`? Check the destination (broker, receiver).
2. `wake_oldest_unacked_age_seconds` and `wake_sink_retries_total{reason}` tell
   which sink and why.
3. If no sink is stuck, look for a long-running transaction on the source
   (`pg_stat_activity`) and for an idle source (the heartbeat should keep the
   position moving every `heartbeat.interval`).
4. If you cannot fix it in time and the slot approaches `max_slot_wal_keep_size`,
   decide between letting Postgres invalidate the slot (then rebuild consumers) and
   adding disk.
