# Configuration

Wake is configured with one YAML file, optionally overridden by environment
variables. This page describes every key. The same keys, with comments and
defaults, are in [deploy/config.example.yaml](../../deploy/config.example.yaml).

- [How configuration is read](#how-configuration-is-read)
- [source](#source)
- [tables](#tables)
- [events](#events)
- [snapshot](#snapshot)
- [stream](#stream)
- [state](#state)
- [sinks](#sinks)
- [lag](#lag)
- [heartbeat](#heartbeat)
- [admin](#admin)
- [cache_invalidator](#cache_invalidator)

## How configuration is read

**File.** Pass the file with `--config <path>` or the environment variable
`WAKE_CONFIG`. Without a file, Wake uses the defaults and the environment, which
is not enough to start: `source.name` and at least one sink are required. Unknown
keys are errors, so a typo never silently falls back to a default.

**Precedence**, highest first:

1. command-line flags (the global `--config` and the flags of each command);
2. environment variables `WAKE_<PATH>`;
3. the YAML file;
4. built-in defaults.

Wake has no per-key command-line flags in v0.1: every key is set through the
file or the environment.

**Environment variables.** Any scalar key outside the `sinks` list can be
overridden by `WAKE_` followed by its dotted path in upper case, with dots and
hyphens replaced by underscores:

| Key | Environment variable |
|---|---|
| `admin.listen` | `WAKE_ADMIN_LISTEN` |
| `stream.status_interval` | `WAKE_STREAM_STATUS_INTERVAL` |
| `source.publication.mode` | `WAKE_SOURCE_PUBLICATION_MODE` |
| `tables.include` | `WAKE_TABLES_INCLUDE` |
| `cache_invalidator.redis.addrs` | `WAKE_CACHE_INVALIDATOR_REDIS_ADDRS` |

Scalars are strings, booleans (`true`, `false`, `1`, `0`), integers, durations
and sizes. List keys (`tables.include`, `tables.exclude`, the Kafka brokers of
the cache invalidator, ...) take comma-separated values. Entries of the `sinks`
list and the map keys `tables.name_map` and `cache_invalidator.templates` can be
set only in the file. An invalid value is reported with the variable name.

**Secrets** never appear as values. Keys ending in `_env` hold the *name* of an
environment variable that holds the secret (`source.dsn_env`,
`sinks[].webhook.secret_env`, `sinks[].kafka.sasl.username_env`, ...); keys
ending in `_file` name a file. A value that is not a valid variable name is rejected
there, and URLs must not embed passwords.
`wake config print` shows the effective configuration, never a secret value;
`--redact` also masks passwords embedded in URLs.

Two environment variables are not configuration keys: `WAKE_CONFIG` (the path of the
file) and `WAKE_LOG_FORMAT` (`text` for text logs on standard error; any other
value, or none, gives JSON lines).

**Durations** are Go duration strings (`500ms`, `10s`, `5m`, `1h30m`) plus a `d`
suffix for 24 hours (`14d`, `1d12h`). Negative durations are rejected.

**Sizes** are a number of bytes with an optional binary suffix: `B`, `KiB`,
`MiB`, `GiB`, `TiB` (`512`, `64MiB`, `1GiB`). A bare YAML integer is bytes.

**Validation.** `wake config validate` loads the file, applies the environment
and reports every problem at once, each with the key it concerns, then exits 2.
`wake run`, `wake check` and the other commands fail the same way
(`config_invalid` in [errors.md](errors.md)). When the `cache_invalidator` section is
configured and the variable named by `cache_invalidator.validate_dsn_env` is set, it also
connects to Postgres and runs the template check of `wake cache-invalidator`. `wake config
print` prints the merged configuration even when it is invalid.

## source

The source Postgres database: connection and publication. Wake reads one
logical replication slot with the `pgoutput` plugin.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `source.name` | string | none, required | Names the replication slot and the managed publication `wake_<name>`, is the `source.db` of every event and the default JetStream stream `wake_<name>`. Must match `[a-z0-9_]{1,58}`; at most 51 characters when `publication.mode` is `managed` and `tables.emit_unkeyed` is `true`, so that `wake_<name>_insert` fits the 63-byte identifier limit |
| `source.dsn_env` | string | `WAKE_SOURCE_DSN` | Name of the environment variable that holds the Postgres connection string (URL or key/value form). Must be a valid variable name. The variable must be set when Wake connects |
| `source.publication.mode` | string | `managed` | `managed`: Wake creates and alters `wake_<name>` (and `wake_<name>_insert` for keyless tables) from `tables.include` and `tables.exclude`. Needs `CREATE` on the database and ownership of every matched table. `existing`: Wake uses the publication named by `publication.name` and never alters it |
| `source.publication.name` | string | `""` | The publication to read. Required when `mode` is `existing`, ignored otherwise |

Publication modes are compared in [getting-started.md](getting-started.md).

## tables

Which tables are captured and how they are named.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `tables.include` | list of globs | `[]` | Globs on `<schema>.<table>`. Required (at least one) in `managed` mode. In `existing` mode an empty list captures every table of the publication, a non-empty list narrows it |
| `tables.exclude` | list of globs | `[]` | Tables matching any glob are not captured, also when included |
| `tables.refresh_interval` | duration | `1m` | How often Wake re-evaluates the globs (managed) or re-reads the publication's table list (existing) to find added and removed tables. Must be greater than 0. A `SIGHUP` triggers it immediately |
| `tables.skip_unkeyed` | bool | `false` | Skip tables without a key (no primary key, no replica-identity index). Without it (and without `emit_unkeyed`) startup fails with `table_keyless` |
| `tables.emit_unkeyed` | bool | `false` | Emit events of keyless tables with `key: null` and no per-key order. In managed mode matched keyless tables go to a second publication `wake_<name>_insert` that publishes inserts only, so the application's UPDATE and DELETE keep working; UPDATE and DELETE of such tables are not captured. See [operations.md](operations.md#keyless-tables) |
| `tables.name_map` | map | `{}` | `"<schema>.<table>": "<schema>.<table>"`. Renames a table in events and topic or subject names. File only. Keys and targets must have the form `<schema>.<table>`; targets must match `[A-Za-z0-9_-]+` per part, be unique and keep every Kafka topic at 249 characters or less. Needed for tables whose names contain other characters |

Glob syntax is Go's `path.Match`: `*`, `?`, `[a-z]`, `\` escapes. Because table
names never contain `/`, `*` also matches the `.` between schema and table:
`*` matches every table, `public.*` every table of schema `public`, `*.orders`
every table named `orders`. A pattern must match the whole `<schema>.<table>`,
so `audit_*` matches no table in `public`; write `public.audit_*`. An empty glob
is invalid.

Schema, table and prefix names must match `[A-Za-z0-9_-]+` (they become topic
and subject names). Tables with other names are rejected at startup with
`identifier_invalid` unless mapped. A table with such a name that appears while
Wake runs is skipped with an error log until you map it and send `SIGHUP`; it is
then treated as newly detected and snapshotted, but its changes before the
mapping are not delivered ([operations.md](operations.md#changes-before-a-name-mapping)).

## events

Optional event kinds.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `events.emit_truncate` | bool | `false` | Emit one `op: "t"` event per truncated table. When off, a truncate is logged and counted in `wake_truncate_skipped_total` |
| `events.emit_messages` | bool | `false` | Forward Postgres logical messages (`pg_logical_emit_message`) other than Wake's own (`wake.heartbeat`, `wake.watermark`, `wake.snapshot`) to `<prefix>._messages` (webhook: `"type": "message"`). They go to every sink that has at least one table glob |

## snapshot

The initial snapshot and the snapshot of tables added later
([operations.md](operations.md#snapshots)).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `snapshot.mode` | string | `initial` | `initial`: on the first start (the slot is created) copy the existing rows of every captured table as `op: "r"` events. `never`: skip the copy. Governs only the initial snapshot |
| `snapshot.added_tables` | bool | `true` | Snapshot tables that join the pipeline later (detected by `tables.refresh_interval`, or mapped by `tables.name_map` and `SIGHUP`), one table at a time, without pausing the stream |
| `snapshot.chunk_rows` | int | `10000` | Rows per chunk. Must be greater than 0. The value in force when a run starts is stored with the run |
| `snapshot.parallel_tables` | int | `4` | Worker sessions of the initial snapshot. At least 1. Snapshots of added tables and the resume of an interrupted initial snapshot run one table at a time regardless |

Wake opens at most `parallel_tables + 4` SQL connections (application name
`wake`) plus one replication connection; keep `max_connections` high enough.

## stream

Reading the replication stream, assembling transactions and acknowledging.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `stream.max_tx_bytes` | size | `256MiB` | Memory one transaction may use before it is spilled to disk. Greater than 0. Also the point where reading pauses while the stream is held back during the initial snapshot |
| `stream.spill_dir` | path | `./spill` | Directory of spill files. Must not be empty. Spill files are deleted at every start. Size the volume for your largest transaction |
| `stream.status_interval` | duration | `10s` | How often Wake reports its position to Postgres. Greater than 0 and, at startup, below half of the server's `wal_sender_timeout` (`status_interval_invalid`; the check is skipped when the timeout is 0) |
| `stream.min_confirm_bytes` | size | `1MiB` | Also send a status update when the confirmed position advanced by at least this much. 0 disables the byte trigger; negative is invalid |

## state

| Key | Type | Default | Meaning |
|---|---|---|---|
| `state.path` | path | `./wake-state.json` | The state file ([operations.md](operations.md#state-file)). Must not be empty. Put it on a persistent volume |

## sinks

A list of destinations. At least one is required. Entries can only be set in the
file. Every entry has these keys:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | none, required | Unique, non-empty. Appears in metrics, logs and status |
| `type` | string | none, required | `kafka`, `nats`, `webhook` or `stdout` |
| `tables` | list of globs | `["*"]` | Tables this sink receives, matched against the event's `<schema>.<table>` (after `name_map`). `[]` means no tables |
| `max_events` | int | `1000` | Events per batch, at least 1. Used by the webhook and stdout sinks; Kafka and NATS ignore it |
| `max_bytes` | size | `1MiB` | Bytes per batch, greater than 0. Used by the Kafka, webhook and stdout sinks; NATS ignores it |
| `linger` | duration | `10ms` | How long a batch waits to fill, not negative. Used by the Kafka, webhook and stdout sinks; NATS ignores it |
| `max_event_bytes` | size | per type | Largest accepted event. Defaults: Kafka `1MiB`, webhook `10MiB`, NATS `0` (the server's `max_payload`), stdout no limit. Not negative. An event above it blocks the sink with `event_too_large` |

Exactly the section named after the type may be present (`kafka`, `nats`,
`webhook`); a section for another type is rejected. Behaviour and error handling
of each sink are in [sinks.md](sinks.md).

### sinks[].kafka

| Key | Type | Default | Meaning |
|---|---|---|---|
| `brokers` | list of `host:port` | none, required | Seed brokers |
| `topic_prefix` | string | none, required | Topics are `<prefix>.<schema>.<table>`, plus `<prefix>._schema` and `<prefix>._messages`. Matches `[A-Za-z0-9_-]+` |
| `tombstones` | bool | `true` | Write a record with the same key and a null value after each `d` event |
| `auto_create_topics` | bool | `false` | Create missing topics. When off, a missing topic blocks the sink (`sink_fatal`) until you create it |
| `partitions` | int | `-1` | Partitions of created topics. `-1` uses the broker's `num.partitions`; otherwise at least 1 |
| `replication_factor` | int | `-1` | Replication factor of created topics. `-1` uses the broker's `default.replication.factor`; otherwise at least 1 |
| `tls` | TLS | disabled | See [TLS settings](#tls-settings) |
| `sasl.mechanism` | string | `""` | `""` (none), `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `sasl.username_env` | string | `""` | Variable holding the user name. Required with a mechanism |
| `sasl.password_env` | string | `""` | Variable holding the password. Required with a mechanism |

### sinks[].nats

| Key | Type | Default | Meaning |
|---|---|---|---|
| `url` | string | none, required | Server URL or a comma-separated list; scheme `nats`, `tls`, `ws` or `wss`; no embedded password |
| `subject_prefix` | string | none, required | Subjects are `<prefix>.<schema>.<table>`, plus `<prefix>._schema` and `<prefix>._messages`. Matches `[A-Za-z0-9_-]+` |
| `create_stream` | bool | `true` | Create the stream, or add the subject and duplicate window to an existing one. When off, the stream must exist |
| `stream` | string | `wake_<source.name>` | JetStream stream; it captures `<subject_prefix>.>` |
| `duplicate_window` | duration | `2m` | Deduplication window of the stream. Greater than 0 when `create_stream` is on |
| `credentials_file` | path | `""` | NATS user credentials file; re-read on every reconnect |
| `tls` | TLS | disabled | See [TLS settings](#tls-settings) |

### sinks[].webhook

| Key | Type | Default | Meaning |
|---|---|---|---|
| `url` | string | none, required | `http` or `https` URL; no embedded password. Use `https` |
| `secret_env` | string | none, required | Variable holding the HMAC signing secret |
| `timeout` | duration | `10s` | Per-request timeout, greater than 0 |
| `lanes` | int | `8` | Parallel lanes, at least 1. Keys hash into lanes; each lane has one batch in flight |
| `max_attempts` | int | `10` | Poison responses (a non-2xx other than 408, 429 and 5xx) tolerated per batch before it is dead-lettered. At least 1 |
| `dead_letter_dir` | path | `""` | Opt-in. When set, a batch that reached `max_attempts` poison responses is written there as JSONL and acknowledged, which breaks per-key order for its keys. When empty, a poison batch blocks the sink until the receiver accepts it |

### TLS settings

The `tls` section of Kafka, NATS and Redis has the same keys:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `enabled` | bool | `false` | Use TLS |
| `ca_file` | path | `""` | PEM file with the CA certificates to trust, replacing the system roots |
| `cert_file` | path | `""` | Client certificate (PEM); set together with `key_file` |
| `key_file` | path | `""` | Client key (PEM); set together with `cert_file` |

## lag

Slot lag monitoring and alerts ([operations.md](operations.md#metrics-and-alerts)).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `lag.interval` | duration | `15s` | How often the slot is polled. Greater than 0 |
| `lag.warn_bytes` | size | `1GiB` | Slot lag above this fires the `slot_lag_warn` alert. Greater than 0 and less than `critical_bytes` |
| `lag.critical_bytes` | size | `10GiB` | Slot lag above this fires `slot_lag_critical`. Greater than 0 |
| `lag.max_ack_age` | duration | `5m` | Age of the oldest event Wake has read and not yet seen acknowledged by every sink it was routed to; above it, `max_ack_age` fires. Greater than 0 |
| `lag.alert_webhook` | URL | `""` | Optional. Each alert state change is POSTed as JSON `{"rule","state","value","threshold","ts"}`. `http` or `https`, no embedded password |

The fourth rule, `sink_fatal`, has no threshold: it fires while any sink is
blocked by a non-retriable error.

## heartbeat

| Key | Type | Default | Meaning |
|---|---|---|---|
| `heartbeat.interval` | duration | `1m` | When no captured transaction was read for this long, Wake emits a `wake.heartbeat` logical message so the confirmed position advances and WAL is recycled on an idle source. Greater than 0 |

## admin

| Key | Type | Default | Meaning |
|---|---|---|---|
| `admin.listen` | `host:port` | `127.0.0.1:9093` | Address of the admin HTTP server: `/healthz`, `/readyz`, `/metrics`, `/v1/status`, `/v1/version`. The compose file and container deployments use `0.0.0.0:9093`. The endpoints are unauthenticated: do not expose them beyond your cluster |

## cache_invalidator

Read only by `wake cache-invalidator` ([cache.md](cache.md#invalidating-from-change-events)).
The section is validated strictly (every required key present) by that command
and, in `wake run` and `wake config validate`, once any of `namespace`,
`templates` or `redis.addrs` is set; structural rules (enums, addresses, formats)
always apply.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `admin_listen` | `host:port` | `127.0.0.1:9096` | `/healthz`, `/readyz` and `/metrics` of the invalidator |
| `source` | string | `kafka` | `kafka` or `nats`: where the change events are read from |
| `kafka.brokers` | list of `host:port` | none | Required with `source: kafka` |
| `kafka.topic_prefix` | string | none | The prefix of the Wake Kafka sink; topics `<prefix>.<schema>.<table>` are derived from the templates. Required with `source: kafka` |
| `kafka.group` | string | `wake-cache-invalidator` | Consumer group |
| `kafka.tls`, `kafka.sasl` | TLS, SASL | disabled | As for the Kafka sink |
| `nats.url` | string | none | Required with `source: nats` |
| `nats.subject_prefix` | string | none | The prefix of the Wake NATS sink. Required with `source: nats` |
| `nats.stream` | string | `wake_<source.name>` | JetStream stream holding the events. Set it explicitly when the file has no `source.name` |
| `nats.durable` | string | `wake-cache-invalidator` | Durable consumer name |
| `nats.credentials_file`, `nats.tls` | path, TLS | none | As for the NATS sink |
| `redis.addrs` | list of `host:port` | none, required | One address: single node. Several: Cluster. With `master_name`: Sentinel addresses |
| `redis.master_name` | string | `""` | Selects Sentinel |
| `redis.password_env` | string | `""` | Variable holding the Redis password; it must be non-empty when named |
| `redis.tls` | TLS | disabled | See [TLS settings](#tls-settings) |
| `namespace` | string | none, required | The `pkg/cache` namespace whose keys are deleted. Matches `[A-Za-z0-9_-]+`; must equal the namespace of your application's caches |
| `bus_channel` | string | `wake:inval:<namespace>` | Invalidation channel; must equal the channel of your caches (`WithInvalidationBus`) |
| `invalidate_on_snapshot` | bool | `false` | Also invalidate on `op: "r"` events. Off by default because a re-snapshot would flood the cache with invalidations |
| `validate_dsn_env` | string | `WAKE_SOURCE_DSN` | Name of a variable holding a Postgres DSN. When it is set, `wake cache-invalidator` connects at startup and rejects templates that reference non-key columns of a table whose replica identity is not `FULL`, columns a table does not have and tables that do not exist (`template_requires_full_identity`). `""` disables the startup check; the runtime check always applies. An unset variable skips the check with a warning |
| `templates` | map of lists | none, required | `<schema>.<table>: [key templates]`. File only. See below |

Templates are literal text with `{column}` placeholders: `user:{id}`,
`user:email:{email}`. `{{` and `}}` are literal braces. Every template of a table
is rendered against the `before` and the `after` image of each event; a null or
absent value skips that rendering. A template without placeholders renders to the
same key for every event of the table (a list or count key).

## Example

A complete example with comments is in
[deploy/config.example.yaml](../../deploy/config.example.yaml); the file used by
the quickstart is [deploy/config.compose.yaml](../../deploy/config.compose.yaml).
