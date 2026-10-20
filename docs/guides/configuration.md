# Configuration

Look up every configuration key, its default and its rules. A commented example is in
[deploy/config.example.yaml](../../deploy/config.example.yaml).

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

```bash
wake run --config wake.yaml          # or set WAKE_CONFIG
wake config validate --config wake.yaml   # every problem at once; exits 2
wake config print --redact           # effective config, never secrets
```

`source.name` and at least one sink are required. Unknown keys are errors.

**Precedence**, highest first: command-line flags, `WAKE_<PATH>` environment variables, the
YAML file, defaults. v0.1 has no per-key flags.

**Environment variables**: `WAKE_` plus the dotted path in upper case, dots and hyphens as
underscores. Works for any scalar key outside `sinks`.

| Key | Environment variable |
|---|---|
| `admin.listen` | `WAKE_ADMIN_LISTEN` |
| `stream.status_interval` | `WAKE_STREAM_STATUS_INTERVAL` |
| `source.publication.mode` | `WAKE_SOURCE_PUBLICATION_MODE` |
| `tables.include` | `WAKE_TABLES_INCLUDE` |
| `cache_invalidator.redis.addrs` | `WAKE_CACHE_INVALIDATOR_REDIS_ADDRS` |

Lists take comma-separated values. Booleans are `true`, `false`, `1`, `0`. The `sinks` list,
`tables.name_map` and `cache_invalidator.templates` are file only.

**Secrets** are never values. Keys ending in `_env` name an environment variable holding the
secret; keys ending in `_file` name a file. URLs must not embed passwords.

Two variables are not keys: `WAKE_CONFIG` (file path) and `WAKE_LOG_FORMAT` (`text` for text
logs; anything else gives JSON).

**Durations**: Go strings (`500ms`, `10s`, `5m`, `1h30m`) plus `d` for 24 hours (`14d`,
`1d12h`). No negatives.

**Sizes**: bytes with an optional `B`, `KiB`, `MiB`, `GiB`, `TiB` (`512`, `64MiB`, `1GiB`). A
bare integer is bytes.

**Validation**: every command fails on an invalid config (`config_invalid`, [errors.md](errors.md)).
With a `cache_invalidator` section and `cache_invalidator.validate_dsn_env` set,
`wake config validate` also checks the templates against Postgres. `wake config print` works
even when the config is invalid.

## source

| Key | Type | Default | Meaning |
|---|---|---|---|
| `source.name` | string | none, required | names slot, publication and `source.db`. `[a-z0-9_]{1,58}`; max 51 with `managed` and `emit_unkeyed` |
| `source.dsn_env` | string | `WAKE_SOURCE_DSN` | variable holding the DSN |
| `source.publication.mode` | string | `managed` | `managed` or `existing` ([compared](getting-started.md#a-role-with-the-least-privileges-recommended)) |
| `source.publication.name` | string | `""` | required with `existing` |

## tables

| Key | Type | Default | Meaning |
|---|---|---|---|
| `tables.include` | list of globs | `[]` | globs; required in `managed`; empty = all in `existing` |
| `tables.exclude` | list of globs | `[]` | never captured |
| `tables.refresh_interval` | duration | `1m` | table re-scan; > 0 |
| `tables.skip_unkeyed` | bool | `false` | skip keyless tables |
| `tables.emit_unkeyed` | bool | `false` | emit keyless inserts ([keyless tables](operations.md#keyless-tables)) |
| `tables.name_map` | map | `{}` | `"<schema>.<table>": "<schema>.<table>"`; file only; unique, `[A-Za-z0-9_-]+` |

Globs use Go's `path.Match` (`*`, `?`, `[a-z]`, `\`). A glob must match the whole
`<schema>.<table>`:

```yaml
tables:
  include: ["public.*"]       # every table in public
  exclude: ["*.orders"]       # every table named orders; "audit_*" alone matches nothing
```

`*` matches every table. An empty glob is invalid.

Schema, table and prefix names must match `[A-Za-z0-9_-]+`. Others fail with
`identifier_invalid` unless mapped. Changes before the mapping are not delivered
([operations.md](operations.md#changes-before-a-name-mapping)).

## events

| Key | Type | Default | Meaning |
|---|---|---|---|
| `events.emit_truncate` | bool | `false` | emit `op: "t"` events |
| `events.emit_messages` | bool | `false` | forward logical messages to `<prefix>._messages` |

## snapshot

See [operations.md](operations.md#snapshots).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `snapshot.mode` | string | `initial` | `initial` or `never` |
| `snapshot.added_tables` | bool | `true` | snapshot added tables |
| `snapshot.chunk_rows` | int | `10000` | rows per chunk; > 0 |
| `snapshot.parallel_tables` | int | `4` | initial snapshot workers; >= 1 |

Wake opens at most `parallel_tables + 4` SQL connections plus one replication connection. Keep
`max_connections` high enough.

## stream

| Key | Type | Default | Meaning |
|---|---|---|---|
| `stream.max_tx_bytes` | size | `256MiB` | memory per transaction before spilling |
| `stream.spill_dir` | path | `./spill` | spill directory; cleared at start |
| `stream.status_interval` | duration | `10s` | position report interval; below half of `wal_sender_timeout` |
| `stream.min_confirm_bytes` | size | `1MiB` | also report after this progress; 0 disables |

## state

| Key | Type | Default | Meaning |
|---|---|---|---|
| `state.path` | path | `./wake-state.json` | [state file](operations.md#state-file); persistent volume |

## sinks

At least one sink is required. File only. Each entry:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | none, required | unique |
| `type` | string | none, required | `kafka`, `nats`, `webhook` or `stdout` |
| `tables` | list of globs | `["*"]` | table globs; `[]` = none |
| `max_events` | int | `1000` | batch events; webhook and stdout only |
| `max_bytes` | size | `1MiB` | batch bytes; not NATS |
| `linger` | duration | `10ms` | batch wait; not NATS |
| `max_event_bytes` | size | per type | Kafka `1MiB`, webhook `10MiB`, NATS `0` (`max_payload`), stdout none; larger blocks |

Only the section matching `type` may be present. Sink behaviour is in [sinks.md](sinks.md).

### sinks[].kafka

| Key | Type | Default | Meaning |
|---|---|---|---|
| `brokers` | list of `host:port` | none, required | seed brokers |
| `topic_prefix` | string | none, required | topic prefix; `[A-Za-z0-9_-]+` |
| `tombstones` | bool | `true` | tombstone after deletes |
| `auto_create_topics` | bool | `false` | create missing topics |
| `partitions` | int | `-1` | `-1` = broker default |
| `replication_factor` | int | `-1` | `-1` = broker default |
| `tls` | TLS | disabled | see [TLS settings](#tls-settings) |
| `sasl.mechanism` | string | `""` | `""`, `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `sasl.username_env` | string | `""` | user name variable |
| `sasl.password_env` | string | `""` | password variable |

### sinks[].nats

| Key | Type | Default | Meaning |
|---|---|---|---|
| `url` | string | none, required | URL list; `nats`, `tls`, `ws`, `wss` |
| `subject_prefix` | string | none, required | subject prefix; `[A-Za-z0-9_-]+` |
| `create_stream` | bool | `true` | create or update the stream |
| `stream` | string | `wake_<source.name>` | JetStream stream |
| `duplicate_window` | duration | `2m` | deduplication window |
| `credentials_file` | path | `""` | credentials file |
| `tls` | TLS | disabled | see [TLS settings](#tls-settings) |

### sinks[].webhook

| Key | Type | Default | Meaning |
|---|---|---|---|
| `url` | string | none, required | use `https` |
| `secret_env` | string | none, required | HMAC secret variable |
| `timeout` | duration | `10s` | per request |
| `lanes` | int | `8` | parallel lanes |
| `max_attempts` | int | `10` | poison responses before dead-letter |
| `dead_letter_dir` | path | `""` | opt-in; **breaks per-key order** |

### TLS settings

Used by Kafka, NATS and Redis `tls` sections.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `enabled` | bool | `false` | use TLS |
| `ca_file` | path | `""` | CA PEM; replaces system roots |
| `cert_file` | path | `""` | client certificate |
| `key_file` | path | `""` | client key |

## lag

See [metrics and alerts](operations.md#metrics-and-alerts).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `lag.interval` | duration | `15s` | slot poll interval |
| `lag.warn_bytes` | size | `1GiB` | `slot_lag_warn`; below `critical_bytes` |
| `lag.critical_bytes` | size | `10GiB` | `slot_lag_critical` |
| `lag.max_ack_age` | duration | `5m` | `max_ack_age` |
| `lag.alert_webhook` | URL | `""` | optional alert POST target |

`sink_fatal` has no threshold.

## heartbeat

| Key | Type | Default | Meaning |
|---|---|---|---|
| `heartbeat.interval` | duration | `1m` | idle time before a heartbeat |

## admin

| Key | Type | Default | Meaning |
|---|---|---|---|
| `admin.listen` | `host:port` | `127.0.0.1:9093` | admin server; containers use `0.0.0.0:9093`. **Unauthenticated** |

## cache_invalidator

Used only by `wake cache-invalidator` ([cache.md](cache.md#invalidating-from-change-events)).
Required keys are checked once `namespace`, `templates` or `redis.addrs` is set.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `admin_listen` | `host:port` | `127.0.0.1:9096` | invalidator admin server |
| `source` | string | `kafka` | `kafka` or `nats` |
| `kafka.brokers` | list of `host:port` | none | required with `source: kafka` |
| `kafka.topic_prefix` | string | none | required with `source: kafka` |
| `kafka.group` | string | `wake-cache-invalidator` | consumer group |
| `kafka.tls`, `kafka.sasl` | TLS, SASL | disabled | as for the Kafka sink |
| `nats.url` | string | none | required with `source: nats` |
| `nats.subject_prefix` | string | none | required with `source: nats` |
| `nats.stream` | string | `wake_<source.name>` | set it if no `source.name` |
| `nats.durable` | string | `wake-cache-invalidator` | durable consumer |
| `nats.credentials_file`, `nats.tls` | path, TLS | none | as for the NATS sink |
| `redis.addrs` | list of `host:port` | none, required | one = node, several = Cluster |
| `redis.master_name` | string | `""` | selects Sentinel |
| `redis.password_env` | string | `""` | password variable |
| `redis.tls` | TLS | disabled | see [TLS settings](#tls-settings) |
| `namespace` | string | none, required | your `pkg/cache` namespace |
| `bus_channel` | string | `wake:inval:<namespace>` | must match your caches |
| `invalidate_on_snapshot` | bool | `false` | also on `op: "r"` |
| `validate_dsn_env` | string | `WAKE_SOURCE_DSN` | DSN variable for the template check; `""` disables |
| `templates` | map of lists | none, required | `<schema>.<table>: [templates]`; file only |

```yaml
templates:
  public.users: ["user:{id}", "user:email:{email}"]   # {column} placeholders; {{ }} are literal braces
```

Each template is rendered for `before` and `after`. A null value skips it. A template without
placeholders gives one fixed key.

## Example

Full example: [deploy/config.example.yaml](../../deploy/config.example.yaml). Quickstart config:
[deploy/config.compose.yaml](../../deploy/config.compose.yaml).
