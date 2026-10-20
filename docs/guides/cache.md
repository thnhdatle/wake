# Cache library and CDC invalidation

`pkg/cache` is a read-through cache for Go with an in-process tier (L1) in front of
a shared Redis tier (L2). It stays correct when rows change outside your
application (migrations, batch jobs, other services) because Wake's change events
delete the affected keys in every instance. This guide covers the library, its
consistency model, what happens when Redis is down, and the
`wake cache-invalidator` that connects the two.

- [Quick start](#quick-start)
- [API](#api)
- [Options](#options)
- [The loader contract](#the-loader-contract)
- [Consistency model](#consistency-model)
- [Redis outages](#redis-outages)
- [Invalidating from change events](#invalidating-from-change-events)
- [Metrics](#metrics)
- [Operating notes](#operating-notes)

## Quick start

L1 only, no Redis (one process):

```go
users, err := cache.New[User]("users", cache.JSONCodec[User](),
  cache.WithTTL(10*time.Minute, time.Minute))
if err != nil {
  return err
}
defer users.Close()

u, err := users.GetOrLoad(ctx, "user:42", func(ctx context.Context) (User, error) {
  return db.LoadUser(ctx, 42) // read from the PRIMARY database
})
```

With a shared Redis tier, so that several instances share values and invalidate each
other:

```go
rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"redis:6379"}})
defer rdb.Close() // the cache never closes your client

users, err := cache.New[User]("users", cache.JSONCodec[User](),
  cache.WithL1(50_000, 256<<20),            // entries, bytes
  cache.WithL2(rdb),                        // single node, Sentinel or Cluster
  cache.WithTTL(10*time.Minute, time.Minute),
  cache.WithStaleIfError(5*time.Minute),    // serve expired L1 values if the loader fails
  cache.WithMetrics(prometheus.DefaultRegisterer),
)
```

`cache.New` starts two goroutines when Redis is configured (the invalidation-bus
subscriber and a maintenance loop); `Close` stops them. `New` succeeds even when
Redis is unreachable at that moment: the cache starts degraded and switches to
Redis once it answers.

## API

```go
type Cache[V any] interface {
    Get(ctx context.Context, key string) (V, bool, error)
    GetOrLoad(ctx context.Context, key string, load LoadFunc[V]) (V, error)
    Set(ctx context.Context, key string, v V, opts ...SetOption) error
    Delete(ctx context.Context, keys ...string) error
    Close() error
}

type LoadFunc[V any] func(ctx context.Context) (V, error)

func New[V any](namespace string, codec Codec[V], opts ...Option) (Cache[V], error)
```

| Method | Behaviour |
|---|---|
| `Get` | L1, then L2. Returns `(value, true, nil)` on a hit and `(zero, false, nil)` on a miss, including a negative entry and an unreachable Redis; it does not load |
| `GetOrLoad` | L1, then L2, then the loader. Concurrent calls for a key in one process share one load, and across instances only one holds a short Redis lock while the others wait for its result. Returns `cache.ErrNotFound` for a cached or freshly loaded absence |
| `Set` | Stores `v` for every instance: increments the key's generation in L2, writes the value and tells every instance to drop its L1 copy, then sets L1 locally. `WithSetTTL(ttl)` overrides the TTL for this value |
| `Delete` | Invalidates keys in L2 and in the L1 of every instance. While Redis is unreachable it deletes locally and returns `cache.ErrDeferred` |
| `Close` | Stops the goroutines and releases the subscription; idempotent; does not close your Redis client |

`namespace` must match `[A-Za-z0-9_-]+`. All instances of a cache, and the
invalidator, must use the same namespace and the same invalidation channel.

Codecs: `cache.JSONCodec[V]()`, `cache.MsgpackCodec[V]()`,
`cache.ProtobufCodec[V]()`; implement `cache.Codec[V]` (`Marshal`, `Unmarshal`) for
anything else. Values are encoded for L2, and the encoded size is what L1's byte
bound counts.

## Options

| Option | Default | Meaning |
|---|---|---|
| `WithL1(maxEntries, maxBytes)` | 10,000 entries, 64 MiB | Bounds of the in-process tier (plain LRU, sharded) |
| `WithL2(client)` | none (L1 only) | Redis tier; `client` is a `redis.UniversalClient`: single node, Sentinel or Cluster |
| `WithTTL(ttl, jitter)` | 5 minutes, no jitter | Value TTL; each value lives `ttl` minus a random amount in `[0, jitter)`, so values loaded together do not expire together. `jitter` must be below `ttl` |
| `WithNegativeTTL(ttl)` | not cached | Cache a loader's `ErrNotFound` for `ttl` |
| `WithStaleIfError(maxStale)` | disabled | When the loader fails, return an L1 value that expired less than `maxStale` ago. A value removed by an invalidation is never served stale |
| `WithInvalidationBus(channel)` | `wake:inval:<namespace>` | Redis pub/sub channel; identical on all instances and in `cache_invalidator.bus_channel` |
| `WithLockTimeout(d)` | 5 s | Lifetime of the load lock, and how long waiters wait for the lock holder before loading themselves. Keep it above your loader's typical latency |
| `WithDegradedL1TTL(d)` | 5 s | Cap on the age of L1 values while Redis or the bus is unreachable (invalidations may be missed); also bounds how fast a dead bus connection is detected |
| `WithMaxPendingInvalidations(n)` | 10,000 | Bound of the keys whose invalidation is deferred during a Redis outage. `0` disables deferral: `Delete` and `Set` then return an error (not `ErrDeferred`) while Redis is unreachable |
| `WithCompression(minBytes)` | off | zstd-compress L2 payloads of at least `minBytes`; instances may differ in this setting |
| `WithMetrics(registerer)` | not exported | Register the `wake_cache_*` metrics; several caches may share a registerer |
| `WithClock(clock)` | system clock | Time source for TTLs and waits (for tests) |

Every option has a runnable example in `pkg/cache/example_test.go`.

## The loader contract

- **Read the primary database.** The loader must return the current value from the
  primary, never from a lagging replica. A replica that has not yet applied the change
  that triggered an invalidation can hand back the old row, which the cache then stores
  under a fresh generation and keeps until the TTL expires. If you must load from a
  replica, set the TTL no higher than the replica lag you are willing to tolerate.
- **Return only the value.** `LoadFunc` returns `(V, error)`; there are no versions or
  timestamps to pass.
- **Return `cache.ErrNotFound` for an absent row** (wrapped is fine). With
  `WithNegativeTTL` the absence is cached; callers get `ErrNotFound`.
- **Other errors** are returned wrapped, except that `WithStaleIfError` can answer from
  an expired L1 value instead.
- **Do not panic.** A panic in a loader crashes the process.
- Concurrent callers for the same key in one process share one load and therefore the
  context and loader of the first caller.
- **Treat `V` as immutable.** Values are shared between callers: L1 hands out the stored
  value itself, so mutating a returned slice, map or pointed-to struct changes what
  every other caller sees. Copy before modifying.

## Consistency model

The cache uses no clocks and no versions from your database. Consistency rests on three
counters:

1. **A generation per key in Redis.** `Delete` and `Set` increment it in the same script
   that removes or writes the value and publishes the invalidation. A cached value is
   valid only while its stored generation equals the current one. A loader notes the
   generation before it loads and writes its result only if the generation is still
   that one, so a load that overlapped an invalidation is discarded, never cached.
2. **A namespace epoch** in every Redis key name. Bumping it makes all older entries
   invisible; it happens when the set of deferred invalidations overflows.
3. **A guard in L1.** Each L1 shard counts invalidations. A reader populates L1 only if no
   invalidation touched the shard since it started reading, so a stale value cannot be
   inserted after an invalidation it missed.

What this gives you:

- After `Delete(key)` returns nil, no instance serves a value loaded before the delete,
  and a load that was in flight during the delete does not repopulate the cache with its
  older result.
- With the invalidator, a change committed in Postgres invalidates the key in every
  instance within the pipeline latency (commit to event to `Delete` to bus message); the
  v0.1.0 benchmark measured a 99th percentile of 18.5 ms on 3 instances
  ([bench/RESULTS.md](../../bench/RESULTS.md)). Before the invalidation arrives, a
  reader can still see the old value: the cache is eventually consistent with the
  database, with that window.
- Anything that bypasses invalidation falls back to the TTL.

Known gaps to design around:

- **`t` (truncate) events are not invalidated.** The invalidator skips and counts them;
  rely on the TTL or delete the keys yourself.
- **Replica loaders** (above).
- **Outages** (below) bound staleness by `WithDegradedL1TTL` instead of making reads fail.
- **Redis eviction.** Run Redis with `maxmemory-policy noeviction` or a `volatile-*`
  policy: evicting a generation key would let an old value be read again.
- **Keys of other namespaces** and keys that your templates do not render are not touched.

## Redis outages

Redis failures never turn into errors for readers. A circuit breaker opens after 5
consecutive Redis failures and probes again every second. While Redis or the invalidation
bus is down:

- `Get` reports a miss, `GetOrLoad` calls your loader, so each instance loads on its own
  (cross-instance stampede protection is lost until Redis is back);
- L1 keeps working with entry ages capped at `WithDegradedL1TTL` (default 5 s), because
  invalidations may have been missed; an expired value can still be served up to
  `WithStaleIfError` when the loader also fails;
- `Delete` removes the key locally and returns `cache.ErrDeferred`: the key is queued (up to
  `WithMaxPendingInvalidations`, default 10,000) and its invalidation replays when Redis
  recovers. A queued key is never read from L2 meanwhile. If the queue overflows, the
  namespace epoch is incremented on recovery, which invalidates every entry. Callers that
  need a global invalidation (the invalidator) retry until `Delete` returns nil;
- `Set` returns an error wrapping `ErrDeferred` and stores no local value.

On recovery, the maintenance loop replays the queue, and when the bus reconnects every
instance flushes its L1 and re-reads the epoch, since messages may have been lost. Expect
a burst of loads after a long outage or an epoch change (`wake_cache_epoch_changes_total`).

## Invalidating from change events

Wake can delete cache keys from its own event stream, so the cache is invalidated by every
change, whoever made it. There are two ways to run this.

### `wake cache-invalidator`

A process that reads the change events from Kafka (consumer group) or NATS (durable
consumer), renders cache keys from key templates and calls `Delete` on the namespace in
Redis. It serves `/healthz`, `/readyz` and `/metrics` on `cache_invalidator.admin_listen`
(default `127.0.0.1:9096`). The `cache_invalidator` section of the configuration
([configuration.md](configuration.md#cache_invalidator)):

```yaml
cache_invalidator:
  source: kafka
  kafka:
    brokers: ["kafka:9092"]
    topic_prefix: cdc              # the prefix of Wake's Kafka sink
    group: wake-cache-invalidator
  redis:
    addrs: ["redis:6379"]
    password_env: REDIS_PASSWORD
  namespace: users                 # the namespace of your cache.New("users", ...)
  bus_channel: wake:inval:users    # default wake:inval:<namespace>
  validate_dsn_env: WAKE_SOURCE_DSN
  templates:
    public.users: ["user:{id}", "user:email:{email}"]
```

Start it with `wake cache-invalidator --config wake.yaml`. Requirements:

- The namespace and the bus channel must equal those of the application's caches, and the
  rendered keys must be the keys your application passes to `Get` and `GetOrLoad`.
- The Wake CDC service must have a sink for the tables, and the Kafka topics (or the NATS
  stream) must exist; topics created later are picked up within 30 seconds.
- You can run several invalidators in one consumer group for availability: each takes a
  share of the partitions.
- On its first start the invalidator reads each topic from the beginning (NATS: all
  messages), so keys cached before the first run are invalidated too. With long retention
  that means many `Delete` calls once.

**Templates.** A template is literal text with `{column}` placeholders: `user:{id}`,
`user:email:{email}`, `order:{customer_id}:{id}`. `{{` and `}}` are literal braces.
Every template of the event's table is rendered against **each available row image**,
`before` and `after`, so a renamed unique attribute invalidates both its old and its new
key; the keys are deduplicated. A placeholder resolves from the event `key` first, then
from the image; integers render as exact decimals (including values above 2^53) and
strings verbatim. A null or absent value skips that rendering. A template without
placeholders renders the same key for every event of the table.

**`REPLICA IDENTITY FULL` for non-key columns.** With the default replica identity,
`before` holds only the key columns. A template such as `user:email:{email}` needs the old
email on updates and deletes to invalidate the old key, so the table must have `REPLICA
IDENTITY FULL`:

```sql
ALTER TABLE public.users REPLICA IDENTITY FULL;
```

Wake enforces this twice:

- at start-up, when `validate_dsn_env` names a variable holding a Postgres DSN (default
  `WAKE_SOURCE_DSN`): the invalidator connects and rejects templates that reference non-key
  columns of a table whose identity is not `FULL`, columns a table does not have, and
  tables that do not exist, with `template_requires_full_identity`. Set it to `""` to skip
  this check; an unset variable skips it with a warning;
- at runtime: the first update or delete whose `before` lacks a referenced column raises
  the same code (error log, `wake_cdcsource_template_requires_full_identity_total`,
  `/readyz` 503 of the invalidator until restart); processing continues with the images
  available.

**Delivery.** The invalidator commits the Kafka offset (or acknowledges the NATS message) only
after `Delete` returned nil for all keys of the batch. If Redis is down, `Delete` returns
`cache.ErrDeferred` and the batch is retried with capped, jittered backoff, with nothing
acknowledged: events wait in Kafka or NATS and are not lost. Invalidations are idempotent,
so redelivery is harmless.

What is invalidated: `c`, `u` and `d` events. Skipped and counted: Kafka tombstones,
invalid payloads, unknown ops, `t` events, events of unmapped tables, and `r` snapshot
events unless `invalidate_on_snapshot: true` (off by default, because a re-snapshot would
flood the cache with invalidations).

**Readiness.** `/readyz` has three checks: `redis` (a ping), `source` (the Kafka or NATS
consumer is running and healthy) and `templates` (no `template_requires_full_identity`
condition). Each failing check carries a code and reason; see
[troubleshooting.md](troubleshooting.md#cache-and-invalidator).

### Inside your application

The same logic is a library: `cdcsource.New(source, mapping, deleter)` with `Run(ctx)`.

```go
m, err := cdcsource.NewMapping(map[string][]string{
  "public.users": {"user:{id}", "user:email:{email}"},
})
if err != nil {
  return err
}
src, err := cdcsource.NewKafkaSource(cdcsource.KafkaConfig{
  Brokers: []string{"kafka:9092"}, TopicPrefix: "cdc", Group: "app-cache-invalidator",
}, m)
if err != nil {
  return err
}
inv, err := cdcsource.New(src, m, users, // users is a cache.Cache[User]; any Deleter works
  cdcsource.WithMetrics(prometheus.DefaultRegisterer))
if err != nil {
  return err
}
go func() { _ = inv.Run(ctx) }() // returns nil when ctx ends
```

`cdcsource.NewNATSSource` is the NATS counterpart. `Invalidator.Ready(ctx)` reports the
health for a readiness probe. `Mapping.CheckReplicaIdentity` performs the start-up identity
check from table metadata you read yourself.

## Metrics

Registered with `WithMetrics`, labeled with the cache `namespace`:

| Metric | Type | Meaning |
|---|---|---|
| `wake_cache_hits_total{namespace,tier}` | counter | Hits by tier (`l1`, `l2`) |
| `wake_cache_misses_total{namespace,tier}` | counter | Misses by tier |
| `wake_cache_loads_total` | counter | Loader calls |
| `wake_cache_lock_waits_total` | counter | `GetOrLoad` calls that waited for another instance's load lock |
| `wake_cache_fallback_loads_total` | counter | Loads by waiters after `lock_timeout` expired (the bounded fallback) |
| `wake_cache_stale_serves_total` | counter | Expired L1 values served because the loader failed |
| `wake_cache_l2_errors_total` | counter | Failed Redis operations |
| `wake_cache_invalidations_received_total` | counter | Invalidation messages received from the bus |
| `wake_cache_deferred_invalidations_total` | counter | Invalidations deferred while Redis was unreachable |
| `wake_cache_pending_overflows_total` | counter | Deferred invalidations beyond the bound (epoch increment on recovery) |
| `wake_cache_epoch_changes_total` | counter | Namespace epoch changes observed |
| `wake_cache_breaker_state` | gauge | Redis circuit breaker: 0 closed, 1 open, 2 half-open |
| `wake_cache_pending_invalidations` | gauge | Keys whose invalidation is deferred |
| `wake_cache_bus_connected` | gauge | 1 while the invalidation-bus subscription is up |

The invalidator (and `cdcsource` with `WithMetrics`) adds:

| Metric | Type | Meaning |
|---|---|---|
| `wake_cdcsource_events_total` | counter | Messages received, including skipped ones |
| `wake_cdcsource_events_skipped_total{reason}` | counter | Messages that caused no invalidation: `tombstone`, `invalid`, `unknown_op`, `truncate`, `snapshot`, `unmapped_table`, `no_keys` |
| `wake_cdcsource_keys_deleted_total` | counter | Cache keys whose `Delete` returned nil |
| `wake_cdcsource_delete_failures_total{kind}` | counter | Failed `Delete` calls (retried): `deferred` or `error` |
| `wake_cdcsource_template_requires_full_identity_total{table}` | counter | Updates and deletes whose `before` lacks a column a template needs |

Useful alerts: `wake_cache_breaker_state != 0` for more than a minute,
`wake_cache_bus_connected == 0`, a growing `wake_cache_pending_invalidations`, and
`rate(wake_cdcsource_delete_failures_total[5m]) > 0`.

## Operating notes

- **Redis**: version 5 or later (the scripts read the server clock before writing); tested
  with Redis 7. Single node, Sentinel and Cluster are supported; every per-key operation is
  one Lua script over keys sharing the `{<key>}` hash tag, so Cluster needs no cross-slot
  commands. The empty key `""` is not supported on Cluster.
- **Pub/sub**: invalidations travel on the pub/sub channel; a subscriber that is too slow is
  disconnected by Redis's output buffer limit and treated as a bus outage (reconnect, flush
  L1, re-read the epoch).
- **Memory**: L1 holds decoded values; size `WithL1` by bytes as well as entries. L2 values
  carry a TTL; generation keys live for the longest TTL plus the lock timeout plus a minute.
- **Stampedes**: concurrent misses for a hot key cost one load per process and one per
  cluster (the lock holder); waiters poll with jitter and fall back to loading themselves
  after `WithLockTimeout`, which is counted in `wake_cache_fallback_loads_total`.
  If that counter grows, your loader is slower than the lock timeout.
- **TLS and authentication to Redis** are configured on the `redis.UniversalClient` you pass
  (`wake cache-invalidator` takes `redis.tls` and `redis.password_env`).
