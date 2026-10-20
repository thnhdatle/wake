# Cache library and CDC invalidation

Cache rows in Go with `pkg/cache` (L1 plus Redis) and invalidate them from Wake's change events.

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

L1 only (one process):

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

With Redis, shared by several instances:

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

`New` succeeds even if Redis is down.

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
| `Get` | L1, then L2; never loads. A miss, negative entry or Redis outage returns `(zero, false, nil)` |
| `GetOrLoad` | L1, L2, then the loader, one load per key across instances. Absence returns `cache.ErrNotFound` |
| `Set` | stores for every instance; `WithSetTTL(ttl)` overrides the TTL |
| `Delete` | invalidates in L2 and every L1; returns `cache.ErrDeferred` while Redis is down |
| `Close` | stops background work; idempotent; does not close your Redis client |

`namespace` is `[A-Za-z0-9_-]+`, shared by all instances and the invalidator. Codecs:
`cache.JSONCodec[V]()`, `cache.MsgpackCodec[V]()`, `cache.ProtobufCodec[V]()` or your own
`cache.Codec[V]`.

## Options

| Option | Default | Meaning |
|---|---|---|
| `WithL1(maxEntries, maxBytes)` | 10,000 entries, 64 MiB | L1 size bounds |
| `WithL2(client)` | none (L1 only) | Redis `redis.UniversalClient` |
| `WithTTL(ttl, jitter)` | 5 minutes, no jitter | value TTL minus random `[0, jitter)`; `jitter` below `ttl` |
| `WithNegativeTTL(ttl)` | not cached | cache `ErrNotFound` for `ttl` |
| `WithStaleIfError(maxStale)` | disabled | on loader error, serve an L1 value expired less than `maxStale` ago; never an invalidated one |
| `WithInvalidationBus(channel)` | `wake:inval:<namespace>` | pub/sub channel; same everywhere and in `cache_invalidator.bus_channel` |
| `WithLockTimeout(d)` | 5 s | load lock lifetime and wait; keep above loader latency |
| `WithDegradedL1TTL(d)` | 5 s | max L1 age while Redis or the bus is down |
| `WithMaxPendingInvalidations(n)` | 10,000 | deferred invalidations during an outage. `0`: `Delete` and `Set` return an error instead |
| `WithCompression(minBytes)` | off | zstd for L2 payloads of at least `minBytes` |
| `WithMetrics(registerer)` | not exported | register `wake_cache_*` metrics |
| `WithClock(clock)` | system clock | time source (tests) |

Runnable examples are in `pkg/cache/example_test.go`.

## The loader contract

- **Read the primary database.** A replica can re-cache an old row until the TTL.
- **Return `cache.ErrNotFound` for an absent row.**
- **Do not panic**; it crashes the process.
- **Treat `V` as immutable.** Copy before modifying.

## Consistency model

- After `Delete(key)` returns nil, no instance serves an older value.
- With the invalidator, changes reach every instance within pipeline latency (p99 18.5 ms in the
  v0.1.0 [benchmark](../../bench/RESULTS.md)). Until then, readers may see the old value.
- Otherwise the TTL applies.

Known gaps:

- **Truncates are not invalidated.**
- **Redis eviction**: use `maxmemory-policy noeviction` or `volatile-*`, or old values can
  reappear.

## Redis outages

Readers never get Redis errors. While Redis or the bus is down:

- `Get` misses; `GetOrLoad` loads per instance.
- L1 entries live at most `WithDegradedL1TTL` (default 5 s).
- `Delete` returns `cache.ErrDeferred` and replays on recovery. Retry until nil if you need a
  global delete.
- `Set` returns an error wrapping `ErrDeferred`.

Expect a burst of loads after recovery.

## Invalidating from change events

### `wake cache-invalidator`

Reads events from Kafka or NATS and deletes the rendered keys
([keys](configuration.md#cache_invalidator)).

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

```bash
wake cache-invalidator --config wake.yaml
```

- Namespace, channel and rendered keys must match your application.
- Wake needs a sink for the tables.
- Several invalidators can share one consumer group.
- The first start reads each topic from the beginning.

Templates use `{column}` placeholders and render for both `before` and `after`, so old and new
keys are invalidated.

**Non-key columns need `REPLICA IDENTITY FULL`**, or the old key cannot be rendered
(`template_requires_full_identity`):

```sql
ALTER TABLE public.users REPLICA IDENTITY FULL;
```

`validate_dsn_env` checks this at startup. At runtime, `/readyz` returns 503 until restart.

Offsets are committed only after `Delete` succeeds, so nothing is lost while Redis is down.
`r` events are skipped unless `invalidate_on_snapshot: true`. `/readyz` checks `redis`,
`source` and `templates` ([troubleshooting](troubleshooting.md#cache-and-invalidator)).

### Inside your application

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

Also: `cdcsource.NewNATSSource`, `Invalidator.Ready(ctx)`, `Mapping.CheckReplicaIdentity`.

## Metrics

Registered with `WithMetrics`, labeled with `namespace`:

| Metric | Type | Meaning |
|---|---|---|
| `wake_cache_hits_total{namespace,tier}` | counter | hits by tier (`l1`, `l2`) |
| `wake_cache_misses_total{namespace,tier}` | counter | misses by tier |
| `wake_cache_loads_total` | counter | loader calls |
| `wake_cache_lock_waits_total` | counter | waits for another instance's load |
| `wake_cache_fallback_loads_total` | counter | loads after `lock_timeout` expired |
| `wake_cache_stale_serves_total` | counter | stale values served on loader error |
| `wake_cache_l2_errors_total` | counter | failed Redis operations |
| `wake_cache_invalidations_received_total` | counter | bus messages received |
| `wake_cache_deferred_invalidations_total` | counter | invalidations deferred during outage |
| `wake_cache_pending_overflows_total` | counter | deferred invalidations over the bound |
| `wake_cache_epoch_changes_total` | counter | full-namespace invalidations |
| `wake_cache_breaker_state` | gauge | 0 closed, 1 open, 2 half-open |
| `wake_cache_pending_invalidations` | gauge | keys awaiting invalidation |
| `wake_cache_bus_connected` | gauge | 1 while the bus is up |

Invalidator (and `cdcsource` with `WithMetrics`):

| Metric | Type | Meaning |
|---|---|---|
| `wake_cdcsource_events_total` | counter | messages received |
| `wake_cdcsource_events_skipped_total{reason}` | counter | skipped: `tombstone`, `invalid`, `unknown_op`, `truncate`, `snapshot`, `unmapped_table`, `no_keys` |
| `wake_cdcsource_keys_deleted_total` | counter | keys deleted |
| `wake_cdcsource_delete_failures_total{kind}` | counter | failed deletes, retried: `deferred` or `error` |
| `wake_cdcsource_template_requires_full_identity_total{table}` | counter | events missing a template column |

Alert on `wake_cache_breaker_state != 0`, `wake_cache_bus_connected == 0` and
`rate(wake_cdcsource_delete_failures_total[5m]) > 0`.

## Operating notes

- **Redis**: 5 or later; single node, Sentinel or Cluster. The empty key `""` fails on Cluster.
- If `wake_cache_fallback_loads_total` grows, your loader is slower than `WithLockTimeout`.
- Set Redis TLS and auth on your `redis.UniversalClient`.
