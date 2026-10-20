# Sinks

A sink is a destination for events. Wake has four types: `kafka`, `nats`,
`webhook` and `stdout`. Any number of sinks can run side by side; each receives
the tables its `tables` globs select, and every event has the same `id` on every
sink. This guide describes how each sink delivers, its options, how errors are
classified and where duplicates can occur. All keys are listed in
[configuration.md](configuration.md#sinks).

- [Common behaviour](#common-behaviour)
- [Kafka](#kafka)
- [NATS JetStream](#nats-jetstream)
- [Webhook](#webhook)
- [stdout](#stdout)
- [Error classification](#error-classification)
- [Duplicates](#duplicates)

## Common behaviour

```yaml
sinks:
  - name: kafka-main        # unique
    type: kafka
    tables: ["public.*"]    # globs on <schema>.<table>, default ["*"]; [] = none
    max_events: 1000
    max_bytes: 1MiB
    linger: 10ms
    max_event_bytes: 1MiB
    kafka: { ... }          # the section named after the type
```

- **Routing.** An event goes to every sink whose `tables` globs match its
  `<schema>.<table>`, using the names after `tables.name_map`. A sink with
  `tables: []` receives no table events.
- **Partitioning.** The partition key is the canonical key JSON, so the events of
  one key stay in order on every sink. Events with `key: null` (keyless tables,
  forwarded messages) are partitioned by `id`.
- **Destinations.** Table events go to `<prefix>.<schema>.<table>` (Kafka topic) or
  the subject of the same name (NATS). Schema-change events go to
  `<prefix>._schema`, forwarded logical messages (`events.emit_messages`) to
  `<prefix>._messages`; the webhook receives all of them in the same array, told
  apart by a `"type"` member ([consuming-events.md](consuming-events.md#schema-events)).
- **Batching.** `max_events`, `max_bytes` and `linger` control batch size and delay
  where the transport batches (see the per-sink sections).
- **Size limit.** `max_event_bytes` caps one event; a larger event blocks the sink
  (`event_too_large`), see [Error classification](#error-classification).
- **Backpressure.** Each sink holds a bounded number of unacknowledged events
  (Kafka and NATS: 10,000 events or 64 MiB); beyond that Wake stops reading.
- **Metrics.** Every sink reports `wake_sink_*` metrics labeled with its `name`
  ([operations.md](operations.md#metrics)).

Delivery is at-least-once; read [delivery-guarantees.md](delivery-guarantees.md).

## Kafka

```yaml
- name: kafka-main
  type: kafka
  kafka:
    brokers: ["kafka-1:9092", "kafka-2:9092"]
    topic_prefix: cdc
    tombstones: true
    auto_create_topics: false
    partitions: 6              # -1 (default): the broker's num.partitions
    replication_factor: 3      # -1 (default): the broker's default.replication.factor
    tls: { enabled: true, ca_file: /etc/wake/kafka-ca.pem }
    sasl: { mechanism: SCRAM-SHA-512, username_env: KAFKA_USER, password_env: KAFKA_PASSWORD }
```

**Records.**

- Topic `<topic_prefix>.<schema>.<table>`; the value is the event JSON
  ([consuming-events.md](consuming-events.md)).
- The message key is the canonical key JSON of the event (`{"id":42}`), so a row
  always maps to the same partition. Keys are hashed with Kafka's murmur2, the same
  partition the Java client picks for the same key and partition count.
- Headers: `wake-event-id` (the event `id`) and `wake-schema-version`.
- With `kafka.tombstones` (default), each `d` event is followed by a record with the
  same key and a null value, so compacted topics drop deleted keys. Consumers skip
  null-value records.
- A `t` (truncate) event has no key and is written to every partition of the
  table's topic, with the same `id` in each. They are only emitted with
  `events.emit_truncate`.
- Schema changes go to `<prefix>._schema`, keyed by the canonical JSON of
  `{"schema","table"}`.

**Producer.** Idempotent producer, `acks=all`, unlimited record retries and no
delivery timeout: transient errors (network, leader change, not enough replicas)
are retried without reordering and without limit. An event is acknowledged when
Kafka acknowledged all its records (a `d` event: also its tombstone; a `t` event:
every partition write). `max_events` is not used; `max_bytes` bounds a batch (raised
to fit one event of `max_event_bytes`, lowered to the topic's `max.message.bytes`)
and `linger` is the producer linger. When a record fails permanently the sink stops
producing, waits for in-flight records and re-produces the failed ones in order
after a capped, jittered backoff, so a fixed cause (a created topic, a raised limit)
resumes delivery without a restart.

**Topics.** Create them yourself, or set `kafka.auto_create_topics: true` (with
`partitions` and `replication_factor`; `-1` means the broker's defaults). With
auto-creation off, a missing topic blocks the sink with `sink_fatal` until it exists.
Recommendations:

- Use a replication factor of 3 and `min.insync.replicas` of 2 so `acks=all` means
  something.
- Use `cleanup.policy=compact` (with or without delete) for topics that consumers
  rebuild state from; the tombstones let deleted keys disappear.
- Keep the partition count fixed. Changing it re-maps keys to other partitions, which
  breaks per-key order across the change.
- Keep `max.message.bytes` of the topic and the broker's `message.max.bytes` at or
  above `max_event_bytes` (default 1 MiB).

**Security.** `kafka.tls` enables TLS (system roots, or `ca_file`; `cert_file` and
`key_file` for a client certificate). `kafka.sasl` supports SCRAM-SHA-256 and
SCRAM-SHA-512; user name and password are read from the environment variables named
by `username_env` and `password_env`.

## NATS JetStream

```yaml
- name: nats-main
  type: nats
  tables: ["public.orders"]
  nats:
    url: nats://nats-1:4222,nats://nats-2:4222
    subject_prefix: cdc
    create_stream: true
    stream: wake_app           # default wake_<source.name>
    duplicate_window: 2m
    credentials_file: /etc/wake/nats.creds
```

**Messages.**

- Subject `<subject_prefix>.<schema>.<table>` (also `<prefix>._schema` and
  `<prefix>._messages`); the payload is the event JSON.
- The header `Nats-Msg-Id` carries the event `id` (never the key), so JetStream
  drops copies that arrive within the stream's duplicate window. The header
  `Nats-Expected-Stream` makes a publish fail instead of landing in a different
  stream that also captures the subject.
- Wake publishes asynchronously in order and acknowledges an event on its PubAck
  (a duplicate PubAck counts: the stream already has it).
- `max_events`, `max_bytes` and `linger` do not apply; each event is one message.

**Stream.** With `create_stream: true` (default) Wake creates the stream
`nats.stream` on the subject `<subject_prefix>.>` with `nats.duplicate_window`, or,
when the stream exists, adds the subject and the window and keeps its other
settings. With `create_stream: false` the stream must exist; otherwise the sink is
blocked with `sink_fatal`. Retention, replicas and limits are yours to configure on
the stream (create it yourself for production settings).

**Consuming.** Create a durable consumer on the stream with a filter such as
`cdc.public.orders`. Wake's cache invalidator does exactly that
([cache.md](cache.md)).

**Security.** `nats.credentials_file` (re-read on every reconnect) and `nats.tls`.
Credentials never appear in the URL.

**Limits.** `max_event_bytes` defaults to the server's `max_payload`. A message
larger than the stream's limit is `sink_fatal`.

## Webhook

```yaml
- name: hooks
  type: webhook
  tables: ["public.users"]
  webhook:
    url: https://example.internal/hooks/cdc
    secret_env: WAKE_WEBHOOK_SECRET
    timeout: 10s
    lanes: 8
    max_attempts: 10
    dead_letter_dir: ./dlq     # opt-in; default "" (poison batches block the sink)
```

**Request.** Wake POSTs a JSON array of events (up to `max_events` and `max_bytes`,
after at most `linger`) to `webhook.url` with a per-request `webhook.timeout`:

```http
POST /hooks/cdc HTTP/1.1
Content-Type: application/json
Wake-Delivery-Id: 01JABCDEFGHJKMNPQRSTVWXYZ0
Wake-Signature: t=1760600000,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd

[{"id":"0/16B3748-3","source":{"db":"app","schema":"public","table":"users"},"op":"c", ...}]
```

The array holds events in order. It can also contain schema-change events and
forwarded messages, which have a `"type"` member (`"schema_change"`, `"message"`);
data events have none ([consuming-events.md](consuming-events.md#schema-events)).

**Answering.** Reply with a 2xx status once the batch is stored: that
acknowledges every event in it. The status decides what happens next:

| Response | Meaning | Wake does |
|---|---|---|
| 2xx | Accepted | Acknowledges the batch |
| 408, 429, 5xx, network error, timeout | Transient | Retries without limit with capped, jittered backoff; honours `Retry-After` (seconds) on 429 and 5xx |
| any other status (400, 401, 403, 404, 422, ...) | **Poison** | Retries up to `webhook.max_attempts` poison responses, then dead-letters or blocks (below) |

**Lanes and barriers.** Keys hash into `webhook.lanes` lanes (default 8). Each lane
has one batch in flight, so the events of one key arrive in order, and different keys
are delivered in parallel. Events without a key (schema changes, truncates, forwarded
messages) are barriers: all lanes flush, the event is sent alone, then the lanes
resume. Events of keyless tables hash by `id`. A receiver can therefore see
concurrent requests; it must be safe for that.

**Retries and duplicates.** `Wake-Delivery-Id` is a ULID per batch and stays the same
across retries of that batch, so a receiver can deduplicate batches by it; events are
also deduplicated by their own `id`. A retry after a timeout can deliver a batch that
the receiver already stored.

**Poison batches and dead letters.** A non-retriable rejection is retried until
`max_attempts` poison responses have been seen for the batch. Then:

- with `webhook.dead_letter_dir` **empty** (default): Wake keeps retrying at the capped
  backoff, sets `/readyz` to 503, reports the sink as `sink_fatal` and fires the
  fatal-sink alert. Delivery resumes by itself when the receiver accepts the batch.
  Nothing is dropped.
- with `webhook.dead_letter_dir` **set**: the batch is written to
  `<dir>/<Wake-Delivery-Id>.jsonl`, one event per line (written to a temporary file,
  synced and renamed; the directory is created with mode 0700 and the files 0600),
  then acknowledged. Delivery continues. The batch is counted in
  `wake_sink_dead_lettered_total` and logged at error level.

Dead-lettering is the only path where Wake acknowledges an event it did not deliver,
and **it breaks per-key order for the keys in the dead-lettered batch**: later events
of those keys are delivered ahead of the missing ones. Wake has no replay tool in
v0.1; replay the files yourself and order by `seq`. Put the directory on a volume
you monitor, and keep it off the state volume if it can grow.

### Verifying signatures

Every attempt carries `Wake-Signature: t=<unix seconds>,v1=<hex>` where

```text
v1 = hex( HMAC-SHA256( secret, t + "." + raw request body ) )
```

and the secret is the value of the environment variable named by `secret_env`. Wake
signs each attempt anew, so `t` is the time of the attempt. A receiver must:

1. compute the HMAC over the **raw** body bytes, before any JSON parsing or
   re-encoding;
2. compare it with every `v1` value in constant time (several may be present while a
   secret is rotated);
3. reject a `t` more than a tolerance (5 minutes is a sensible default) away from its
   own clock, which bounds replays of a captured request;
4. only then parse the body.

Ready-to-copy verifiers in Go, Python and Node, with commands and expected output, are
in [examples/webhook-verify](../../examples/webhook-verify/README.md). Use `https` for
the URL: the signature authenticates Wake to the receiver but does not encrypt the
payload.

## stdout

`type: stdout` writes each event as one JSON line to standard output, in order. It
has no options besides the common ones and is meant for trying Wake out and for
debugging, for example with `tables: ["public.users"]`. An event is acknowledged
when its batch has been written; a failed write is retried without limit. It adds no
durability: output that is not consumed is not kept.

## Error classification

Sinks never drop events; they retry every error without limit with capped, jittered
backoff. What differs is whether Wake considers the error fixable by waiting.

| Class | Examples | Wake's reaction |
|---|---|---|
| Retriable | network errors, broker unavailable, leader change, not enough replicas, timeouts, HTTP 408 / 429 / 5xx, NATS disconnects | Retry; the sink state is `retrying`; `/readyz` stays 200; the `max_ack_age` alert covers long outages |
| Non-retriable | an event above `max_event_bytes` (`event_too_large`); message too large for the broker, topic or stream; authentication or authorization failure; certificate error; unknown Kafka topic with auto-creation off; missing NATS stream with `create_stream: false`; webhook poison batch with no dead-letter directory; a failed dead-letter write (`sink_fatal`) | Retry at the capped backoff **and** set the sink state to `fatal`, `/readyz` to 503 with the reason, and fire the `sink_fatal` alert. Wake stops reading; the confirmed position stays |

Fixing the cause at the destination (raising a broker limit, correcting
credentials, creating the topic or stream, repairing the receiver) resumes
delivery without a restart. A fix in Wake's own configuration (`max_event_bytes`,
excluding a column or table) takes effect on restart. Remediation for each code is
in [errors.md](errors.md#sinks-and-readiness).

## Duplicates

Delivery is at-least-once, so every sink can deliver an event more than once after a
crash or a retry; consumers deduplicate by `id` or upsert by key
([delivery-guarantees.md](delivery-guarantees.md)). Sink-specific points:

- **Kafka**: the idempotent producer removes duplicates caused by its own retries;
  duplicates after a Wake restart carry the same `id` and the same key.
- **NATS**: JetStream drops a re-published message whose `Nats-Msg-Id` was seen within
  `nats.duplicate_window` (default 2 minutes). Outages or restarts **longer than the
  window** can store duplicates, as can **snapshot replays**, whose ids are new.
- **Webhook**: retries re-send the batch with the same `Wake-Delivery-Id`.
- **All sinks**: a re-executed snapshot chunk gets new ids and is not deduplicated by
  `id`; upsert by key ordered by `seq`.

Two ordering limits exist in the transports, both only reachable by an unusual
rejection pattern and both harmless to consumers that order by `seq`: Kafka, a record
produced in the instant between a broker-side rejection (message too large,
authorization) and the sink learning of it can be written before the rejected
records are retried; NATS, a transient JetStream rejection of one message (for example
no stream leader during an election) while the next message on the connection is
accepted lets the later one overtake it.
