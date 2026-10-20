# Sinks

Configure where Wake sends events: `kafka`, `nats`, `webhook` or `stdout`. Run any number side by
side. All keys are in [configuration.md](configuration.md#sinks).

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
    max_events: 1000        # batch size in events
    max_bytes: 1MiB         # batch size in bytes
    linger: 10ms            # batch wait
    max_event_bytes: 1MiB   # larger events block the sink (event_too_large)
    kafka: { ... }          # the section named after the type
```

Table events go to `<prefix>.<schema>.<table>`, schema changes to `<prefix>._schema` and
messages to `<prefix>._messages` ([schema events](consuming-events.md#schema-events)). One key
stays in order. Metrics: `wake_sink_*` ([operations.md](operations.md#metrics)).

## Kafka

```yaml
- name: kafka-main
  type: kafka
  kafka:
    brokers: ["kafka-1:9092", "kafka-2:9092"]
    topic_prefix: cdc
    tombstones: true           # null-value record after each delete
    auto_create_topics: false  # missing topic blocks the sink (sink_fatal)
    partitions: 6              # -1 (default): the broker's num.partitions
    replication_factor: 3      # -1 (default): the broker's default.replication.factor
    tls: { enabled: true, ca_file: /etc/wake/kafka-ca.pem }
    sasl: { mechanism: SCRAM-SHA-512, username_env: KAFKA_USER, password_env: KAFKA_PASSWORD }
```

The key is the canonical key JSON (`{"id":42}`). Recommended topic settings:

- Replication factor 3 and `min.insync.replicas` 2.
- `cleanup.policy=compact` for topics consumers rebuild state from.
- **Never change the partition count**: it breaks per-key order.
- Topic `max.message.bytes` and broker `message.max.bytes` at least `max_event_bytes`
  (default 1 MiB).

## NATS JetStream

```yaml
- name: nats-main
  type: nats
  tables: ["public.orders"]
  nats:
    url: nats://nats-1:4222,nats://nats-2:4222
    subject_prefix: cdc
    create_stream: true        # create or update the stream
    stream: wake_app           # default wake_<source.name>
    duplicate_window: 2m       # JetStream deduplication window
    credentials_file: /etc/wake/nats.creds   # re-read on reconnect
```

- With `create_stream: false` the stream must exist. Set retention, replicas and limits on the
  stream yourself for production.
- Consume with a durable consumer filtered on, for example, `cdc.public.orders`.

## Webhook

```yaml
- name: hooks
  type: webhook
  tables: ["public.users"]
  webhook:
    url: https://example.internal/hooks/cdc
    secret_env: WAKE_WEBHOOK_SECRET   # env var with the HMAC secret
    timeout: 10s                      # per request
    lanes: 8                          # parallel lanes by key
    max_attempts: 10                  # poison responses before dead-lettering
    dead_letter_dir: ./dlq     # opt-in; default "" (poison batches block the sink)
```

Wake POSTs a JSON array of events:

```http
POST /hooks/cdc HTTP/1.1
Content-Type: application/json
Wake-Delivery-Id: 01JABCDEFGHJKMNPQRSTVWXYZ0
Wake-Signature: t=1760600000,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd

[{"id":"0/16B3748-3","source":{"db":"app","schema":"public","table":"users"},"op":"c", ...}]
```

| Response | Meaning | Wake does |
|---|---|---|
| 2xx | accepted | acknowledges the batch |
| 408, 429, 5xx, network error, timeout | transient | retries forever; honours `Retry-After` (seconds) on 429 and 5xx |
| any other status (400, 401, 403, 404, 422, ...) | **poison** | up to `webhook.max_attempts`, then dead-letters or blocks |

Reply 2xx only after storing the batch. **Expect concurrent requests** (one per lane).
Retries keep the same `Wake-Delivery-Id`.

**Poison batches** after `max_attempts`:

- `dead_letter_dir` empty (default): the sink blocks (`sink_fatal`, `/readyz` 503) and keeps
  retrying. Nothing is dropped.
- `dead_letter_dir` set: written to `<dir>/<Wake-Delivery-Id>.jsonl` and acknowledged.
  **This breaks per-key order.** Replay the files yourself in `seq` order.

### Verifying signatures

```text
Wake-Signature: t=<unix seconds>,v1=<hex>
v1 = hex( HMAC-SHA256( secret, t + "." + raw request body ) )
```

The receiver must:

1. Compute the HMAC over the **raw** body, before parsing.
2. Compare in constant time with every `v1` (several appear during secret rotation).
3. Reject a `t` too far from its clock (5 minutes is a sensible tolerance).
4. Only then parse the body.

Verifiers for Go, Python and Node are in [examples/webhook-verify](../../examples/webhook-verify/README.md).
**Use `https`**: the signature does not encrypt the payload.

## stdout

`type: stdout` prints one JSON line per event, for debugging.

## Error classification

| Class | Examples | Wake's reaction |
|---|---|---|
| Retriable | network, broker down, leader change, not enough replicas, timeouts, HTTP 408 / 429 / 5xx, NATS disconnects | state `retrying`; `/readyz` stays 200; `max_ack_age` alert covers long outages |
| Non-retriable | over `max_event_bytes` (`event_too_large`); message too large; auth failure; certificate error; missing Kafka topic; missing NATS stream; webhook poison batch; failed dead-letter write (`sink_fatal`) | state `fatal`, `/readyz` 503, `sink_fatal` alert; Wake stops reading |

Fixing the destination resumes delivery. Fixes in Wake's config need a restart
([errors.md](errors.md#sinks-and-readiness)).

## Duplicates

Every sink can deliver duplicates ([delivery-guarantees.md](delivery-guarantees.md)).

- **NATS**: duplicates appear after outages **longer than `nats.duplicate_window`** (default
  2 minutes).
- **Webhook**: retries keep the same `Wake-Delivery-Id`.
- **All sinks**: snapshot replays get new ids. Upsert by key ordered by `seq`.
