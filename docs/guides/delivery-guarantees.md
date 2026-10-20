# Delivery guarantees

What Wake promises about your data, what it does not, and what you must do on the
consuming side.

## In short

- **At-least-once.** Every committed change in a captured table is delivered to
  every sink configured for that table, at least once. Some events may arrive more
  than once, never zero times.
- **Commit order per key.** For one row (one table and key), events reach the
  destination in the order the changes were committed.
- **Deduplicate by `id`.** Every event has a deterministic `id`; applying events
  idempotently, ordered by `seq`, gives the right result.
- **Safe position.** Wake tells Postgres it may discard WAL only up to the point
  every interested sink has acknowledged.
- **Backpressure, never dropping.** When a sink is slow, down or rejecting events,
  Wake stops reading and retries. The only way an undelivered event is
  acknowledged is the opt-in webhook dead-letter directory.
- **Not exactly-once.** Wake cannot make a Kafka write, a NATS publish or an HTTP
  call atomic with advancing the replication slot.

## At-least-once

Postgres keeps WAL for a slot until the consumer confirms a position. Wake
confirms a position only after every sink the transactions up to that position
were routed to has acknowledged them. If Wake crashes, is killed or loses its
connection, the next run starts at the confirmed position and reads again
everything after it. Those events were possibly delivered already, so they are
delivered again. The window of possible duplicates is "events after the last
confirmed position", which is small while sinks keep up.

The same holds for the initial snapshot: rows are delivered at least once, and an
interrupted snapshot resumes without restarting and without leaving rows behind.

## The confirmed LSN

The position Wake reports to Postgres, the **confirmed LSN**, is the end of the
longest prefix of transactions (in commit order) that are completely
acknowledged by every sink they were routed to. Consequences:

- A slow sink holds the position back for all sinks: Postgres retains WAL
  meanwhile. This is deliberate; see [operations.md](operations.md#slot-sizing-and-wal-retention).
- Sinks may acknowledge out of order; the position never skips a gap.
- Transactions that reach no sink (heartbeats, snapshot markers, changes to
  tables no sink is configured for) count as acknowledged and let the position
  advance.

You can watch it: `confirmed_lsn` in `wake status` and `wake_slot_lag_bytes`.

## Ordering

Per key, events are in commit order: the events of one table and key reach a Kafka
partition, a NATS subject or a webhook lane in `seq` order. This works because the
partition key is the canonical key JSON. What this does **not** give you:

- No order across different keys or different tables. Use `seq` (the position of an
  event in the total order of the source) when you need to compare events.
- A `u` event that changes the primary key is routed by the *new* key, so it can
  land in another partition than earlier events of the old key; compare `seq`
  ([consuming-events.md](consuming-events.md#key-changing-updates)).
- Events of keyless tables (`key: null`) have no per-key order; they are spread by
  `id`.
- The webhook dead-letter exception below.
- Snapshot rows (`op: "r"`) are ordered against stream events by `seq`: any stream
  event for the same key with a greater `seq` supersedes a snapshot row, and Wake
  never emits a snapshot row older than a stream event it already emitted for that
  key.

## Backpressure, never dropping

If a destination is slow, unreachable or refuses events, Wake does not discard
them:

- **Retriable errors** (network, broker unavailable, leader change, HTTP 408, 429,
  5xx): retried without limit with capped, jittered exponential backoff (100 ms up
  to 30 s). Wake stops reading from Postgres when its buffers are full, so memory
  stays bounded. The `max_ack_age` alert tells you when this lasts too long.
- **Non-retriable errors** (an event larger than `max_event_bytes`, a message too
  large for the destination, authentication failure, an unknown topic with
  auto-creation off, a webhook poison batch with no dead-letter directory): Wake
  keeps retrying at the capped backoff, stops reading, sets `/readyz` to 503 with
  the reason and fires the `sink_fatal` alert. Fixing the cause resumes delivery
  without a restart (a fix in Wake's own configuration needs one). Nothing is
  dropped.
- **The one exception** is the opt-in webhook dead-letter directory
  (`webhook.dead_letter_dir`): after `webhook.max_attempts` poison responses a
  batch is written to disk as JSONL and acknowledged. That is the only path where
  an event is acknowledged without being delivered, it breaks per-key order for the
  keys in that batch, and it is counted in `wake_sink_dead_lettered_total` and
  logged at error level. Leave it unset unless you can handle those files.

The cost of never dropping is that a stuck sink stalls the whole pipeline and WAL
accumulates on the source. Bound that with `max_slot_wal_keep_size` and alerts.

## What happens when something fails

| Event | What happens | Your data |
|---|---|---|
| Wake crashes or is killed | Restart from the confirmed position; events after it are sent again | No loss; possible duplicates, removable by `id` |
| Wake is stopped for a while | The slot keeps WAL; on restart Wake catches up | No loss while the WAL fits `max_slot_wal_keep_size` |
| Postgres restarts or fails over to the same data | The replication connection drops, Wake exits 1 and is restarted; the slot persists | No loss |
| A sink is down | Wake retries and holds back the position; other sinks wait too | No loss; lag grows |
| A sink acknowledged but Wake did not see it before dying | The event is sent again | Duplicate with the same `id` |
| A very large transaction | Assembled at commit, spilled to disk beyond `stream.max_tx_bytes`; delivered only after its commit | No loss; latency of a large transaction is its size |
| The slot is lost or invalidated, or the database is restored without it | Wake refuses to start (`slot_missing`, `slot_invalidated`) | Changes in the gap are gone, deletes included: consumers must rebuild ([operations.md](operations.md#when-the-slot-or-the-state-file-is-lost)) |
| The state file is lost while the slot exists | Wake refuses to start (`state_mismatch`) until you decide (`--reinit-state`) | No events lost; snapshot progress is discarded |

## Duplicates and how to remove them

Event ids are deterministic: a stream event's id is `<commit LSN>-<index in
transaction>`, the same on every sink and the same after a restart, because it
derives from the WAL position. A schema-change event replayed after a crash has the
same id as before. So two copies of one event carry the same `id`, and consumers
can

- deduplicate by `id` (keep a set of recent ids, or a unique index), or
- apply events as idempotent upserts by key, ordered by `seq`, which needs no id
  bookkeeping.

Wake's own deduplication helps where available: Kafka's idempotent producer
removes duplicates caused by its own retries, and the NATS sink sends the event
`id` as `Nats-Msg-Id`, so JetStream drops copies within its duplicate window.

Where deduplication by `id` **does not** work:

- **Snapshot replays.** A re-executed snapshot chunk (after a retry, a resume or
  `wake snapshot table`) gets new ids, so replayed `r` events are not duplicates by
  `id`. Upsert by key ordered by `seq`.
- **NATS beyond the duplicate window.** Re-sends after an outage or restart longer
  than `nats.duplicate_window` (default 2 minutes) are stored again.
- **A changed capture set.** Ids number the captured events of a transaction. If you
  change `tables.include` or `tables.exclude` while events are unconfirmed, replayed
  transactions can be numbered differently.

## Latency

Events of a transaction are released at its commit, in commit order, so the
delay between commit and delivery is the transport latency plus the time a large
transaction needs to be read. `wake_end_to_end_latency_seconds{sink}` measures
commit time to acknowledgement.

## Checklist for a correct consumer

1. Treat delivery as at-least-once: deduplicate by `id` or upsert by key.
2. Apply the events of a key in `seq` order; compare LSNs numerically.
3. Handle `d` events and Kafka tombstones, `r` events as upserts, key-changing
   updates and `unchanged_columns` ([consuming-events.md](consuming-events.md)).
4. Decode `int8` values exactly.
5. Monitor the lag of your consumer as well as Wake's.
