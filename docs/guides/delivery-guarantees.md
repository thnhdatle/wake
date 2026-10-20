# Delivery guarantees

Know what Wake promises about your data and what your consumer must handle.

## In short

- **At-least-once**: every committed change reaches every configured sink, maybe more than once.
- **Commit order per key**: one row's events arrive in commit order.
- **Deterministic `id`**: idempotent upserts ordered by `seq` give the right result.
- **Safe position**: Postgres discards WAL only after every sink acknowledged.
- **Backpressure, never dropping**: a failing sink stops reading and retries. Only the opt-in
  webhook dead-letter directory acknowledges undelivered events.
- **Not exactly-once.**

## At-least-once

After a crash Wake re-sends everything after the confirmed position.

## The confirmed LSN

A slow sink holds back the position for all sinks, and WAL accumulates
([slot sizing](operations.md#slot-sizing-and-wal-retention)). Watch `confirmed_lsn` in
`wake status`.

## Ordering

Not guaranteed:

- Order across keys or tables. Use `seq`.
- Key-changing updates route by the new key; compare `seq`
  ([key-changing updates](consuming-events.md#key-changing-updates)).
- Keyless tables (`key: null`) have no per-key order.
- Dead-lettered webhook batches (below).

## Backpressure, never dropping

- **Retriable errors** (network, broker down, HTTP 408, 429, 5xx): retried forever with backoff
  from 100 ms to 30 s. The `max_ack_age` alert fires if it lasts too long.
- **Non-retriable errors** (event over `max_event_bytes`, message too large, auth failure,
  missing topic, webhook poison batch): retried, `/readyz` returns 503, `sink_fatal` fires.
  Fixing the cause resumes delivery; a Wake config fix needs a restart.
- **Exception**: with `webhook.dead_letter_dir`, a batch with `webhook.max_attempts` poison
  responses goes to disk as JSONL and is acknowledged. **It breaks per-key order** and counts
  in `wake_sink_dead_lettered_total`. Leave it unset unless you handle those files.

Bound the WAL a stuck sink causes with `max_slot_wal_keep_size` and alerts.

## What happens when something fails

| Event | What happens | Your data |
|---|---|---|
| Wake crashes or is killed | restarts from the confirmed position | no loss; duplicates |
| Wake is stopped for a while | slot keeps WAL; Wake catches up | no loss while WAL fits `max_slot_wal_keep_size` |
| Postgres restarts or fails over to the same data | Wake exits 1 and is restarted | no loss |
| A sink is down | retries; other sinks wait | no loss; lag grows |
| Ack lost when Wake dies | event re-sent | duplicate, same `id` |
| A very large transaction | spilled beyond `stream.max_tx_bytes`; sent after commit | no loss; higher latency |
| Slot lost or invalidated | refuses to start (`slot_missing`, `slot_invalidated`) | **gap lost, deletes included: rebuild consumers** ([runbook](operations.md#when-the-slot-or-the-state-file-is-lost)) |
| State file lost, slot exists | refuses to start (`state_mismatch`) until `--reinit-state` | no events lost; snapshot progress discarded |

## Duplicates and how to remove them

Deduplicate by `id`, or upsert by key ordered by `seq`. Deduplicating by `id` **fails** for:

- **Snapshot replays**: re-sent `r` events get new ids. Upsert by `seq`.
- **NATS beyond the window**: re-sends after more than `nats.duplicate_window` (default 2
  minutes) are stored again.
- **A changed capture set**: changing `tables.include` or `tables.exclude` with unconfirmed
  events can renumber replayed ids.

## Latency

Events are released at commit. `wake_end_to_end_latency_seconds{sink}` measures commit to
acknowledgement.

## Checklist for a correct consumer

1. Deduplicate by `id` or upsert by key.
2. Apply a key's events in `seq` order; compare LSNs numerically.
3. Handle `d`, tombstones, `r` upserts, key-changing updates and `unchanged_columns`
   ([consuming-events.md](consuming-events.md)).
4. Decode `int8` exactly.
5. Monitor your consumer's lag too.
