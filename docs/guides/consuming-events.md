# Consuming events

Write a consumer that applies Wake's events correctly (format v1: [JSON schemas](../../api/schema),
Go package [`pkg/event`](../../pkg/event)).

- [The event](#the-event)
- [Operations](#operations)
- [Ids, `seq` and ordering](#ids-seq-and-ordering)
- [Idempotent consumers](#idempotent-consumers)
- [Key-changing updates](#key-changing-updates)
- [Unchanged columns](#unchanged-columns)
- [Integers above 2^53](#integers-above-253)
- [Deletes and Kafka tombstones](#deletes-and-kafka-tombstones)
- [Snapshot events](#snapshot-events)
- [Events without a key](#events-without-a-key)
- [Schema events](#schema-events)
- [Transports](#transports)
- [Using `pkg/event`](#using-pkgevent)

## The event

```json
{
  "id": "0/16B3748-3",
  "source": {"db": "app", "schema": "public", "table": "users"},
  "op": "c",
  "ts_ms": 1760600000000,
  "lsn": "0/16B3748",
  "tx_id": 7812,
  "key": {"id": 42},
  "before": null,
  "after": {"id": 42, "email": "a@example.com"},
  "schema_version": 3
}
```

| Field | Meaning |
|---|---|
| `id` | unique per source; idempotency key. Stream `<commit LSN>-<index>`, snapshot `<LSN>-r<run>-<n>`, schema change `<commit LSN>-s<index>` |
| `source` | `db` = `source.name`; `schema`, `table` after `tables.name_map` |
| `op` | `c` create, `u` update, `d` delete, `r` snapshot read, `t` truncate (with `events.emit_truncate`). Ignore and count others |
| `ts_ms` | commit time, Unix milliseconds ([snapshots](#snapshot-events) differ) |
| `lsn` | commit LSN, hex `X/X` |
| `tx_id` | Postgres transaction id; absent on `r` |
| `key` | key columns; `null` for `t` and keyless tables |
| `before` | `u` and `d` only. All columns with `REPLICA IDENTITY FULL`, else only key columns (others **omitted**, not `null`) |
| `after` | `c`, `u`, `r`; `null` for `d` |
| `unchanged_columns` | optional; see [below](#unchanged-columns) |
| `schema_version` | per-table, never decreases ([schema-changes.md](schema-changes.md)) |

Values follow [type-mapping.md](type-mapping.md). Ignore unknown fields.

## Operations

| `op` | What to do |
|---|---|
| `c` | upsert `after` by `key` |
| `u` | upsert `after` by `key`; delete the old key first if it changed; fill `unchanged_columns` |
| `d` | delete by `key` |
| `r` | upsert `after` by `key` |
| `t` | whole-table truncate, no key; decide whether to clear your copy |
| anything else | ignore and count |

## Ids, `seq` and ordering

`seq = (lsn, k)` orders all events of a source.

- **Compare LSNs numerically, never as strings.** `"0/16B3748" < "0/9"` is true as text.
  Use `event.ParseSeq(id)` and `event.Compare`.
- One key: commit order. Across keys and tables: no order; use `seq`.
- A stream event with a greater `seq` supersedes a snapshot event for the same key.

## Idempotent consumers

Expect duplicates. **Upsert by key, ordered by `seq`**: ignore events whose `seq` is not
greater than the stored one. Deduplicating by `id` misses snapshot replays
([duplicates](delivery-guarantees.md#duplicates-and-how-to-remove-them)).

A complete in-memory replica:

```go
package main

import (
  "errors"
  "slices"
  "strings"

  "github.com/thnhdatle/wake/pkg/event"
)

type versioned struct {
  after event.Row // nil: deleted
  seq   event.Seq
}

type state struct{ rows map[string]versioned }

// rowID identifies a row: the table plus the values of the key columns.
func rowID(src event.Source, cols []string, img event.Row) string {
  var b strings.Builder
  b.WriteString(src.Schema + "." + src.Table)
  for _, c := range cols {
    b.WriteString("|" + string(img[c]))
  }
  return b.String()
}

// Apply applies one Kafka record value. Applying it twice changes nothing.
func (s *state) Apply(value []byte) error {
  e, err := event.Decode(value)
  switch {
  case errors.Is(err, event.ErrTombstone):
    return nil // Kafka tombstone: skip and count
  case err != nil:
    return err
  case !e.Op.Known() || e.Op == event.OpTruncate || e.Key == nil:
    return nil // unknown op, truncate or keyless event: skip and count
  }
  seq, err := e.Seq()
  if err != nil {
    return err
  }
  cols := make([]string, 0, len(e.Key))
  for c := range e.Key {
    cols = append(cols, c)
  }
  slices.Sort(cols)

  if e.KeyChanged() { // the row moved: delete the old key first
    s.write(rowID(e.Source, cols, e.Before), versioned{seq: seq})
  }
  id := rowID(e.Source, cols, e.Key)
  if e.Op == event.OpDelete {
    s.write(id, versioned{seq: seq})
    return nil
  }
  if prev, ok := s.rows[id]; ok {
    event.MergeUnchanged(prev.after, e) // fill unchanged_columns from the previous image
  }
  s.write(id, versioned{after: e.After, seq: seq})
  return nil
}

// write keeps the newer of the stored and the new version of a row.
func (s *state) write(id string, v versioned) {
  if prev, ok := s.rows[id]; ok && event.Compare(prev.seq, v.seq) >= 0 {
    return // an equal or newer event was applied already: duplicate or replay
  }
  s.rows[id] = v
}
```

Keep the `seq` of deleted rows while replays can arrive. In SQL, store `seq` as two columns and
use `INSERT ... ON CONFLICT DO UPDATE ... WHERE excluded.seq > t.seq`.

## Key-changing updates

When an update changes the primary key, `key` is the **new** key and `before` holds the
**old** one:

```json
{"id":"0/16B3A00-0","source":{"db":"app","schema":"public","table":"users"},"op":"u","ts_ms":1760600003000,"lsn":"0/16B3A00","tx_id":7815,"key":{"id":43},"before":{"id":42},"after":{"id":43,"email":"b@example.com"},"schema_version":3}
```

**Delete the `before` key first, then upsert the new key** (`Event.KeyChanged()`). Compare
`seq` across both keys; they may be in different partitions.

## Unchanged columns

An unchanged large (TOAST) value is **omitted from `after`** and listed in
`unchanged_columns`:

```json
{"id":"0/16B3900-1","source":{"db":"app","schema":"public","table":"docs"},"op":"u","ts_ms":1760600002000,"lsn":"0/16B3900","tx_id":7814,"key":{"id":7},"before":{"id":7},"after":{"id":7,"title":"new"},"unchanged_columns":["body"],"schema_version":1}
```

Keep your previous value (`event.MergeUnchanged(prev, e)`). With `REPLICA IDENTITY FULL` the
field never appears.

## Integers above 2^53

`int8` values can exceed 2^53. **Never decode them as floating-point numbers**; they get
rounded.

```json
{"key":{"id":9223372036854775807},"after":{"id":9223372036854775807,"amount":9007199254740993}}
```

- **Go**: `event.Decode`, then `Value.Int64()`, `Value.BigInt()`, `Value.Number()` or
  `Value.Text()`. With `encoding/json`, use `Decoder.UseNumber()`.
- **JavaScript**: `JSON.parse` rounds. Use a lossless parser (such as `lossless-json`) or a
  reviver with source text access:

  ```js
  const e = JSON.parse(text, (key, value, context) =>
    typeof value === 'number' && /^-?\d+$/.test(context.source) && !Number.isSafeInteger(value)
      ? BigInt(context.source)
      : value);
  ```

- **Python**: `json.loads` is exact.
- **Java and others**: 64-bit integers or `BigInteger`, never `double`.

`numeric` is a string and not affected.

## Deletes and Kafka tombstones

A `d` event has `after: null`. On Kafka a tombstone (null value) follows. Skip it:
`event.Decode` returns `event.ErrTombstone`. Disable with `kafka.tombstones: false`.

## Snapshot events

```json
{"id":"0/16B3748-r1-0","source":{"db":"app","schema":"public","table":"users"},"op":"r","ts_ms":1760599999000,"lsn":"0/16B3748","key":{"id":1},"before":null,"after":{"id":1,"email":"x@example.com"},"schema_version":3}
```

- Apply `r` as an upsert by `seq`. **Replays get new ids.**
- Rows deleted before a snapshot are not announced; only a
  [rebuild](operations.md#when-the-slot-or-the-state-file-is-lost) removes them.

## Events without a key

`tables.emit_unkeyed` tables emit only `c` and `r`, with `key: null`. Truncates also have
`key: null`.

## Schema events

Schema-change events have `"type": "schema_change"` and go to `<prefix>._schema`
([schema-changes.md](schema-changes.md)). Or watch `schema_version` go up.

Logical messages (`events.emit_messages`) have `"type": "message"`
([schema](../../api/schema/logical-message.v1.json)).

## Transports

| | Location of the event | Extra metadata |
|---|---|---|
| Kafka | record value in `<prefix>.<schema>.<table>` | key = canonical key JSON; headers `wake-event-id`, `wake-schema-version` |
| NATS JetStream | message payload on `<prefix>.<schema>.<table>` | `Nats-Msg-Id` = event `id` |
| Webhook | element of the JSON array body | `Wake-Signature`, `Wake-Delivery-Id` |

A webhook body mixes kinds. Classify each element:

```go
var batch []json.RawMessage
if err := json.Unmarshal(body, &batch); err != nil {
  return err
}
for _, raw := range batch {
  kind, err := event.KindOf(raw)
  if err != nil {
    return err
  }
  switch kind {
  case event.KindEvent:
    e, err := event.Decode(raw) // c, u, d, r, t
    // ...
  case event.KindSchemaChange:
    sc, err := event.DecodeSchemaChange(raw)
    // ...
  case event.KindMessage:
    m, err := event.DecodeMessage(raw)
    // ...
  }
}
```

Verify the signature before parsing ([sinks.md](sinks.md#verifying-signatures)).

## Using `pkg/event`

```go
import "github.com/thnhdatle/wake/pkg/event"   // standard library only; safe for concurrent use
```

| Need | API |
|---|---|
| Decode and validate | `event.Decode(data) (*Event, error)`; `ErrTombstone` if empty, `ErrInvalid` on violation |
| Skip unknown operations | `e.Op.Known()` |
| Schema changes and messages | `event.DecodeSchemaChange`, `event.DecodeMessage`, `event.KindOf` |
| Read values exactly | `Row.Value(col)`: `Int64`, `Uint64`, `BigInt`, `Number`, `String`, `Text`, `Bool`, `IsNull`, `Raw` |
| Order and compare | `e.Seq()`, `event.ParseSeq(id)`, `event.Compare(a, b)`, `event.ParseLSN`, `event.FormatLSN` |
| Detect a moved row | `e.KeyChanged()` |
| Fill omitted TOAST columns | `event.MergeUnchanged(prev, e)` |
| Kafka tombstone | `event.IsKafkaTombstone(value)` |

Cache invalidation uses the same decoder ([cache.md](cache.md#invalidating-from-change-events)).
