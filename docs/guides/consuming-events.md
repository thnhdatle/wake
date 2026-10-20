# Consuming events

How to read Wake's events correctly. This guide is the reference for the CDC
event format v1 (JSON schemas in [`api/schema`](../../api/schema)); the Go package
[`pkg/event`](../../pkg/event) implements it. Read
[delivery-guarantees.md](delivery-guarantees.md) first for what the transport
promises: events are delivered at least once, in commit order per key.

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
| `id` | Unique per source; the idempotency key. Stream events: `<commit LSN>-<index in transaction>`; snapshot events: `<LSN>-r<run>-<n>`; schema-change events: `<commit LSN>-s<index>` |
| `source` | `db` is Wake's `source.name`; `schema` and `table` are the table's names (after `tables.name_map`) |
| `op` | `c` create, `u` update, `d` delete, `r` snapshot read, `t` truncate (only with `events.emit_truncate`). Ignore and count any other value |
| `ts_ms` | Commit time of the transaction, in Unix milliseconds. For snapshot events, see [Snapshot events](#snapshot-events) |
| `lsn` | Commit LSN, hexadecimal `X/X` |
| `tx_id` | Postgres transaction id; absent on `r` events |
| `key` | The primary-key (or replica-identity) columns. `null` for `t` events and for keyless tables (`emit_unkeyed`) |
| `before` | `u` and `d` only; `null` for `c` and `r`. With `REPLICA IDENTITY FULL`: all columns. With the default identity: only the key columns, other columns are **omitted** (not `null`) |
| `after` | `c`, `u`, `r`; `null` for `d` |
| `unchanged_columns` | Optional; see [Unchanged columns](#unchanged-columns) |
| `schema_version` | Per-table counter that never decreases ([schema-changes.md](schema-changes.md)) |

Column values are JSON as described in [type-mapping.md](type-mapping.md):
for example `numeric` as a string, `bytea` as base64, timestamps as RFC 3339 text.
Unknown fields may be added in later versions; ignore what you do not know. The JSON
Schema is [api/schema/cdc-event.v1.json](../../api/schema/cdc-event.v1.json).

## Operations

| `op` | What to do |
|---|---|
| `c` | Insert or upsert `after` by `key` |
| `u` | Upsert `after` by `key`; if the key changed, delete the old key first ([below](#key-changing-updates)); fill `unchanged_columns` from the previous image |
| `d` | Delete by `key` (or `before`) |
| `r` | Upsert `after` by `key` (a snapshot row) |
| `t` | Truncate of a table, only when enabled. It has no key and covers the whole table; decide yourself whether to clear your copy |
| anything else | Ignore and count |

## Ids, `seq` and ordering

The **sequence** of an event, `seq = (lsn, k)`, is a total order over all events of
a source: compare the LSN numerically, then `k`, which is the index in the
transaction for stream events and the counter `n` for snapshot events. A
schema-change event sorts before the data event with the same `seq`.

- Compare LSNs **numerically**, never as strings. They are hexadecimal with a variable
  number of digits, so `"0/16B3748" < "0/9"` is true as text and wrong as a number.
  `event.ParseSeq(id)` and `event.Compare` do it right.
- For one key, events arrive in commit order. Two changes to the same row in one
  transaction are ordered by `k`.
- Across keys and tables there is no delivery order guarantee; use `seq` if you need
  one.
- Initial-snapshot `r` events share the consistent-point LSN; watermark snapshot
  events carry the LSN of their HIGH watermark. In both cases any stream event for the
  same key with a greater `seq` supersedes them.

## Idempotent consumers

Delivery is at-least-once, so any consumer sees duplicates after a crash or a retry.
Two ways to be correct:

1. **Deduplicate by `id`.** Remember the ids you applied (a unique index, or a
   bounded set of recent ones). This removes duplicates of stream events, which keep
   the same `id` after a restart.
2. **Upsert by key, ordered by `seq`.** Store the `seq` of the event that produced
   each row, and ignore an event whose `seq` is not greater. This needs no id set,
   survives replays, and is **required** for snapshot events, which are not
   deduplicated by id: a re-executed snapshot chunk gets new ids
   ([delivery-guarantees.md](delivery-guarantees.md#duplicates-and-how-to-remove-them)).

The second approach is the safer default. These rules give a correct replica:
apply events per key in `seq` order as upserts; a `d` deletes; a `u` whose `before`
key differs from `key` deletes the `before` key first; Kafka tombstones are skipped;
`unchanged_columns` keep the previous value.

A complete small consumer (a replica of all tables in memory):

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

Keep the `seq` of deleted rows (as above) at least for as long as snapshot replays or
old events can still arrive; in a database use `INSERT ... ON CONFLICT DO UPDATE ...
WHERE excluded.seq > t.seq` with the `seq` stored as two columns (LSN as a number and
`k`).

## Key-changing updates

When an update changes a primary-key column, the event has `op: "u"`, `key` is the
**new** key and `before` holds the **old** key columns:

```json
{"id":"0/16B3A00-0","source":{"db":"app","schema":"public","table":"users"},"op":"u","ts_ms":1760600003000,"lsn":"0/16B3A00","tx_id":7815,"key":{"id":43},"before":{"id":42},"after":{"id":43,"email":"b@example.com"},"schema_version":3}
```

A consumer that only upserts by `key` leaves the row `42` behind. **A `u` whose `before`
key differs from `key` deletes the `before` key first**, then upserts the new key
(`Event.KeyChanged()` tells you). The event is partitioned by the new key, so it can be in
another partition than earlier events of the old key: compare `seq` rather than relying on
arrival order across the two keys. With the default replica identity Postgres sends the old
key only when it changed; Wake otherwise takes `before` from the new row, so `before`
always has the key columns for an update.

## Unchanged columns

Postgres does not resend a large (TOAST) value that an update did not change. With the
default replica identity, such a column is **omitted from `after`** and listed in
`unchanged_columns`:

```json
{"id":"0/16B3900-1","source":{"db":"app","schema":"public","table":"docs"},"op":"u","ts_ms":1760600002000,"lsn":"0/16B3900","tx_id":7814,"key":{"id":7},"before":{"id":7},"after":{"id":7,"title":"new"},"unchanged_columns":["body"],"schema_version":1}
```

Keep your previous value of those columns. `event.MergeUnchanged(prev, e)` fills them
from the previous row image (`prev` is a `Row`, `e` the `*Event` from `Decode`): it copies each listed column from `prev`
into `e.After`, removes the resolved names from `UnchangedColumns`, and sets the field to
nil once all are resolved. Columns for which `prev` has no value stay listed. With
`REPLICA IDENTITY FULL` Wake copies the value from `before` itself and never emits
`unchanged_columns`.

## Integers above 2^53

`int2`, `int4` and `int8` are JSON numbers, and `int8` values can exceed 2^53, the largest
integer a double represents exactly. **Never decode them as floating-point numbers.** The
digits in the event are exact; a parser that goes through `float64` rounds them:

```json
{"key":{"id":9223372036854775807},"after":{"id":9223372036854775807,"amount":9007199254740993}}
```

- **Go**: decode with `event.Decode`, which keeps values as raw JSON, and read them with
  `Value.Int64()`, `Value.BigInt()`, `Value.Number()` (a `json.Number`) or `Value.Text()`.
  If you use `encoding/json` directly, call `Decoder.UseNumber()` and never decode into
  `interface{}`/`float64`.
- **JavaScript**: `JSON.parse` rounds. Use a lossless parser (for example the
  `lossless-json` package), or on engines with JSON source text access read the original
  digits in a reviver:

  ```js
  const e = JSON.parse(text, (key, value, context) =>
    typeof value === 'number' && /^-?\d+$/.test(context.source) && !Number.isSafeInteger(value)
      ? BigInt(context.source)
      : value);
  ```

- **Python**: `json.loads` produces exact `int` values.
- **Java and others**: use a 64-bit integer or `BigInteger` and a parser configured for
  integers; do not read through `double`.

`numeric` columns are strings and are not affected; `float4` and `float8` are doubles by
nature.

## Deletes and Kafka tombstones

A `d` event has `after: null`, `key` and `before` (the key columns, or all columns with
`REPLICA IDENTITY FULL`). On Kafka, each `d` event is followed by a record with the same
key and a **null value** (a tombstone), so compacted topics eventually drop the key.
Consumers skip null-value records and count them: `event.Decode` returns
`event.ErrTombstone` for them, and `event.IsKafkaTombstone(value)` tests a record value.
Set `kafka.tombstones: false` to stop writing them.

## Snapshot events

`op: "r"` events carry existing rows:

```json
{"id":"0/16B3748-r1-0","source":{"db":"app","schema":"public","table":"users"},"op":"r","ts_ms":1760599999000,"lsn":"0/16B3748","key":{"id":1},"before":null,"after":{"id":1,"email":"x@example.com"},"schema_version":3}
```

- They have no `tx_id`. The id is `<LSN>-r<run id>-<n>`; `n` is unique across all tables of
  the run.
- Initial snapshot: `lsn` is the slot's consistent point and `ts_ms` the time the point was
  created. Snapshots through watermarks (added tables, resumed or re-requested
  snapshots): `lsn` and `ts_ms` are those of the chunk's HIGH watermark transaction.
- Apply `r` as an upsert. A later stream event for the key has a greater `seq` and
  supersedes it; Wake never emits an `r` value older than a stream event it already emitted
  for that key.
- **Snapshot replays are not deduplicated by id**: a re-executed chunk (a retry, a resume
  after a crash, a repeated `wake snapshot table`) gets a new HIGH LSN and so new ids. This
  is why upserting by key ordered by `seq` is the right consumer, not id bookkeeping.
- A snapshot does not announce rows that were deleted before it ran; if a downstream copy
  holds such a row, only a rebuild removes it
  ([operations.md](operations.md#when-the-slot-or-the-state-file-is-lost)).

## Events without a key

For tables captured with `tables.emit_unkeyed`, events have `key: null`, and only `c` (inserts) and
`r` (snapshot rows) exist: UPDATE and DELETE of those tables are not captured. They have no per-key
order and are spread over partitions by `id`. A consumer that needs a key skips and counts
them. Truncate events (`t`) also have `key: null`.

## Schema events

Schema-change events have `"type": "schema_change"`, an id of the form `<LSN>-s<k>` and the
column lists before and after; see [schema-changes.md](schema-changes.md). They go to
`<prefix>._schema` on Kafka and NATS and into the same array as data events on a webhook.
Consumers that do not subscribe to the schema topic detect a change from
`schema_version` going up on a data event. Forwarded logical messages
(`events.emit_messages`) have `"type": "message"`, an id `<lsn>-m<n>`, a `prefix` and
`content_base64` ([api/schema/logical-message.v1.json](../../api/schema/logical-message.v1.json)).

## Transports

| | Location of the event | Extra metadata |
|---|---|---|
| Kafka | value of the record in topic `<prefix>.<schema>.<table>` | key = canonical key JSON; headers `wake-event-id`, `wake-schema-version` |
| NATS JetStream | payload of the message on subject `<prefix>.<schema>.<table>` | `Nats-Msg-Id` = event `id` |
| Webhook | an element of the JSON array in the request body | `Wake-Signature`, `Wake-Delivery-Id` |

The Kafka key is **canonical key JSON**: UTF-8, object keys sorted by Unicode code point, no
whitespace, no HTML escaping (`<`, `>`, `&` literal), integers as exact decimals. It is what
makes equal keys land on one partition.

For a webhook, the body is an array that can mix kinds. Classify each element with
`event.KindOf`:

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
import "github.com/thnhdatle/wake/pkg/event"
```

The package depends only on the standard library and is safe for concurrent use.

| Need | API |
|---|---|
| Decode and validate a data event | `event.Decode(data) (*Event, error)`; `ErrTombstone` for an empty value, an error wrapping `ErrInvalid` for a contract violation |
| Skip unknown operations | `e.Op.Known()` |
| Schema-change events and messages | `event.DecodeSchemaChange`, `event.DecodeMessage`; classify with `event.KindOf` |
| Read column values exactly | `Row.Value(col)` returns a `Value` with `Int64`, `Uint64`, `BigInt`, `Number`, `String`, `Text`, `Bool`, `IsNull`, `Raw` |
| Order and compare | `e.Seq()`, `event.ParseSeq(id)`, `event.Compare(a, b)`; `event.ParseLSN`, `event.FormatLSN` |
| Detect a moved row | `e.KeyChanged()` |
| Fill omitted TOAST columns | `event.MergeUnchanged(prev, e)` |
| Kafka tombstone | `event.IsKafkaTombstone(value)` |

Reading a key:

```go
e, err := event.Decode(record.Value)
if err != nil {
  return err
}
v, ok := e.Key.Value("id")
if ok {
  n, err := v.Int64()    // exact; fails if the value does not fit or is not an integer
  text, _ := v.Text()    // "9223372036854775807" for any scalar, usable in a cache key
  _, _ = n, text
  _ = err
}
```

Wake's own cache invalidation uses the same decoder: see
[cache.md](cache.md#invalidating-from-change-events).
