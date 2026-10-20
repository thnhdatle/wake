# Schema changes

Postgres logical replication carries row changes but no DDL. Wake detects when a
table's shape changes, gives every table a schema version that only ever goes up,
stamps it on every event, and emits a schema-change event so that consumers can
react before they see rows of the new shape.

## Schema versions

Every data event carries `schema_version`, the per-table counter in force when the
row was written. On Kafka it is also the header `wake-schema-version`. The counter

- starts at 1 for a table Wake sees for the first time (and never at 0);
- increases by one whenever the table's shape changes;
- never decreases: not after a restart, not after a crash, not after
  `--reinit-state`.

A change is any difference in the **replica identity** or in a column's **name,
type, type modifier, key flag or position** (the column order). The type's display
name is not part of the comparison. Examples that bump the version: `ADD COLUMN`,
`DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN ... TYPE`, `ALTER TABLE ... REPLICA
IDENTITY ...`, changing which columns form the key, `ALTER COLUMN ... TYPE
varchar(20)` (see [Limitations](#limitations)). Changes that do not alter the shape
(indexes, constraints other than the key, defaults, comments) do not.

Versions are tracked per `<schema>.<table>` name, not per table OID. A table that
is dropped and recreated under the same name, or rewritten, keeps counting from its
last version. A renamed table starts a new history under its new name.

## Schema-change events

For every observed change Wake emits one schema-change event, ordered **immediately
before the first data event that uses the new shape**:

```json
{
  "type": "schema_change",
  "id": "0/16C0000-s0",
  "source": {"db": "app", "schema": "public", "table": "users"},
  "lsn": "0/16C0000",
  "ts_ms": 1760600100000,
  "schema_version": 4,
  "columns_before": [{"name": "id", "type": "int8"}, {"name": "email", "type": "text"}],
  "columns_after": [{"name": "id", "type": "int8"}, {"name": "email", "type": "text"}, {"name": "country", "type": "text"}]
}
```

| Field | Meaning |
|---|---|
| `type` | Always `"schema_change"`; it distinguishes the event from data events, which have no `type` |
| `id` | `<commit LSN>-s<index in transaction>`: the id and `seq` of the data event it precedes, with an `s` marker. A schema event sorts before the data event with the same `seq` |
| `lsn`, `ts_ms` | Commit LSN and commit time of that first data event |
| `schema_version` | The new version; the data events that follow carry it |
| `columns_before` | The previous columns (name and Postgres type name), or `null` when the previous shape is not known |
| `columns_after` | The new columns, in order |

The JSON Schema is [api/schema/schema-change.v1.json](../../api/schema/schema-change.v1.json).

**Where they go.** Kafka: topic `<kafka.topic_prefix>._schema`, with the message key
the canonical JSON of `{"schema","table"}`. NATS: subject `<nats.subject_prefix>._schema`.
Webhook: in the same array as data events, with `"type": "schema_change"`. A sink
receives the schema events of the tables it receives, and no others.

**Consumers that do not subscribe** to the `_schema` topic can detect changes
in-band: a data event whose `schema_version` is higher than the last one seen for
that table means the shape changed.

### When the event appears

pgoutput sends a table's definition only before the next row change that uses it. So
the schema-change event is emitted at the first write after the DDL, with that
write's `lsn`, `ts_ms` and `index_in_tx`. **A DDL statement without a later write
produces no event until the next write.** Several DDL statements between two writes
produce one event whose `columns_before` and `columns_after` span all of them.

Replays are safe: schema versions are persisted only once the transaction that
carried the change is confirmed, so after a crash the change is detected again and
the event is re-emitted with the **same id**; deduplicate it like any event.

### Per statement

| Statement, then a write | `schema_version` | Event |
|---|---|---|
| `ADD COLUMN country text` | +1 | `columns_after` has `country`; later events carry the column (existing rows are not re-sent) |
| `DROP COLUMN country` | +1 | `country` is gone from the column list and from later events |
| `RENAME COLUMN a TO b` | +1 | The lists show `a` replaced by `b`; later events use `b` |
| `ALTER COLUMN x TYPE bigint` | +1 | The type changes in the list; later events are decoded with the new type |
| `CREATE INDEX`, `ADD CHECK` | unchanged | none |

Wake keeps running through all of them; there is nothing to restart.

## Restarts and `--reinit-state`

The versions and last known columns of every table are in the state file. On
restart Wake compares the first definition it receives for each table with the
stored columns: a difference (a DDL while Wake was down) produces a schema-change
event as usual.

`wake run --reinit-state` is used when the state file was lost or must be reset
([operations.md](operations.md#when-the-slot-or-the-state-file-is-lost)). The shape
history is gone, but versions must still never decrease, so Wake seeds them with the
clock:

- each table's new version is `max(stored version if any, current Unix time in
  seconds)`;
- the same value becomes a floor, which is stored, so a table first seen later also
  starts at or above it.

After a reinit, versions look like timestamps (around `1.7e9` and growing). Consumers
must treat versions as opaque ordered numbers: compare them, never assume small
steps. No schema-change event is emitted for the reseeding itself, and a table with no
stored columns only records them from its first definition.

## Limitations

- **Type-modifier-only changes.** A change such as `varchar(10)` to `varchar(20)` bumps
  `schema_version`, but schema-change events list column names and type names without
  modifiers, so `columns_before` and `columns_after` look identical. Detect it from the
  bumped version.
- **No DDL text.** Events describe the new shape, not the statement that caused it.
- **No event without a write.** See above.
- **Per-name history.** Versions follow the table name; a rename starts over at 1 (or at
  the floor after a reinit) under the new name.
- **Unknown types.** A column of a type outside the type mapping appears with its
  Postgres type name and its value in text form
  ([type-mapping.md](type-mapping.md)).

See also [consuming-events.md](consuming-events.md#schema-events) for how a consumer
reacts, and [type-mapping.md](type-mapping.md) for how each type is
represented.
