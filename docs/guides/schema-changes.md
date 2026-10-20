# Schema changes

Detect and react when a captured table's columns change.

## Schema versions

Every data event carries `schema_version` (Kafka header `wake-schema-version`).

- Starts at 1 for a new table.
- Goes up by one on every shape change.
- Never decreases, even after `--reinit-state`.

Bumps it: `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN ... TYPE` (including
`varchar(20)`), `REPLICA IDENTITY` or key changes, column order. Does not: indexes, other
constraints, defaults, comments.

Versions follow the `<schema>.<table>` name. A recreated table keeps counting. A renamed table
starts over.

## Schema-change events

One event per change, right before the first data event with the new shape:

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
| `type` | always `"schema_change"`; data events have no `type` |
| `id` | `<commit LSN>-s<index in transaction>`; sorts before the data event it precedes |
| `lsn`, `ts_ms` | of that first data event |
| `schema_version` | the new version |
| `columns_before` | previous columns, or `null` if unknown |
| `columns_after` | new columns, in order |

Schema: [api/schema/schema-change.v1.json](../../api/schema/schema-change.v1.json).

**Where they go**: Kafka `<kafka.topic_prefix>._schema`, NATS `<nats.subject_prefix>._schema`,
webhook in the same array. A sink gets schema events only for its tables. Without the
`_schema` topic, watch `schema_version` go up.

### When the event appears

**A DDL statement produces no event until the next write to the table.** Several DDLs between
writes give one event. After a crash the event is re-sent with the same id.

### Per statement

| Statement, then a write | `schema_version` | Event |
|---|---|---|
| `ADD COLUMN country text` | +1 | `country` added; existing rows not re-sent |
| `DROP COLUMN country` | +1 | `country` removed |
| `RENAME COLUMN a TO b` | +1 | `a` replaced by `b` |
| `ALTER COLUMN x TYPE bigint` | +1 | new type in the list |
| `CREATE INDEX`, `ADD CHECK` | unchanged | none |

Wake keeps running; no restart needed.

## Restarts and `--reinit-state`

DDL made while Wake was down is detected on restart and emits an event.

After `wake run --reinit-state` ([runbook](operations.md#when-the-slot-or-the-state-file-is-lost)),
versions become `max(stored version, current Unix time in seconds)`. They look like timestamps.
**Treat versions as opaque ordered numbers**; never assume small steps.

## Limitations

- **Type-modifier changes** (`varchar(10)` to `varchar(20)`) bump the version but the column
  lists look identical.
- **No DDL text** in events.
- **No event without a write.**
- **Per-name history**: a rename starts over under the new name.
- **Unknown types** appear with their Postgres name and a text value
  ([type-mapping.md](type-mapping.md)).

Consumer handling: [consuming-events.md](consuming-events.md#schema-events).
