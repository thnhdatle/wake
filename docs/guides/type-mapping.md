# Type mapping

How Wake renders Postgres column values in the JSON of a change event. The same
mapping applies to `before`, `after` and `key`, and a value renders to the same bytes
whether the row comes from the replication stream or from a snapshot. For the event
envelope, see [consuming-events.md](consuming-events.md).

- [General rules](#general-rules)
- [Session settings](#session-settings)
- [The mapping](#the-mapping)
- [Details and edge cases](#details-and-edge-cases)
- [Integers above 2^53](#integers-above-253)
- [Unknown types](#unknown-types)
- [Limits](#limits)

## General rules

- SQL `NULL` is always JSON `null`.
- Values that JSON numbers cannot carry without loss (`numeric`, `NaN`, infinities,
  dates outside RFC 3339) are strings.
- Integers are copied digit for digit and never pass through a floating-point number.
- Strings are valid JSON and valid UTF-8: escaping is as in Go's `encoding/json` with
  HTML escaping off (`<`, `>` and `&` stay literal); control characters, U+2028 and
  U+2029 are escaped, and each invalid UTF-8 byte is replaced by U+FFFD.
- Output is deterministic: the same value always renders to the same bytes.
- A type outside the mapping never stops the stream: its text form is delivered as a
  string ([Unknown types](#unknown-types)).

## Session settings

Wake reads values in Postgres's text format, which depends on session settings. The
replication connection and every SQL session Wake opens set these as startup
parameters, and options cannot override them, so a role- or database-level default
(`ALTER ROLE ... SET DateStyle`) does not change the output:

| Setting | Value | Why |
|---|---|---|
| `DateStyle` | `ISO` | Dates and timestamps are parsed from their ISO form |
| `IntervalStyle` | `postgres` | Intervals are parsed from the `postgres` form |
| `client_encoding` | `UTF8` | Strings are JSON, hence UTF-8 |
| `TimeZone` | `UTC` | Keeps the offsets of `timestamptz` stable |
| `extra_float_digits` | `1` | Floats print in the shortest round-trip form |

## The mapping

| Postgres type | JSON | Example (text to JSON) |
|---|---|---|
| `int2`, `int4`, `int8`, `oid`, `xid`, `cid`, `xid8` | number, digits verbatim (`int8` may exceed 2^53) | `9223372036854775807` to `9223372036854775807` |
| `float4`, `float8` | number; `NaN`, `Infinity`, `-Infinity` as strings | `1e+100` to `1e+100`; `NaN` to `"NaN"` |
| `numeric` | string (including `NaN`, `Infinity`, `-Infinity`) | `1234.5600` to `"1234.5600"`; `NaN` to `"NaN"` |
| `money` | string (`lc_monetary` format) | `$12.34` to `"$12.34"` |
| `bool` | `true` / `false` | `t` to `true` |
| `text`, `varchar`, `bpchar`, `name`, `"char"`, `uuid`, `xml`, `jsonpath` | string (`bpchar` keeps its padding) | `ab␠␠␠` to `"ab␠␠␠"` |
| `inet`, `cidr`, `macaddr`, `macaddr8` | string | `192.168.0.1/24` to `"192.168.0.1/24"` |
| `bytea` | string, standard padded base64 (hex or escape input) | `\xdeadbeef00` to `"3q2+7wA="` |
| `json`, `jsonb` | embedded JSON, validated, insignificant space removed, member order kept | `{"b": 1}` to `{"b":1}` |
| `date` | `"YYYY-MM-DD"` | `2026-10-18` to `"2026-10-18"` |
| `timestamp` | `"YYYY-MM-DDThh:mm:ss[.f]"` (no offset) | `2026-10-18 12:34:56.5` to `"2026-10-18T12:34:56.5"` |
| `timestamptz` | `"YYYY-MM-DDThh:mm:ss[.f]±hh:mm"` | `2026-10-18 12:34:56.123+02` to `"2026-10-18T12:34:56.123+02:00"` |
| `time` | `"hh:mm:ss[.f]"` | `12:34:56.789` to `"12:34:56.789"` |
| `timetz` | `"hh:mm:ss[.f]±hh:mm"` | `12:34:56+05:30` to `"12:34:56+05:30"` |
| `interval` | ISO 8601 duration, each component signed (as `IntervalStyle iso_8601`) | `1 year 2 mons -3 days +04:05:06.5` to `"P1Y2M-3DT4H5M6.5S"`; `-00:00:01` to `"PT-1S"`; `00:00:00` to `"PT0S"` |
| `int4range`, `int8range`, `numrange`, `daterange`, `tsrange`, `tstzrange` | object; bounds mapped by the bound type | `[1,10)` to `{"lower":1,"upper":10,"lower_inc":true,"upper_inc":false}`; `empty` to `{"empty":true}` |
| `int4multirange` ... `tstzmultirange` | array of range objects | `{[1,3),[5,7)}` to `[{...},{...}]` |
| arrays of every type above and of the other built-in types in this table | JSON array (nested for multi-dimensional arrays), elements mapped by the element type | `{1,NULL,3}` to `[1,null,3]`; `{{1,2},{3,4}}` to `[[1,2],[3,4]]`; `{"a,b","c\"d"}` to `["a,b","c\"d"]` |
| `point`, `lseg`, `path`, `box`, `polygon`, `line`, `circle`, `bit`, `varbit`, `tsvector`, `tsquery`, `pg_lsn`, `reg*` types, `tid`, `int2vector`, `oidvector`, `aclitem`, `refcursor`, `txid_snapshot`, `pg_snapshot` | string (text output) | `(1.5,2)` to `"(1.5,2)"` |
| anything else (enums, domains, composites, extension types such as `citext`, arrays of those) | string (text output); the type is unknown and counted ([below](#unknown-types)) | `happy` to `"happy"` |

A column of a domain type is mapped by the domain's own type, which is not in the
table, so it is unknown and arrives as a string even when the base type is numeric.

## Details and edge cases

- **Infinities.** Infinite dates, timestamps and `timestamptz` become the strings
  `"infinity"` and `"-infinity"`; so do Postgres 17 infinite intervals.
- **BC and large years.** BC dates use ISO 8601 astronomical year numbering with a sign
  and at least four digits: `0044-03-15 BC` becomes `"-0043-03-15"` (1 BC is year
  `0000`). Years above 9999 keep all their digits without a sign (`"10000-01-01"`).
  Such values are ISO 8601 but outside RFC 3339; use an ISO 8601 parser.
- **Offsets.** `timestamptz` offsets are rendered `±hh:mm` (`+00` becomes `+00:00`,
  `-08` becomes `-08:00`). An offset with seconds (local mean time zones before about
  1900, for example `-04:56:02`) is not representable in RFC 3339: the value is
  converted to UTC and rendered with `+00:00`; `timetz` likewise, modulo 24 hours.
  Fractional seconds are copied verbatim.
- **`time` `24:00:00`** is kept as is.
- **Interval.** Years and months come from the month total and share its sign; days are
  signed on their own; the time part has one sign applied to each of H, M and S
  (`-04:05:06` becomes `"PT-4H-5M-6S"`). Zero components are omitted, and a zero
  interval is `"PT0S"`.
- **Ranges.** An unbounded side is `null` with its `_inc` flag false (Postgres never
  reports an unbounded side as inclusive); a bound whose value is `infinity` is the
  string `"infinity"`, which is distinct from `null`.
- **Arrays.** Non-default lower bounds (`[0:1]={1,2}`) are dropped. `box` arrays use `;`
  as element delimiter, like Postgres.
- **`json` and `jsonb`.** The value is validated and embedded as JSON, with
  insignificant whitespace removed and the member order kept.
- **`bpchar`** keeps its blank padding.
- **`bytea`** accepts hex and escape input and always outputs padded standard base64.
- **Unparseable values.** A value that does not parse as its declared type is an error;
  Wake never emits a guessed value.

## Integers above 2^53

`int2`, `int4`, `int8`, `oid`, `xid`, `cid` and `xid8` are JSON numbers with their
digits verbatim. `int8` and `xid8` values can exceed 2^53 (9,007,199,254,740,992), the
largest integer a double represents exactly; the maximum `int8` is
9,223,372,036,854,775,807. **Decode integers exactly**: Go `json.Number` (or the
`Value` accessors of `pkg/event`), a big-integer-aware parser or `BigInt` in
JavaScript, and never `float64`. A consumer that goes through a double sees
`9007199254740993` as `9007199254740992`. Keys are affected too: the canonical key
JSON, which is the Kafka message key, prints integers as exact decimals, and templates
of the cache invalidator render them exactly. See also
[consuming-events.md](consuming-events.md#integers-above-253).

## Unknown types

A type outside the mapping is delivered as its text form, as a JSON string, so the
value is preserved but not structured. Wake counts it once per type in
`wake_unknown_types_total{type}` (labels are capped) and logs a warning; the stream
continues. Schema-change events name such a column with its Postgres type name (or a
fallback name derived from the type OID). To get structure, cast in a view or store the
value in `jsonb`. A type added in a later Postgres version is text until Wake supports
it.

## Limits

- Domains are delivered as text, not by their base type.
- Schema-change events do not list the labels of enum columns.
- `bytea` is delivered as base64 only.
- `money` is delivered as the formatted string, because `lc_monetary` is a session
  property.
- Binary pgoutput is not used, and user-defined types (composites, enums, extension
  types) are not mapped to structured JSON.
