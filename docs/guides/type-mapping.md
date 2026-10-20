# Type mapping

Look up how each Postgres type appears in event JSON. The same rules apply to `before`,
`after` and `key`, for stream and snapshot rows alike.

- [General rules](#general-rules)
- [Session settings](#session-settings)
- [The mapping](#the-mapping)
- [Details and edge cases](#details-and-edge-cases)
- [Integers above 2^53](#integers-above-253)
- [Unknown types](#unknown-types)
- [Limits](#limits)

## General rules

- SQL `NULL` is JSON `null`.
- Values JSON numbers cannot hold exactly (`numeric`, `NaN`, infinities, out-of-range dates)
  are strings.
- Integers are exact.
- Strings are valid UTF-8 JSON. `<`, `>` and `&` stay literal; invalid bytes become U+FFFD.
- Output is deterministic.
- An unknown type never stops the stream ([Unknown types](#unknown-types)).

## Session settings

Wake forces these on every session. Role or database defaults do not change the output.

| Setting | Value | Why |
|---|---|---|
| `DateStyle` | `ISO` | parse dates |
| `IntervalStyle` | `postgres` | parse intervals |
| `client_encoding` | `UTF8` | JSON is UTF-8 |
| `TimeZone` | `UTC` | stable `timestamptz` offsets |
| `extra_float_digits` | `1` | shortest round-trip floats |

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

A domain column is unknown and arrives as a string, even over a numeric base type.

## Details and edge cases

- **Infinities**: infinite dates, timestamps and intervals become `"infinity"` and
  `"-infinity"`.
- **BC and large years**: `0044-03-15 BC` becomes `"-0043-03-15"`; `"10000-01-01"` keeps all
  digits. These are ISO 8601 but not RFC 3339; use an ISO 8601 parser.
- **Offsets**: rendered `±hh:mm`. Offsets with seconds (before about 1900) are converted to UTC
  `+00:00`.
- **`time` `24:00:00`** is kept.
- **Interval**: the time sign applies to each part (`-04:05:06` becomes `"PT-4H-5M-6S"`). Zero
  parts are omitted.
- **Ranges**: an unbounded side is `null` with `_inc` false. An `infinity` bound is the string
  `"infinity"`, not `null`.
- **Arrays**: non-default lower bounds (`[0:1]={1,2}`) are dropped.
- **Unparseable values** are an error; Wake never guesses.

## Integers above 2^53

`int8` and `xid8` can exceed 2^53 (9,007,199,254,740,992). **Decode integers exactly**: Go
`json.Number` or `pkg/event` `Value`, `BigInt` in JavaScript, never `float64`. A double turns
`9007199254740993` into `9007199254740992`. Kafka keys and cache templates are exact too. See
[consuming-events.md](consuming-events.md#integers-above-253).

## Unknown types

Unmapped types arrive as their text form in a JSON string. Each is counted once in
`wake_unknown_types_total{type}` and logged. For structure, cast in a view or store as `jsonb`.

## Limits

- Domains arrive as text.
- Schema-change events omit enum labels.
- `bytea` is base64 only.
- `money` is the formatted string (`lc_monetary`).
- Composites, enums and extension types are not structured.
