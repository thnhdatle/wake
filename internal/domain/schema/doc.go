// SPDX-License-Identifier: Apache-2.0

// Package schema describes published tables (Relation, Column, replica
// identity), converts column values from Postgres text output to the JSON of
// the CDC event contract (AppendJSON, the type mapping table below) and tracks
// per-table schema versions (Registry).
//
// # Type mapping
//
// pgoutput sends column values in the type's text output format. The
// conversions assume the replication session uses DateStyle=ISO and
// IntervalStyle=postgres (Postgres defaults; the replication adapter sets
// them explicitly) and client_encoding UTF8. SQL NULL is always JSON null.
// Every JSON string is produced by change.AppendString.
//
//	Postgres type                          JSON                          Example (text → JSON)
//	-------------------------------------  ----------------------------  ------------------------------------------------
//	int2, int4, int8, oid, xid, cid, xid8  number, digits verbatim       9223372036854775807 → 9223372036854775807
//	                                       (int8 may exceed 2^53)
//	float4, float8                         number; NaN, Infinity,        1e+100 → 1e+100; NaN → "NaN"
//	                                       -Infinity as strings
//	numeric                                string (incl. NaN, ±Infinity) 1234.5600 → "1234.5600"; NaN → "NaN"
//	money                                  string (lc_monetary format)   $12.34 → "$12.34"
//	bool                                   true / false                  t → true
//	text, varchar, bpchar, name, "char",   string (bpchar keeps its      ab␠␠␠ → "ab␠␠␠"
//	uuid, xml, jsonpath                    padding)
//	inet, cidr, macaddr, macaddr8          string                        192.168.0.1/24 → "192.168.0.1/24"
//	bytea                                  string, standard padded       \xdeadbeef00 → "3q2+7wA="
//	                                       base64 (hex or escape input)
//	json, jsonb                            embedded JSON, validated,     {"b": 1} → {"b":1}
//	                                       insignificant space removed,
//	                                       member order kept
//	date                                   "YYYY-MM-DD"                  2026-10-18 → "2026-10-18"
//	timestamp                              "YYYY-MM-DDThh:mm:ss[.f]"     2026-10-18 12:34:56.5 → "2026-10-18T12:34:56.5"
//	                                       (no offset)
//	timestamptz                            "YYYY-MM-DDThh:mm:ss[.f]±hh:mm"
//	                                                                     2026-10-18 12:34:56.123+02
//	                                                                     → "2026-10-18T12:34:56.123+02:00"
//	time                                   "hh:mm:ss[.f]"                12:34:56.789 → "12:34:56.789"
//	timetz                                 "hh:mm:ss[.f]±hh:mm"          12:34:56+05:30 → "12:34:56+05:30"
//	interval                               ISO 8601 duration, each       1 year 2 mons -3 days +04:05:06.5
//	                                       component signed (as          → "P1Y2M-3DT4H5M6.5S";
//	                                       IntervalStyle iso_8601)       -00:00:01 → "PT-1S"; 00:00:00 → "PT0S"
//	int4range, int8range, numrange,        object; bounds mapped by      [1,10) → {"lower":1,"upper":10,
//	daterange, tsrange, tstzrange          the bound type                 "lower_inc":true,"upper_inc":false}
//	                                                                     empty → {"empty":true}
//	int4multirange … tstzmultirange        array of range objects        {[1,3),[5,7)} → [{…},{…}]
//	arrays of every type above and of the  JSON array (nested for        {1,NULL,3} → [1,null,3]
//	other built-in types listed by         multi-dimensional arrays),    {{1,2},{3,4}} → [[1,2],[3,4]]
//	IsKnownType                            elements mapped by the        {"a,b","c\"d"} → ["a,b","c\"d"]
//	                                       element type
//	point, lseg, path, box, polygon,       string (text output)          (1.5,2) → "(1.5,2)"
//	line, circle, bit, varbit, tsvector,
//	tsquery, pg_lsn, reg* types, tid,
//	int2vector, oidvector, aclitem,
//	refcursor, txid_snapshot, pg_snapshot
//	anything else (enums, domains,         string (text output); the     happy → "happy"
//	composites, extension types such as    type is "unknown": IsKnownType
//	citext, arrays of those)               is false and the caller counts
//	                                       a warning once per type
//
// Details:
//
//   - Infinite dates, timestamps and timestamptz become the strings
//     "infinity" and "-infinity"; so do Postgres 17 infinite intervals.
//   - BC dates use ISO 8601 astronomical year numbering with a sign and at
//     least four digits: "0044-03-15 BC" → "-0043-03-15" (1 BC is "0000").
//     Years above 9999 keep all their digits without a sign ("10000-01-01").
//     Such values are ISO 8601 but outside RFC 3339.
//   - timestamptz offsets are rendered "±hh:mm" ("+00" → "+00:00", "-08" →
//     "-08:00"). An offset with seconds (local mean time zones before ~1900,
//     e.g. "-04:56:02") is not representable in RFC 3339: the value is
//     converted to UTC and rendered with "+00:00"; timetz likewise, modulo
//     24 hours. Fractional seconds are copied verbatim.
//   - time "24:00:00" is kept as is.
//   - interval: years and months come from the month total and share its
//     sign, days are signed on their own, and the time part has one sign
//     applied to each of H, M and S ("-04:05:06" → "PT-4H-5M-6S"). Zero
//     components are omitted; a zero interval is "PT0S". Output already in
//     ISO 8601 form (IntervalStyle iso_8601) is passed through.
//   - Ranges: an unbounded side is null with its _inc flag false (Postgres
//     never reports an unbounded side as inclusive); a bound whose value is
//     'infinity' is the string "infinity", which is distinct from null.
//   - Arrays: non-default lower bounds ("[0:1]={1,2}") are dropped; box
//     arrays use ';' as element delimiter like Postgres.
//
// # Schema versions
//
// Registry keys tables by "<schema>.<table>" (Relation.QualifiedName), the
// identity consumers see in CDC event sources, so that a table dropped and
// re-created under the same name never goes back to a lower version; a
// renamed table starts a new history under its new name. A version
// increments when the replica identity or any column's name, type OID, type
// modifier or identity flag changes (column order included); type names do
// not take part. A type-modifier-only change (varchar(10) → varchar(20))
// therefore yields a schema-change event whose column lists look identical.
//
//	reg := schema.NewRegistry()
//	if err := reg.Seed(stored); err != nil { ... }
//	obs := reg.Observe(&rel)
//	if obs.Changed {
//		// emit a schema-change event with obs.Version, obs.Before, obs.After
//	}
//	val, err := schema.AppendJSON(nil, schema.OIDInt8, []byte("9223372036854775807"))
package schema
