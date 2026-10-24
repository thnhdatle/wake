// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

const (
	synthTsMs = int64(1_792_308_604_187)
	synthXID  = uint32(900)
)

func encodeAll(msgs ...pgoutput.Message) [][]byte {
	out := make([][]byte, len(msgs))
	for i, m := range msgs {
		out[i] = encode(m)
	}
	return out
}

func begin(at lsn.LSN, xid uint32) *pgoutput.Begin {
	return &pgoutput.Begin{FinalLSN: at, TsMs: synthTsMs, XID: xid}
}

func commit(at lsn.LSN) *pgoutput.Commit {
	return &pgoutput.Commit{CommitLSN: at, EndLSN: at + 0x30, TsMs: synthTsMs}
}

func synRelation(oid uint32, name string, identity schema.ReplicaIdentity, cols ...schema.Column) *pgoutput.Relation {
	rel := relationMsg(oid, name, identity, cols...)
	rel.Namespace = "syn"
	for i := range rel.Columns {
		rel.Columns[i].TypeName = ""
	}
	return rel
}

func synthesizedScenarios() map[string][][]byte {
	return map[string][][]byte{
		"types":    typesScenario(),
		"toast":    toastScenario(),
		"identity": identityScenario(),
		"schema":   schemaScenario(),
		"messages": messagesScenario(),
		"errors":   errorsScenario(),
	}
}

func typeColumns() ([]schema.Column, pgoutput.Tuple) {
	spec := []struct {
		name string
		oid  uint32
		text string
	}{
		{"id", schema.OIDInt8, "9223372036854775807"},
		{"c_int2", schema.OIDInt2, "-32768"},
		{"c_int4", schema.OIDInt4, "2147483647"},
		{"c_oid", schema.OIDOID, "4294967295"},
		{"c_numeric_nan", schema.OIDNumeric, "NaN"},
		{"c_numeric", schema.OIDNumeric, "-1234.5600"},
		{"c_float4_inf", schema.OIDFloat4, "Infinity"},
		{"c_float8", schema.OIDFloat8, "-1.5e-300"},
		{"c_float8_nan", schema.OIDFloat8, "NaN"},
		{"c_bool", schema.OIDBool, "t"},
		{"c_text", schema.OIDText, "he said \"hi\" <&> \\ é\n"},
		{"c_varchar", schema.OIDVarchar, "v"},
		{"c_bpchar", schema.OIDBPChar, "ab   "},
		{"c_name", schema.OIDName, "pg_name"},
		{"c_char", schema.OIDChar, "x"},
		{"c_uuid", schema.OIDUUID, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		{"c_bytea", schema.OIDBytea, `\x00ff10`},
		{"c_json", schema.OIDJSON, `{"b": 1, "a": [1, 2], "b": {"c": null}}`},
		{"c_jsonb", schema.OIDJSONB, `{"a": [1, 2.50], "s": "<&>"}`},
		{"c_date_bc", schema.OIDDate, "0044-03-15 BC"},
		{"c_time", schema.OIDTime, "24:00:00"},
		{"c_timetz", schema.OIDTimeTZ, "12:34:56.789-08"},
		{"c_timestamp", schema.OIDTimestamp, "2026-10-18 12:34:56.123456"},
		{"c_tstz_0530", schema.OIDTimestampTZ, "2026-10-18 12:34:56.123+05:30"},
		{"c_tstz_m08", schema.OIDTimestampTZ, "2026-10-18 12:34:56-08"},
		{"c_tstz_lmt", schema.OIDTimestampTZ, "1850-01-01 00:00:00-04:56:02"},
		{"c_tstz_inf", schema.OIDTimestampTZ, "-infinity"},
		{"c_interval", schema.OIDInterval, "-1 years -2 mons +3 days -04:05:06.5"},
		{"c_interval_neg", schema.OIDInterval, "-00:00:01"},
		{"c_int4_arr", schema.OIDInt4Array, "{{1,NULL},{3,4}}"},
		{"c_text_arr", schema.OIDTextArray, `{"a,b","c\"d","e\\f",NULL,"","NULL"}`},
		{"c_jsonb_arr", schema.OIDJSONBArray, `{"{\"k\": [1, \"x,y\"]}",NULL}`},
		{"c_tstz_arr", schema.OIDTimestampTZArray, `{"2026-10-18 12:00:00+00",infinity}`},
		{"c_inet", schema.OIDInet, "192.168.0.1/24"},
		{"c_cidr", schema.OIDCIDR, "10.0.0.0/8"},
		{"c_macaddr", schema.OIDMacaddr, "08:00:2b:01:02:03"},
		{"c_money", schema.OIDMoney, "$1,234.50"},
		{"c_int4range", schema.OIDInt4Range, "[1,10)"},
		{"c_numrange", schema.OIDNumRange, "(,5.5]"},
		{"c_tstzrange", schema.OIDTSTZRange, `["2026-01-01 00:00:00+00",infinity)`},
		{"c_daterange", schema.OIDDateRange, "empty"},
		{"c_int8range", schema.OIDInt8Range, "(,)"},
		{"c_int4multirange", schema.OIDInt4Multirange, "{[1,3),[5,7)}"},
		{"c_mood", 20100, "happy"},
		{"c_citext", 20101, "MiXeD"},
		{"c_point", schema.OIDPoint, "(1.5,2)"},
	}
	cols := make([]schema.Column, len(spec))
	tuple := make(pgoutput.Tuple, len(spec))
	for i, s := range spec {
		cols[i] = schema.Column{Name: s.name, TypeOID: s.oid, TypeMod: -1, Key: i == 0}
		tuple[i] = txt(s.text)
	}
	return cols, tuple
}

func typesScenario() [][]byte {
	cols, values := typeColumns()
	nulls := make(pgoutput.Tuple, len(cols))
	for i := range nulls {
		nulls[i] = null()
	}
	nulls[0] = txt("2")
	invalid := append(pgoutput.Tuple{txt("3"), txt("not-an-int")}, nulls[2:]...)
	return encodeAll(
		begin(0x1000, synthXID),
		&pgoutput.TypeMessage{OID: 20100, Namespace: "syn", Name: "mood"},
		&pgoutput.Relation{Relation: schema.Relation{
			OID: 20001, Namespace: "syn", Name: "types", Identity: schema.IdentityDefault, Columns: cols,
		}},
		&pgoutput.Insert{RelationOID: 20001, New: values},
		&pgoutput.Insert{RelationOID: 20001, New: nulls},
		&pgoutput.Insert{RelationOID: 20001, New: invalid},
		commit(0x1000),
	)
}

func docColumns(identity schema.ReplicaIdentity) []schema.Column {
	full := identity == schema.IdentityFull
	return []schema.Column{
		col("id", schema.OIDInt4, true), col("title", schema.OIDText, full), col("body", schema.OIDText, full),
	}
}

func toastScenario() [][]byte {
	return encodeAll(
		begin(0x2000, synthXID),
		synRelation(20010, "docs", schema.IdentityDefault, docColumns(schema.IdentityDefault)...),
		synRelation(20011, "full_pk", schema.IdentityFull, docColumns(schema.IdentityFull)...),
		&pgoutput.Update{RelationOID: 20010, New: tup(txt("1"), txt("b"), toast())},
		&pgoutput.Update{
			RelationOID: 20010, OldKind: pgoutput.OldKey,
			Old: tup(txt("1"), null(), null()), New: tup(txt("2"), toast(), toast()),
		},
		&pgoutput.Update{
			RelationOID: 20011, OldKind: pgoutput.OldFull,
			Old: tup(txt("1"), txt("a"), txt("long body")), New: tup(txt("1"), txt("b"), toast()),
		},
		commit(0x2000),
	)
}

func identityScenario() [][]byte {
	return encodeAll(
		begin(0x3000, synthXID),
		synRelation(20020, "full_pk", schema.IdentityFull, docColumns(schema.IdentityFull)...),
		synRelation(20021, "full_nopk", schema.IdentityFull,
			col("a", schema.OIDInt4, true), col("b", schema.OIDText, true)),
		synRelation(20022, "uniq_idx", schema.IdentityIndex,
			col("a", schema.OIDInt4, true), col("b", schema.OIDInt4, true), col("c", schema.OIDText, false)),
		synRelation(20023, "keyless", schema.IdentityDefault,
			col("a", schema.OIDInt4, false), col("b", schema.OIDText, false)),
		synRelation(20024, "nothing", schema.IdentityNothing, col("a", schema.OIDInt4, false)),
		&pgoutput.Insert{RelationOID: 20020, New: tup(txt("1"), txt("t"), txt("b"))},
		&pgoutput.Delete{RelationOID: 20020, OldKind: pgoutput.OldFull, Old: tup(txt("1"), txt("t"), txt("b"))},
		&pgoutput.Update{
			RelationOID: 20021, OldKind: pgoutput.OldFull,
			Old: tup(txt("1"), txt("one")), New: tup(txt("1"), txt("uno")),
		},
		&pgoutput.Delete{RelationOID: 20021, OldKind: pgoutput.OldFull, Old: tup(txt("1"), null())},
		&pgoutput.Update{RelationOID: 20022, New: tup(txt("1"), txt("2"), txt("d"))},
		&pgoutput.Update{
			RelationOID: 20022, OldKind: pgoutput.OldKey,
			Old: tup(txt("1"), txt("2"), null()), New: tup(txt("1"), txt("3"), txt("d")),
		},
		&pgoutput.Delete{RelationOID: 20022, OldKind: pgoutput.OldKey, Old: tup(txt("1"), txt("3"), null())},
		&pgoutput.Insert{RelationOID: 20023, New: tup(txt("1"), txt("k"))},
		&pgoutput.Insert{RelationOID: 20024, New: tup(txt("1"))},
		commit(0x3000),
	)
}

func schemaScenario() [][]byte {
	evolve := func(identity schema.ReplicaIdentity, cols ...schema.Column) *pgoutput.Relation {
		return synRelation(20030, "evolve", identity, cols...)
	}
	id := col("id", schema.OIDInt4, true)
	b := col("b", schema.OIDVarchar, false)
	b10 := b
	b10.TypeMod = 14
	return encodeAll(
		begin(0x4000, synthXID),
		evolve(schema.IdentityDefault, id),
		&pgoutput.Insert{RelationOID: 20030, New: tup(txt("1"))},
		evolve(schema.IdentityDefault, id),
		&pgoutput.Insert{RelationOID: 20030, New: tup(txt("2"))},
		commit(0x4000),
		begin(0x5000, synthXID+1),
		&pgoutput.Insert{RelationOID: 20030, New: tup(txt("3"))},
		evolve(schema.IdentityDefault, id, b),
		&pgoutput.Insert{RelationOID: 20030, New: tup(txt("4"), txt("x"))},
		evolve(schema.IdentityDefault, id, b10),
		&pgoutput.Update{RelationOID: 20030, New: tup(txt("4"), txt("y"))},
		evolve(schema.IdentityFull, col("id", schema.OIDInt4, true), col("b", schema.OIDVarchar, true)),
		&pgoutput.Delete{RelationOID: 20030, OldKind: pgoutput.OldFull, Old: tup(txt("4"), txt("y"))},
		commit(0x5000),
	)
}

func messagesScenario() [][]byte {
	return encodeAll(
		begin(0x6000, synthXID),
		&pgoutput.Origin{CommitLSN: 0x5FFF, Name: "upstream"},
		&pgoutput.LogicalMessage{Flags: 1, LSN: 0x5F00, Prefix: "app.audit", Content: []byte("in-tx")},
		&pgoutput.LogicalMessage{Flags: 1, LSN: 0x5F10, Prefix: pgoutput.PrefixWatermark, Content: []byte(`{"kind":"low"}`)},
		&pgoutput.LogicalMessage{Flags: 1, LSN: 0x5F20, Prefix: "app.audit", Content: []byte{0, 0xFF}},
		commit(0x6000),
		&pgoutput.LogicalMessage{LSN: 0x6100, Prefix: "app.audit", Content: []byte("outside")},
		&pgoutput.LogicalMessage{LSN: 0x6110, Prefix: pgoutput.PrefixHeartbeat, Content: []byte("{}")},
		begin(0x7000, synthXID+1),
		synRelation(20040, "a", schema.IdentityDefault, col("id", schema.OIDInt4, true)),
		synRelation(20041, "b", schema.IdentityDefault, col("id", schema.OIDInt4, true)),
		&pgoutput.Truncate{
			Options: pgoutput.TruncateCascade | pgoutput.TruncateRestartIdentity, RelationOIDs: []uint32{20040, 20041},
		},
		commit(0x7000),
	)
}

func errorsScenario() [][]byte {
	valid := encodeAll(
		begin(0x8000, synthXID),
		synRelation(20050, "full", schema.IdentityFull, col("id", schema.OIDInt4, true)),
		&pgoutput.Update{RelationOID: 20050, New: tup(txt("1"))},
		&pgoutput.Insert{RelationOID: 29999, New: tup(txt("1"))},
		&pgoutput.Insert{RelationOID: 20050, New: tup(pgoutput.TupleColumn{Kind: pgoutput.TupleBinary, Data: []byte{1}})},
		commit(0x8000),
	)
	truncated := encode(begin(0x9000, synthXID))
	return append(valid,
		[]byte{'S', 0, 0, 0, 1, 1},
		[]byte{'Z'},
		truncated[:len(truncated)-2],
	)
}
