// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/event"
)

// typeCol is one column of wak07.types: its SQL type, the SQL expressions
// inserted in row A and row B (row C is all NULL) and the JSON Wake must
// emit for each (the type mapping of internal/domain/schema).
type typeCol struct {
	name, sqlType string
	litA, litB    string
	wantA, wantB  string
}

// typeColumns covers every type of the type mapping table: integers, floats,
// numeric, money, bool, the text family, network types, bytea, json/jsonb,
// date/time types, interval, ranges, multiranges, arrays, the text-output
// types (geometry, bit strings, text search, pg_lsn, reg*, tid, vectors,
// aclitem, refcursor, snapshots) and unknown types (enum, domain, composite,
// extension type citext, array of enum).
var typeColumns = []typeCol{
	{"c_int2", "int2", "-32768", "32767", "-32768", "32767"},
	{"c_int4", "int4", "2147483647", "-2147483648", "2147483647", "-2147483648"},
	{"c_int8", "int8", "9223372036854775807", "-9223372036854775808", "9223372036854775807", "-9223372036854775808"},
	{"c_oid", "oid", "4294967295", "0", "4294967295", "0"},
	{"c_xid", "xid", "'4294967295'", "'3'", "4294967295", "3"},
	{"c_cid", "cid", "'42'", "'0'", "42", "0"},
	{"c_xid8", "xid8", "'18446744073709551615'", "'0'", "18446744073709551615", "0"},
	{"c_float4", "float4", "1.5", "'NaN'", "1.5", `"NaN"`},
	{"c_float4_b", "float4", "'-Infinity'", "'3.4028235e+38'", `"-Infinity"`, "3.4028235e+38"},
	{"c_float8", "float8", "-1.5e-300", "'Infinity'", "-1.5e-300", `"Infinity"`},
	{"c_float8_b", "float8", "0.1", "1e+100", "0.1", "1e+100"},
	{"c_numeric", "numeric(20,4)", "1234567890123456.1234", "1.5", `"1234567890123456.1234"`, `"1.5000"`},
	{"c_numeric_b", "numeric", "'NaN'", "'-Infinity'", `"NaN"`, `"-Infinity"`},
	{
		"c_numeric_c", "numeric", "123456789012345678901234567890.000000000000000000001", "'Infinity'",
		`"123456789012345678901234567890.000000000000000000001"`, `"Infinity"`,
	},
	{"c_money", "money", "'12.34'", "'-1.5'", `"$12.34"`, `"-$1.50"`},
	{"c_bool", "bool", "true", "false", "true", "false"},
	{
		"c_text", "text", `'he said "hi" <&> é ☃'`, `E'tab\there\nline\\back   end'`,
		`"he said \"hi\" <&> é ☃"`, `"tab\there\nline\\back   end"`,
	},
	{"c_varchar", "varchar(20)", "'varchar'", "''", `"varchar"`, `""`},
	{"c_bpchar", "char(5)", "'ab'", "'abcde'", `"ab   "`, `"abcde"`},
	{"c_name", "name", "'pg_name'", "'Ünïcode'", `"pg_name"`, `"Ünïcode"`},
	{"c_char", `"char"`, "'x'", "'Z'", `"x"`, `"Z"`},
	{
		"c_uuid", "uuid", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'00000000-0000-0000-0000-000000000000'",
		`"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`, `"00000000-0000-0000-0000-000000000000"`,
	},
	{"c_xml", "xml", "'<a>1</a>'", `'<b x="1">t &amp; u</b>'`, `"<a>1</a>"`, `"<b x=\"1\">t &amp; u</b>"`},
	{"c_jsonpath", "jsonpath", "'$.a[*] ? (@ > 1)'", "'strict $.b'", `"$.\"a\"[*]?(@ > 1)"`, `"strict $.\"b\""`},
	{"c_inet", "inet", "'192.168.0.1/24'", "'::1'", `"192.168.0.1/24"`, `"::1"`},
	{"c_cidr", "cidr", "'10.0.0.0/8'", "'2001:db8::/32'", `"10.0.0.0/8"`, `"2001:db8::/32"`},
	{"c_macaddr", "macaddr", "'08:00:2b:01:02:03'", "'08-00-2B-01-02-04'", `"08:00:2b:01:02:03"`, `"08:00:2b:01:02:04"`},
	{
		"c_macaddr8", "macaddr8", "'08:00:2b:01:02:03:04:05'", "'08:00:2b:01:02:03'",
		`"08:00:2b:01:02:03:04:05"`, `"08:00:2b:ff:fe:01:02:03"`,
	},
	{"c_bytea", "bytea", `'\xdeadbeef00'`, `'\x'`, `"3q2+7wA="`, `""`},
	{"c_bytea_b", "bytea", "'abc'", `'\x00ff'`, `"YWJj"`, `"AP8="`},
	{
		"c_json", "json", `'{"b": 1, "a": [1, 2]}'`, `'[12345678901234567890, 1.0e2, null, "s"]'`,
		`{"b":1,"a":[1,2]}`, `[12345678901234567890,1.0e2,null,"s"]`,
	},
	{
		"c_jsonb", "jsonb", `'{"b": 1, "a": [1, 2]}'`, `'{"s": "xé", "n": 12345678901234567890.5}'`,
		`{"a":[1,2],"b":1}`, `{"n":12345678901234567890.5,"s":"xé"}`,
	},
	{"c_date", "date", "'2026-10-18'", "'0044-03-15 BC'", `"2026-10-18"`, `"-0043-03-15"`},
	{"c_date_b", "date", "'-infinity'", "'10000-01-01'", `"-infinity"`, `"10000-01-01"`},
	{"c_time", "time", "'12:34:56.789'", "'24:00:00'", `"12:34:56.789"`, `"24:00:00"`},
	{"c_timetz", "timetz", "'12:34:56+05:30'", "'00:00:00-08'", `"12:34:56+05:30"`, `"00:00:00-08:00"`},
	{
		"c_timestamp", "timestamp", "'2026-10-18 12:34:56.123456'", "'infinity'",
		`"2026-10-18T12:34:56.123456"`, `"infinity"`,
	},
	{
		"c_timestamptz", "timestamptz", "'2026-10-18 12:34:56.5+00'", "'2026-10-18 12:34:56+02'",
		`"2026-10-18T12:34:56.5+00:00"`, `"2026-10-18T10:34:56+00:00"`,
	},
	{"c_interval", "interval", "'1 year 2 mons -3 days 04:05:06.5'", "'-00:00:01'", `"P1Y2M-3DT4H5M6.5S"`, `"PT-1S"`},
	{"c_interval_b", "interval", "'00:00:00'", "'infinity'", `"PT0S"`, `"infinity"`},
	{
		"c_int4range", "int4range", "'[1,10)'", "'empty'",
		`{"lower":1,"upper":10,"lower_inc":true,"upper_inc":false}`, `{"empty":true}`,
	},
	{
		"c_int8range", "int8range", "'[1,5]'", "'(,9223372036854775806]'",
		`{"lower":1,"upper":6,"lower_inc":true,"upper_inc":false}`,
		`{"lower":null,"upper":9223372036854775807,"lower_inc":false,"upper_inc":false}`,
	},
	{
		"c_numrange", "numrange", "'(,5.5]'", "'[1.0,2.50)'",
		`{"lower":null,"upper":"5.5","lower_inc":false,"upper_inc":true}`,
		`{"lower":"1.0","upper":"2.50","lower_inc":true,"upper_inc":false}`,
	},
	{
		"c_daterange", "daterange", "'[2026-01-01,2026-02-01)'", "'empty'",
		`{"lower":"2026-01-01","upper":"2026-02-01","lower_inc":true,"upper_inc":false}`, `{"empty":true}`,
	},
	{
		"c_tsrange", "tsrange", "'[2026-01-01 00:00,2026-01-02 00:00)'", "'(,infinity]'",
		`{"lower":"2026-01-01T00:00:00","upper":"2026-01-02T00:00:00","lower_inc":true,"upper_inc":false}`,
		`{"lower":null,"upper":"infinity","lower_inc":false,"upper_inc":true}`,
	},
	{
		"c_tstzrange", "tstzrange", "'[2026-01-01 00:00:00+00,infinity)'",
		"'(2026-01-01 00:00:00+02,2026-01-01 01:00:00+00]'",
		`{"lower":"2026-01-01T00:00:00+00:00","upper":"infinity","lower_inc":true,"upper_inc":false}`,
		`{"lower":"2025-12-31T22:00:00+00:00","upper":"2026-01-01T01:00:00+00:00","lower_inc":false,"upper_inc":true}`,
	},
	{
		"c_int4mr", "int4multirange", "'{[1,3),[5,7)}'", "'{}'",
		`[{"lower":1,"upper":3,"lower_inc":true,"upper_inc":false},` +
			`{"lower":5,"upper":7,"lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_int8mr", "int8multirange", "'{[1,2)}'", "'{}'",
		`[{"lower":1,"upper":2,"lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_nummr", "nummultirange", "'{[1.5,2.5]}'", "'{(,0)}'",
		`[{"lower":"1.5","upper":"2.5","lower_inc":true,"upper_inc":true}]`,
		`[{"lower":null,"upper":"0","lower_inc":false,"upper_inc":false}]`,
	},
	{
		"c_datemr", "datemultirange", "'{[2026-01-01,2026-02-01)}'", "'{}'",
		`[{"lower":"2026-01-01","upper":"2026-02-01","lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_tsmr", "tsmultirange", "'{[2026-01-01 00:00,2026-01-02 00:00)}'", "'{}'",
		`[{"lower":"2026-01-01T00:00:00","upper":"2026-01-02T00:00:00","lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_tstzmr", "tstzmultirange", "'{[2026-01-01 00:00+00,infinity)}'", "'{}'",
		`[{"lower":"2026-01-01T00:00:00+00:00","upper":"infinity","lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{"c_int4_arr", "int4[]", "'{1,NULL,3}'", "'{}'", `[1,null,3]`, `[]`},
	{
		"c_text_arr", "text[]", `'{"a,b","c\"d","e\\f",NULL,"","NULL"}'`, "'[0:1]={x,y}'",
		`["a,b","c\"d","e\\f",null,"","NULL"]`, `["x","y"]`,
	},
	{
		"c_int8_arr2", "int8[][]", "'{{1,2},{3,4}}'", "'{{9223372036854775807}}'",
		`[[1,2],[3,4]]`, `[[9223372036854775807]]`,
	},
	{"c_bool_arr", "bool[]", "'{t,f,NULL}'", "'{}'", `[true,false,null]`, `[]`},
	{"c_float8_arr", "float8[]", "'{NaN,1.5,-Infinity}'", "'{1e+100}'", `["NaN",1.5,"-Infinity"]`, `[1e+100]`},
	{"c_numeric_arr", "numeric[]", "'{1.50,NaN}'", "'{}'", `["1.50","NaN"]`, `[]`},
	{
		"c_jsonb_arr", "jsonb[]", `ARRAY['{"a": 1}'::jsonb, 'null'::jsonb]`, "'{}'",
		`[{"a":1},null]`, `[]`,
	},
	{"c_bytea_arr", "bytea[]", `'{"\\xdead",NULL}'`, "'{}'", `["3q0=",null]`, `[]`},
	{"c_date_arr", "date[]", "'{2026-10-18,infinity}'", "'{}'", `["2026-10-18","infinity"]`, `[]`},
	{
		"c_tstz_arr", "timestamptz[]", `'{"2026-10-18 12:34:56+00"}'`, "'{}'",
		`["2026-10-18T12:34:56+00:00"]`, `[]`,
	},
	{"c_interval_arr", "interval[]", `'{"1 day","-00:00:01"}'`, "'{}'", `["P1D","PT-1S"]`, `[]`},
	{
		"c_uuid_arr", "uuid[]", "'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'", "'{}'",
		`["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]`, `[]`,
	},
	{"c_inet_arr", "inet[]", "'{10.0.0.1,::1}'", "'{}'", `["10.0.0.1","::1"]`, `[]`},
	{"c_box_arr", "box[]", "'{(1,1),(0,0);(2,2),(1,1)}'", "'{}'", `["(1,1),(0,0)","(2,2),(1,1)"]`, `[]`},
	{
		"c_range_arr", "int4range[]", `'{"[1,3)",empty}'`, "'{}'",
		`[{"lower":1,"upper":3,"lower_inc":true,"upper_inc":false},{"empty":true}]`, `[]`,
	},
	{"c_point", "point", "'(1.5,2)'", "'(0,0)'", `"(1.5,2)"`, `"(0,0)"`},
	{"c_lseg", "lseg", "'[(0,0),(1,1)]'", "'[(1,2),(3,4)]'", `"[(0,0),(1,1)]"`, `"[(1,2),(3,4)]"`},
	{"c_path", "path", "'((0,0),(1,1),(2,0))'", "'[(0,0),(1,1)]'", `"((0,0),(1,1),(2,0))"`, `"[(0,0),(1,1)]"`},
	{"c_box", "box", "'(1,1),(0,0)'", "'(0,0),(2,2)'", `"(1,1),(0,0)"`, `"(2,2),(0,0)"`},
	{
		"c_polygon", "polygon", "'((0,0),(1,1),(1,0))'", "'((0,0),(0,2),(2,2))'",
		`"((0,0),(1,1),(1,0))"`, `"((0,0),(0,2),(2,2))"`,
	},
	{"c_line", "line", "'{1,-1,0}'", "'{0,1,-2}'", `"{1,-1,0}"`, `"{0,1,-2}"`},
	{"c_circle", "circle", "'<(0,0),1>'", "'<(1,2),3.5>'", `"<(0,0),1>"`, `"<(1,2),3.5>"`},
	{"c_bit", "bit(4)", "B'1010'", "B'0001'", `"1010"`, `"0001"`},
	{"c_varbit", "varbit", "B'101'", "B''", `"101"`, `""`},
	{"c_tsvector", "tsvector", "'a fat cat'", "'fat:2,4 cat:3B'", `"'a' 'cat' 'fat'"`, `"'cat':3B 'fat':2,4"`},
	{"c_tsquery", "tsquery", "'fat & rat'", "'!cat'", `"'fat' & 'rat'"`, `"!'cat'"`},
	{"c_pg_lsn", "pg_lsn", "'16/B374D848'", "'0/0'", `"16/B374D848"`, `"0/0"`},
	{"c_regclass", "regclass", "'pg_class'", "'pg_type'", `"pg_class"`, `"pg_type"`},
	{"c_regtype", "regtype", "'integer'", "'text[]'", `"integer"`, `"text[]"`},
	{"c_regproc", "regproc", "'now'", "'pg_sleep'", `"now"`, `"pg_sleep"`},
	{"c_regprocedure", "regprocedure", "'abs(integer)'", "'now()'", `"abs(integer)"`, `"now()"`},
	{"c_regoper", "regoper", "'||/'", "'|/'", `"||/"`, `"|/"`},
	{
		"c_regoperator", "regoperator", "'+(integer,integer)'", "'-(NONE,integer)'",
		`"+(integer,integer)"`, `"-(NONE,integer)"`,
	},
	{"c_regconfig", "regconfig", "'english'", "'simple'", `"english"`, `"simple"`},
	{"c_regdictionary", "regdictionary", "'simple'", "'english_stem'", `"simple"`, `"english_stem"`},
	{"c_regnamespace", "regnamespace", "'public'", "'pg_catalog'", `"public"`, `"pg_catalog"`},
	{"c_regrole", "regrole", "'postgres'", "'pg_monitor'", `"postgres"`, `"pg_monitor"`},
	{"c_regcollation", "regcollation", `'"C"'`, `'"POSIX"'`, `"\"C\""`, `"\"POSIX\""`},
	{"c_tid", "tid", "'(0,1)'", "'(4294967295,65535)'", `"(0,1)"`, `"(4294967295,65535)"`},
	{"c_int2vector", "int2vector", "'1 2'", "'3'", `"1 2"`, `"3"`},
	{"c_oidvector", "oidvector", "'1 2'", "'4294967295'", `"1 2"`, `"4294967295"`},
	{
		"c_aclitem", "aclitem", "'postgres=arwdDxt/postgres'", "'=r/postgres'",
		`"postgres=arwdDxt/postgres"`, `"=r/postgres"`,
	},
	{"c_refcursor", "refcursor", "'cur'", "'other'", `"cur"`, `"other"`},
	{"c_txid_snapshot", "txid_snapshot", "'10:20:10,14,15'", "'1:1:'", `"10:20:10,14,15"`, `"1:1:"`},
	{"c_pg_snapshot", "pg_snapshot", "'10:20:10,14,15'", "'1:1:'", `"10:20:10,14,15"`, `"1:1:"`},
	// Arrays of the remaining built-in types (elements mapped by element type).
	{"c_int2_arr", "int2[]", "'{-32768,32767}'", "'{NULL}'", `[-32768,32767]`, `[null]`},
	{"c_oid_arr", "oid[]", "'{4294967295}'", "'{}'", `[4294967295]`, `[]`},
	{"c_xid_arr", "xid[]", "'{3}'", "'{}'", `[3]`, `[]`},
	{"c_cid_arr", "cid[]", "'{42}'", "'{}'", `[42]`, `[]`},
	{"c_xid8_arr", "xid8[]", "'{18446744073709551615}'", "'{}'", `[18446744073709551615]`, `[]`},
	{"c_float4_arr", "float4[]", "'{1.5,Infinity}'", "'{}'", `[1.5,"Infinity"]`, `[]`},
	{"c_money_arr", "money[]", "'{12.34,-1.5}'", "'{}'", `["$12.34","-$1.50"]`, `[]`},
	{"c_varchar_arr", "varchar(10)[]", `'{"a b",c}'`, "'{}'", `["a b","c"]`, `[]`},
	{"c_bpchar_arr", "char(3)[]", "'{a,bcd}'", "'{}'", `["a  ","bcd"]`, `[]`},
	{"c_name_arr", "name[]", `'{pg_name,"x y"}'`, "'{}'", `["pg_name","x y"]`, `[]`},
	{"c_char_arr", `"char"[]`, "'{x,y}'", "'{}'", `["x","y"]`, `[]`},
	{
		"c_json_arr", "json[]", `'{"{\"b\": 1, \"a\": 2}","[1, 2]"}'`, "'{}'",
		`[{"b":1,"a":2},[1,2]]`, `[]`,
	},
	{"c_jsonpath_arr", "jsonpath[]", `'{"$.a"}'`, "'{}'", `["$.\"a\""]`, `[]`},
	{"c_xml_arr", "xml[]", `'{"<a>1</a>"}'`, "'{}'", `["<a>1</a>"]`, `[]`},
	{"c_cidr_arr", "cidr[]", "'{10.0.0.0/8}'", "'{}'", `["10.0.0.0/8"]`, `[]`},
	{"c_macaddr_arr", "macaddr[]", "'{08:00:2b:01:02:03}'", "'{}'", `["08:00:2b:01:02:03"]`, `[]`},
	{"c_macaddr8_arr", "macaddr8[]", "'{08:00:2b:01:02:03:04:05}'", "'{}'", `["08:00:2b:01:02:03:04:05"]`, `[]`},
	{"c_time_arr", "time[]", "'{12:34:56.789,24:00:00}'", "'{}'", `["12:34:56.789","24:00:00"]`, `[]`},
	{"c_timetz_arr", "timetz[]", "'{12:34:56+05:30}'", "'{}'", `["12:34:56+05:30"]`, `[]`},
	{
		"c_timestamp_arr", "timestamp[]", `'{"2026-10-18 12:34:56.5",infinity}'`, "'{}'",
		`["2026-10-18T12:34:56.5","infinity"]`, `[]`,
	},
	{"c_bit_arr", "bit(2)[]", "'{10,01}'", "'{}'", `["10","01"]`, `[]`},
	{"c_varbit_arr", "varbit[]", "'{1,101}'", "'{}'", `["1","101"]`, `[]`},
	{"c_point_arr", "point[]", `'{"(1,2)","(3,4)"}'`, "'{}'", `["(1,2)","(3,4)"]`, `[]`},
	{"c_lseg_arr", "lseg[]", `'{"[(0,0),(1,1)]"}'`, "'{}'", `["[(0,0),(1,1)]"]`, `[]`},
	{"c_path_arr", "path[]", `'{"((0,0),(1,1))"}'`, "'{}'", `["((0,0),(1,1))"]`, `[]`},
	{"c_polygon_arr", "polygon[]", `'{"((0,0),(1,1),(1,0))"}'`, "'{}'", `["((0,0),(1,1),(1,0))"]`, `[]`},
	{"c_line_arr", "line[]", `'{"{1,-1,0}"}'`, "'{}'", `["{1,-1,0}"]`, `[]`},
	{"c_circle_arr", "circle[]", `'{"<(0,0),1>"}'`, "'{}'", `["<(0,0),1>"]`, `[]`},
	{"c_tsvector_arr", "tsvector[]", "ARRAY['a fat'::tsvector]", "'{}'", `["'a' 'fat'"]`, `[]`},
	{"c_tsquery_arr", "tsquery[]", "ARRAY['fat & rat'::tsquery]", "'{}'", `["'fat' & 'rat'"]`, `[]`},
	{"c_pg_lsn_arr", "pg_lsn[]", "'{16/B374D848,0/0}'", "'{}'", `["16/B374D848","0/0"]`, `[]`},
	{"c_regclass_arr", "regclass[]", "'{pg_class,pg_type}'", "'{}'", `["pg_class","pg_type"]`, `[]`},
	{"c_regtype_arr", "regtype[]", `'{integer,"double precision"}'`, "'{}'", `["integer","double precision"]`, `[]`},
	{"c_regproc_arr", "regproc[]", "'{now}'", "'{}'", `["now"]`, `[]`},
	{"c_regprocedure_arr", "regprocedure[]", `'{"abs(integer)"}'`, "'{}'", `["abs(integer)"]`, `[]`},
	{"c_regoper_arr", "regoper[]", `'{"||/"}'`, "'{}'", `["||/"]`, `[]`},
	{"c_regoperator_arr", "regoperator[]", `'{"+(integer,integer)"}'`, "'{}'", `["+(integer,integer)"]`, `[]`},
	{"c_regconfig_arr", "regconfig[]", "'{english}'", "'{}'", `["english"]`, `[]`},
	{"c_regdictionary_arr", "regdictionary[]", "'{simple}'", "'{}'", `["simple"]`, `[]`},
	{"c_regnamespace_arr", "regnamespace[]", "'{public}'", "'{}'", `["public"]`, `[]`},
	{"c_regrole_arr", "regrole[]", "'{postgres}'", "'{}'", `["postgres"]`, `[]`},
	{"c_regcollation_arr", "regcollation[]", `'{"\"C\""}'`, "'{}'", `["\"C\""]`, `[]`},
	{"c_tid_arr", "tid[]", `'{"(0,1)"}'`, "'{}'", `["(0,1)"]`, `[]`},
	{"c_int2vector_arr", "int2vector[]", `'{"1 2"}'`, "'{}'", `["1 2"]`, `[]`},
	{"c_oidvector_arr", "oidvector[]", `'{"1 2"}'`, "'{}'", `["1 2"]`, `[]`},
	{"c_aclitem_arr", "aclitem[]", "'{postgres=arwdDxt/postgres}'", "'{}'", `["postgres=arwdDxt/postgres"]`, `[]`},
	{"c_refcursor_arr", "refcursor[]", "'{cur}'", "'{}'", `["cur"]`, `[]`},
	{"c_txid_snapshot_arr", "txid_snapshot[]", `'{"10:20:10,14,15"}'`, "'{}'", `["10:20:10,14,15"]`, `[]`},
	{"c_pg_snapshot_arr", "pg_snapshot[]", `'{"10:20:10,14,15"}'`, "'{}'", `["10:20:10,14,15"]`, `[]`},
	{
		"c_int8range_arr", "int8range[]", `'{"[1,2)"}'`, "'{}'",
		`[{"lower":1,"upper":2,"lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_numrange_arr", "numrange[]", `'{"(,5.5]"}'`, "'{}'",
		`[{"lower":null,"upper":"5.5","lower_inc":false,"upper_inc":true}]`, `[]`,
	},
	{
		"c_daterange_arr", "daterange[]", `'{"[2026-01-01,2026-02-01)"}'`, "'{}'",
		`[{"lower":"2026-01-01","upper":"2026-02-01","lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_tsrange_arr", "tsrange[]", `'{"[\"2026-01-01 00:00\",\"2026-01-02 00:00\")"}'`, "'{}'",
		`[{"lower":"2026-01-01T00:00:00","upper":"2026-01-02T00:00:00","lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_tstzrange_arr", "tstzrange[]", `'{"[2026-01-01 00:00+00,infinity)"}'`, "'{}'",
		`[{"lower":"2026-01-01T00:00:00+00:00","upper":"infinity","lower_inc":true,"upper_inc":false}]`, `[]`,
	},
	{
		"c_int4mr_arr", "int4multirange[]", `'{"{[1,3)}"}'`, "'{}'",
		`[[{"lower":1,"upper":3,"lower_inc":true,"upper_inc":false}]]`, `[]`,
	},
	{
		"c_int8mr_arr", "int8multirange[]", `'{"{[1,2)}"}'`, "'{}'",
		`[[{"lower":1,"upper":2,"lower_inc":true,"upper_inc":false}]]`, `[]`,
	},
	{
		"c_nummr_arr", "nummultirange[]", `'{"{(,0)}"}'`, "'{}'",
		`[[{"lower":null,"upper":"0","lower_inc":false,"upper_inc":false}]]`, `[]`,
	},
	{
		"c_datemr_arr", "datemultirange[]", `'{"{[2026-01-01,2026-02-01)}"}'`, "'{}'",
		`[[{"lower":"2026-01-01","upper":"2026-02-01","lower_inc":true,"upper_inc":false}]]`, `[]`,
	},
	{
		"c_tsmr_arr", "tsmultirange[]", `'{"{[\"2026-01-01 00:00\",\"2026-01-02 00:00\")}"}'`, "'{}'",
		`[[{"lower":"2026-01-01T00:00:00","upper":"2026-01-02T00:00:00","lower_inc":true,"upper_inc":false}]]`, `[]`,
	},
	{
		"c_tstzmr_arr", "tstzmultirange[]", `'{"{[2026-01-01 00:00+00,infinity)}"}'`, "'{}'",
		`[[{"lower":"2026-01-01T00:00:00+00:00","upper":"infinity","lower_inc":true,"upper_inc":false}]]`, `[]`,
	},
	// Types outside the mapping table: text output.
	{"c_mood", "wak07.mood", "'happy'", "'sad'", `"happy"`, `"sad"`},
	{"c_mood_arr", "wak07.mood[]", "'{sad,happy}'", "'{}'", `"{sad,happy}"`, `"{}"`},
	{"c_domain", "wak07.posint", "5", "2147483647", `"5"`, `"2147483647"`},
	{"c_composite", "wak07.pair", "ROW(1, 'x y')", "ROW(NULL, '')", `"(1,\"x y\")"`, `"(,\"\")"`},
	{"c_citext", "citext", "'MiXeD'", "'ÄÖ'", `"MiXeD"`, `"ÄÖ"`},
}

// Row ids: rows 1-3 exist before Wake starts (snapshot `r` events), rows
// 101-103 are inserted while streaming (`c` events); row 101 is then updated
// to the values of row B (`u` event with the full after image).
const (
	rowA, rowB, rowC = 1, 2, 3
	streamOffset     = 100
	toastBodyBytes   = 10000
)

func wak07DDL() string {
	var cols strings.Builder
	for _, c := range typeColumns {
		fmt.Fprintf(&cols, ",\n  %s %s", c.name, c.sqlType)
	}
	return `DROP SCHEMA IF EXISTS wak07 CASCADE;
CREATE SCHEMA wak07;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE TYPE wak07.mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN wak07.posint AS int CHECK (VALUE > 0);
CREATE TYPE wak07.pair AS (a int, b text);
CREATE TABLE wak07.types (
  id bigint PRIMARY KEY` + cols.String() + `
);
CREATE TABLE wak07.docs (id int PRIMARY KEY, title text, body text);
ALTER TABLE wak07.docs ALTER COLUMN body SET STORAGE EXTERNAL;
CREATE TABLE wak07.docs_full (id int PRIMARY KEY, title text, body text);
ALTER TABLE wak07.docs_full ALTER COLUMN body SET STORAGE EXTERNAL;
ALTER TABLE wak07.docs_full REPLICA IDENTITY FULL;`
}

func typesInsert(id, row int) string {
	names, vals := typesLiterals(row)
	return fmt.Sprintf("INSERT INTO wak07.types (id, %s) VALUES (%d, %s)", strings.Join(names, ", "), id,
		strings.Join(vals, ", "))
}

// typesUpdate sets every column of row id to the literals of row: the new
// values are written in the transaction, so the after image is complete even
// where Postgres stores the row's values out of line (an UPDATE that leaves a
// TOASTed value untouched would legitimately list it in unchanged_columns).
func typesUpdate(id, row int) string {
	names, vals := typesLiterals(row)
	return fmt.Sprintf("UPDATE wak07.types SET (%s) = ROW(%s) WHERE id = %d", strings.Join(names, ", "),
		strings.Join(vals, ", "), id)
}

func typesLiterals(row int) (names, vals []string) {
	names = make([]string, 0, len(typeColumns))
	vals = make([]string, 0, len(typeColumns))
	for _, c := range typeColumns {
		names = append(names, c.name)
		switch row {
		case rowA:
			vals = append(vals, c.litA)
		case rowB:
			vals = append(vals, c.litB)
		default:
			vals = append(vals, "NULL")
		}
	}
	return names, vals
}

// typesImage is the expected after image of a types row.
func typesImage(id, row int) map[string]string {
	img := map[string]string{"id": strconv.Itoa(id)}
	for _, c := range typeColumns {
		switch row {
		case rowA:
			img[c.name] = c.wantA
		case rowB:
			img[c.name] = c.wantB
		default:
			img[c.name] = "null"
		}
	}
	return img
}

// expectedEvent is one event Wake must emit with literal JSON images.
type expectedEvent struct {
	table string
	op    event.Op
	id    int
	// before and after map column -> JSON; nil means JSON null.
	before, after map[string]string
	unchanged     []string
}

func docImage(id int, title, body string) map[string]string {
	return map[string]string{"id": strconv.Itoa(id), "title": strconv.Quote(title), "body": strconv.Quote(body)}
}

func wak07Expected() []expectedEvent {
	var exp []expectedEvent
	for _, row := range []int{rowA, rowB, rowC} {
		exp = append(exp,
			expectedEvent{table: "types", op: event.OpRead, id: row, after: typesImage(row, row)},
			expectedEvent{table: "types", op: event.OpCreate, id: row + streamOffset, after: typesImage(row+streamOffset, row)},
		)
	}
	bodyX, bodyY := strings.Repeat("x", toastBodyBytes), strings.Repeat("y", toastBodyBytes)
	keyOnly := func(id int) map[string]string { return map[string]string{"id": strconv.Itoa(id)} }
	exp = append(exp,
		expectedEvent{
			table: "types", op: event.OpUpdate, id: rowA + streamOffset, before: keyOnly(rowA + streamOffset),
			after: typesImage(rowA+streamOffset, rowB),
		},
		// Default replica identity: unchanged TOAST is omitted and listed.
		expectedEvent{table: "docs", op: event.OpRead, id: 1, after: docImage(1, "a", bodyX)},
		expectedEvent{table: "docs", op: event.OpCreate, id: 2, after: docImage(2, "a", bodyY)},
		expectedEvent{
			table: "docs", op: event.OpUpdate, id: 1, before: keyOnly(1),
			after: map[string]string{"id": "1", "title": `"b"`}, unchanged: []string{"body"},
		},
		expectedEvent{
			table: "docs", op: event.OpUpdate, id: 2, before: keyOnly(2),
			after: map[string]string{"id": "2", "title": `"b"`}, unchanged: []string{"body"},
		},
		expectedEvent{table: "docs", op: event.OpDelete, id: 2, before: keyOnly(2)},
		// REPLICA IDENTITY FULL: the value is copied from before, never listed.
		expectedEvent{table: "docs_full", op: event.OpRead, id: 1, after: docImage(1, "a", bodyX)},
		expectedEvent{table: "docs_full", op: event.OpCreate, id: 2, after: docImage(2, "a", bodyY)},
		expectedEvent{
			table: "docs_full", op: event.OpUpdate, id: 1, before: docImage(1, "a", bodyX),
			after: docImage(1, "b", bodyX),
		},
		expectedEvent{
			table: "docs_full", op: event.OpUpdate, id: 2, before: docImage(2, "a", bodyY),
			after: docImage(2, "b", bodyY),
		},
		expectedEvent{table: "docs_full", op: event.OpDelete, id: 2, before: docImage(2, "b", bodyY)},
	)
	return exp
}

const wak07StreamSQL = `INSERT INTO wak07.docs VALUES (2, 'a', repeat('y', %d));
UPDATE wak07.docs SET title = 'b' WHERE id IN (1, 2);
DELETE FROM wak07.docs WHERE id = 2;
INSERT INTO wak07.docs_full VALUES (2, 'a', repeat('y', %d));
UPDATE wak07.docs_full SET title = 'b' WHERE id IN (1, 2);
DELETE FROM wak07.docs_full WHERE id = 2;`

// TestAcceptance_WAK07_TypeFidelity implements WAK-07: Type fidelity for every type in type-mapping.md
// including unchanged TOAST.
func TestAcceptance_WAK07_TypeFidelity(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-07"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	minPct, err := c.Float("json_match_pct_min")
	failIf(t, id, err, "threshold json_match_pct_min")

	stack.EnsureUp(t)
	pg := stack.ConnectPG(t, 2)
	pg.Exec(t, wak07DDL())
	for _, row := range []int{rowA, rowB, rowC} {
		pg.Exec(t, typesInsert(row, row))
	}
	pg.Exec(t, fmt.Sprintf(`INSERT INTO wak07.docs VALUES (1, 'a', repeat('x', %d));
INSERT INTO wak07.docs_full VALUES (1, 'a', repeat('x', %d));`, toastBodyBytes, toastBodyBytes))

	secret := "wak07-secret"
	recv := stack.StartReceiver(t, stack.ReceiverOptions{Secret: []byte(secret)})
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID:         id,
		Config:     wakeConfig("wak07", []string{"wak07.*"}, webhookSink(recv.URL("wak07"), "WAKE_WEBHOOK_SECRET"), ""),
		Env:        map[string]string{"WAKE_WEBHOOK_SECRET": secret},
		FreshState: true,
	})

	for _, row := range []int{rowA, rowB, rowC} {
		pg.Exec(t, typesInsert(row+streamOffset, row))
	}
	pg.Exec(t, typesUpdate(rowA+streamOffset, rowB))
	pg.Exec(t, fmt.Sprintf(wak07StreamSQL, toastBodyBytes, toastBodyBytes))

	ctx := withTimeout(t, 3*time.Minute)
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN")
	expected := wak07Expected()
	err = recv.WaitItems(ctx, func(items [][]byte) bool { return countEvents(items) >= len(expected) })
	failIf(t, id, err, fmt.Sprintf("wait for %d events", len(expected)))

	dec, err := stack.DecodePayloads(recv.Items())
	failIf(t, id, err, "decode webhook payloads")
	res := compareTypeEvents(expected, dec.Events)
	stats := recv.Stats()
	pct := stack.Pct(res.matched, res.total)
	pass := pct >= minPct && res.total > 0
	details := fmt.Sprintf("%d/%d JSON values match over %d expected events (%d columns per types row, "+
		"snapshot r and stream c/u/d, unchanged TOAST with default and FULL identity); signatures valid %d/%d",
		res.matched, res.total, len(expected), len(typeColumns)+1, stats.SignatureValid,
		stats.SignatureValid+stats.SignatureFailed)
	if len(res.mismatches) > 0 {
		details += "; mismatches: " + strings.Join(res.mismatches, "; ")
	}
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"json_match_pct": pct, "values_checked": res.total, "values_matched": res.matched,
			"events_expected": len(expected), "events_received": len(dec.Events), "types_columns": len(typeColumns),
		},
		Pass:    pass,
		Details: details,
	})
}

func countEvents(items [][]byte) int {
	n := 0
	for _, it := range items {
		if k, err := event.KindOf(it); err == nil && k == event.KindEvent {
			n++
		}
	}
	return n
}

type compareResult struct {
	total, matched int
	mismatches     []string
}

func (r *compareResult) check(ok bool, format string, args ...any) {
	r.total++
	if ok {
		r.matched++
		return
	}
	if len(r.mismatches) < 40 {
		r.mismatches = append(r.mismatches, fmt.Sprintf(format, args...))
	}
}

// compareTypeEvents checks every expected event: each column of its after
// and before images, the absence of other columns, and unchanged_columns.
// A missing event counts all its values as mismatches.
func compareTypeEvents(expected []expectedEvent, events []*event.Event) compareResult {
	var res compareResult
	byKey := map[string]*event.Event{}
	for _, e := range events {
		if e.Source.Schema != "wak07" {
			continue
		}
		k := fmt.Sprintf("%s/%s/%s", e.Source.Table, e.Op, e.Key["id"])
		if _, dup := byKey[k]; !dup {
			byKey[k] = e
		}
	}
	for _, x := range expected {
		label := fmt.Sprintf("%s %s id=%d", x.table, x.op, x.id)
		e := byKey[fmt.Sprintf("%s/%s/%d", x.table, x.op, x.id)]
		if e == nil {
			for range len(x.after) + len(x.before) + 3 {
				res.check(false, "%s: event missing", label)
			}
			continue
		}
		compareImage(&res, label+" after", x.after, e.After)
		compareImage(&res, label+" before", x.before, e.Before)
		res.check(slices.Equal(x.unchanged, e.UnchangedColumns), "%s: unchanged_columns %v, want %v", label,
			e.UnchangedColumns, x.unchanged)
	}
	return res
}

func compareImage(res *compareResult, label string, want map[string]string, got event.Row) {
	if want == nil {
		res.check(got == nil, "%s: want null, got %d columns", label, len(got))
		return
	}
	res.check(got != nil, "%s: image is null", label)
	cols := make([]string, 0, len(want))
	for col := range want {
		cols = append(cols, col)
	}
	slices.Sort(cols)
	for _, col := range cols {
		raw, ok := got[col]
		if !ok {
			res.check(false, "%s.%s: missing", label, col)
			continue
		}
		eq, err := jsonEqual(raw, []byte(want[col]))
		res.check(err == nil && eq, "%s.%s: got %s, want %s", label, col, truncate(string(raw), 120),
			truncate(want[col], 120))
	}
	var extra []string
	for col := range got {
		if _, ok := want[col]; !ok {
			extra = append(extra, col)
		}
	}
	slices.Sort(extra)
	res.check(len(extra) == 0, "%s: unexpected columns %v", label, extra)
}
