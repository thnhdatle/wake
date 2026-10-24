// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/schema"
)

type mappingCase struct {
	name string
	oid  uint32
	text string
	want string
}

func runMappingCases(t *testing.T, cases []mappingCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prefix := []byte(`[`)
			got, err := schema.AppendJSON(prefix, tc.oid, []byte(tc.text))
			require.NoError(t, err)
			assert.Equal(t, `[`+tc.want, string(got))
			assert.True(t, json.Valid(got[1:]), "output must be valid JSON: %s", got[1:])
		})
	}
}

func TestAppendJSON_Scalars(t *testing.T) {
	t.Parallel()
	runMappingCases(t, []mappingCase{
		{name: "int2 min", oid: schema.OIDInt2, text: "-32768", want: "-32768"},
		{name: "int4 max", oid: schema.OIDInt4, text: "2147483647", want: "2147483647"},
		{name: "int8 max", oid: schema.OIDInt8, text: "9223372036854775807", want: "9223372036854775807"},
		{name: "int8 min", oid: schema.OIDInt8, text: "-9223372036854775808", want: "-9223372036854775808"},
		{name: "int8 zero", oid: schema.OIDInt8, text: "0", want: "0"},
		{name: "oid max", oid: schema.OIDOID, text: "4294967295", want: "4294967295"},
		{name: "xid8", oid: schema.OIDXID8, text: "18446744073709551615", want: "18446744073709551615"},
		{name: "numeric", oid: schema.OIDNumeric, text: "1234567890123456.1234", want: `"1234567890123456.1234"`},
		{name: "numeric NaN", oid: schema.OIDNumeric, text: "NaN", want: `"NaN"`},
		{name: "numeric Infinity", oid: schema.OIDNumeric, text: "-Infinity", want: `"-Infinity"`},
		{name: "float4 Infinity", oid: schema.OIDFloat4, text: "Infinity", want: `"Infinity"`},
		{name: "float8 -Infinity", oid: schema.OIDFloat8, text: "-Infinity", want: `"-Infinity"`},
		{name: "float8 NaN", oid: schema.OIDFloat8, text: "NaN", want: `"NaN"`},
		{name: "float8 tiny", oid: schema.OIDFloat8, text: "-1.5e-300", want: "-1.5e-300"},
		{name: "float8 big", oid: schema.OIDFloat8, text: "1e+100", want: "1e+100"},
		{name: "float8 padded exponent", oid: schema.OIDFloat8, text: "1e-05", want: "1e-05"},
		{name: "float8 negative zero", oid: schema.OIDFloat8, text: "-0", want: "-0"},
		{name: "float4 plain", oid: schema.OIDFloat4, text: "0.1", want: "0.1"},
		{name: "float4 max", oid: schema.OIDFloat4, text: "3.4028235e+38", want: "3.4028235e+38"},
		{name: "bool true", oid: schema.OIDBool, text: "t", want: "true"},
		{name: "bool false", oid: schema.OIDBool, text: "f", want: "false"},
		{
			name: "text escapes", oid: schema.OIDText, text: "he said \"hi\" <&> é ☃\n\\",
			want: `"he said \"hi\" <&> é ☃\n\\"`,
		},
		{name: "varchar", oid: schema.OIDVarchar, text: "varchar", want: `"varchar"`},
		{name: "bpchar keeps padding", oid: schema.OIDBPChar, text: "ab   ", want: `"ab   "`},
		{name: "name", oid: schema.OIDName, text: "pg_name", want: `"pg_name"`},
		{name: "char", oid: schema.OIDChar, text: "x", want: `"x"`},
		{
			name: "uuid", oid: schema.OIDUUID, text: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			want: `"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`,
		},
		{name: "money", oid: schema.OIDMoney, text: "-$1.50", want: `"-$1.50"`},
		{name: "inet", oid: schema.OIDInet, text: "192.168.0.1/24", want: `"192.168.0.1/24"`},
		{name: "cidr", oid: schema.OIDCIDR, text: "10.0.0.0/8", want: `"10.0.0.0/8"`},
		{name: "macaddr", oid: schema.OIDMacaddr, text: "08:00:2b:01:02:03", want: `"08:00:2b:01:02:03"`},
		{name: "xml", oid: schema.OIDXML, text: "<a>1</a>", want: `"<a>1</a>"`},
		{name: "point", oid: schema.OIDPoint, text: "(1.5,2)", want: `"(1.5,2)"`},
		{name: "bytea hex", oid: schema.OIDBytea, text: `\xdeadbeef00`, want: `"3q2+7wA="`},
		{name: "bytea hex upper", oid: schema.OIDBytea, text: `\xDEADBEEF`, want: `"3q2+7w=="`},
		{name: "bytea empty", oid: schema.OIDBytea, text: `\x`, want: `""`},
		{name: "bytea escape", oid: schema.OIDBytea, text: `\000\377\\A`, want: `"AP9cQQ=="`},
		{name: "bytea escape plain", oid: schema.OIDBytea, text: `abc`, want: `"YWJj"`},
		{
			name: "json compacted, order kept", oid: schema.OIDJSON, text: `{"b": 1, "a": [1, 2]}`,
			want: `{"b":1,"a":[1,2]}`,
		},
		{name: "jsonb", oid: schema.OIDJSONB, text: `{"a": [1, 2], "b": 1}`, want: `{"a":[1,2],"b":1}`},
		{name: "json no html escaping", oid: schema.OIDJSON, text: ` "<&>" `, want: `"<&>"`},
		{
			name: "json big number", oid: schema.OIDJSONB, text: `[18446744073709551616]`,
			want: `[18446744073709551616]`,
		},
		{name: "unknown enum", oid: 16387, text: "happy", want: `"happy"`},
		{name: "unknown builtin", oid: 705, text: "x\ty", want: `"x\ty"`},
	})
}

func TestAppendJSON_DateTime(t *testing.T) {
	t.Parallel()
	runMappingCases(t, []mappingCase{
		{name: "date", oid: schema.OIDDate, text: "2026-10-18", want: `"2026-10-18"`},
		{name: "date BC", oid: schema.OIDDate, text: "0044-03-15 BC", want: `"-0043-03-15"`},
		{name: "date 1 BC", oid: schema.OIDDate, text: "0001-01-01 BC", want: `"0000-01-01"`},
		{name: "date 5 digit year", oid: schema.OIDDate, text: "10000-01-01", want: `"10000-01-01"`},
		{name: "date infinity", oid: schema.OIDDate, text: "infinity", want: `"infinity"`},
		{name: "date -infinity", oid: schema.OIDDate, text: "-infinity", want: `"-infinity"`},
		{name: "time", oid: schema.OIDTime, text: "12:34:56.789", want: `"12:34:56.789"`},
		{name: "time 24", oid: schema.OIDTime, text: "24:00:00", want: `"24:00:00"`},
		{name: "timetz +05:30", oid: schema.OIDTimeTZ, text: "12:34:56+05:30", want: `"12:34:56+05:30"`},
		{name: "timetz -08", oid: schema.OIDTimeTZ, text: "12:00:00.5-08", want: `"12:00:00.5-08:00"`},
		{
			name: "timetz seconds offset to utc", oid: schema.OIDTimeTZ, text: "12:00:00+05:53:28",
			want: `"06:06:32+00:00"`,
		},
		{
			name: "timetz seconds offset wraps", oid: schema.OIDTimeTZ, text: "22:00:00.25-04:56:02",
			want: `"02:56:02.25+00:00"`,
		},
		{
			name: "timestamp", oid: schema.OIDTimestamp, text: "2026-10-18 12:34:56.123456",
			want: `"2026-10-18T12:34:56.123456"`,
		},
		{
			name: "timestamp BC", oid: schema.OIDTimestamp, text: "0044-03-15 12:00:00.5 BC",
			want: `"-0043-03-15T12:00:00.5"`,
		},
		{name: "timestamp infinity", oid: schema.OIDTimestamp, text: "infinity", want: `"infinity"`},
		{
			name: "timestamptz +00", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:34:56.5+00",
			want: `"2026-10-18T12:34:56.5+00:00"`,
		},
		{
			name: "timestamptz +02", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:34:56.123+02",
			want: `"2026-10-18T12:34:56.123+02:00"`,
		},
		{
			name: "timestamptz +05:30", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:34:56+05:30",
			want: `"2026-10-18T12:34:56+05:30"`,
		},
		{
			name: "timestamptz -08", oid: schema.OIDTimestampTZ, text: "2026-10-18 01:02:03.000001-08",
			want: `"2026-10-18T01:02:03.000001-08:00"`,
		},
		{
			name: "timestamptz lmt offset", oid: schema.OIDTimestampTZ, text: "1850-01-01 00:00:00-04:56:02",
			want: `"1850-01-01T04:56:02+00:00"`,
		},
		{
			name: "timestamptz lmt BC", oid: schema.OIDTimestampTZ, text: "0044-03-15 12:00:00.25-04:56:02 BC",
			want: `"-0043-03-15T16:56:02.25+00:00"`,
		},
		{
			name: "timestamptz lmt day wrap", oid: schema.OIDTimestampTZ, text: "1850-12-31 23:00:00-04:56:02",
			want: `"1851-01-01T03:56:02+00:00"`,
		},
		{name: "timestamptz -infinity", oid: schema.OIDTimestampTZ, text: "-infinity", want: `"-infinity"`},
	})
}

func TestAppendJSON_Interval(t *testing.T) {
	t.Parallel()
	runMappingCases(t, []mappingCase{
		{
			name: "mixed signs", oid: schema.OIDInterval, text: "1 year 2 mons -3 days +04:05:06.5",
			want: `"P1Y2M-3DT4H5M6.5S"`,
		},
		{
			name: "all negative", oid: schema.OIDInterval, text: "-1 years -2 mons +3 days -04:05:06.000001",
			want: `"P-1Y-2M3DT-4H-5M-6.000001S"`,
		},
		{name: "negative second", oid: schema.OIDInterval, text: "-00:00:01", want: `"PT-1S"`},
		{name: "negative fraction", oid: schema.OIDInterval, text: "-00:00:01.5", want: `"PT-1.5S"`},
		{name: "zero", oid: schema.OIDInterval, text: "00:00:00", want: `"PT0S"`},
		{name: "negative days positive time", oid: schema.OIDInterval, text: "-2 days +03:00:00", want: `"P-2DT3H"`},
		{name: "hours beyond a day", oid: schema.OIDInterval, text: "100:00:00", want: `"PT100H"`},
		{name: "minutes", oid: schema.OIDInterval, text: "00:30:00", want: `"PT30M"`},
		{name: "negative month", oid: schema.OIDInterval, text: "-1 mons", want: `"P-1M"`},
		{name: "one day", oid: schema.OIDInterval, text: "1 day", want: `"P1D"`},
		{name: "fraction only", oid: schema.OIDInterval, text: "1 day 00:00:00.000001", want: `"P1DT0.000001S"`},
		{name: "max", oid: schema.OIDInterval, text: "2562047788:00:54.775807", want: `"PT2562047788H54.775807S"`},
		{name: "huge years", oid: schema.OIDInterval, text: "-178000000 years", want: `"P-178000000Y"`},
		{name: "iso passthrough", oid: schema.OIDInterval, text: "P1Y2M-3DT4H5M6.5S", want: `"P1Y2M-3DT4H5M6.5S"`},
		{name: "infinity", oid: schema.OIDInterval, text: "infinity", want: `"infinity"`},
		{name: "-infinity", oid: schema.OIDInterval, text: "-infinity", want: `"-infinity"`},
	})
}

func TestAppendJSON_Arrays(t *testing.T) {
	t.Parallel()
	runMappingCases(t, []mappingCase{
		{name: "int4 with null", oid: schema.OIDInt4Array, text: "{1,NULL,3}", want: `[1,null,3]`},
		{
			name: "text quoted", oid: schema.OIDTextArray, text: `{"a,b","c\"d","e\\f",NULL,"","NULL"}`,
			want: `["a,b","c\"d","e\\f",null,"","NULL"]`,
		},
		{name: "int8 2d", oid: schema.OIDInt8Array, text: "{{1,2},{3,4}}", want: `[[1,2],[3,4]]`},
		{name: "3d", oid: schema.OIDInt4Array, text: "{{{1}}}", want: `[[[1]]]`},
		{name: "empty", oid: schema.OIDInt4Array, text: "{}", want: `[]`},
		{name: "empty nested", oid: schema.OIDInt4Array, text: "{{},{}}", want: `[[],[]]`},
		{name: "lower bounds dropped", oid: schema.OIDInt4Array, text: "[0:1]={1,2}", want: `[1,2]`},
		{name: "text nested quoted", oid: schema.OIDTextArray, text: `{{"a b",c}}`, want: `[["a b","c"]]`},
		{name: "unquoted escape", oid: schema.OIDTextArray, text: `{ab\"c, d\ ,null}`, want: `["ab\"c","d ",null]`},
		{name: "spaces", oid: schema.OIDInt4Array, text: "{ 1 , 2 }", want: `[1,2]`},
		{name: "bytea", oid: schema.OIDByteaArray, text: `{"\\x01"}`, want: `["AQ=="]`},
		{name: "jsonb", oid: schema.OIDJSONBArray, text: `{"{\"a\": \"x,y\"}"}`, want: `[{"a":"x,y"}]`},
		{
			name: "int4range", oid: schema.OIDInt4RangeArray, text: `{"[1,2)",empty}`,
			want: `[` + rng(`1`, `2`, true, false) + `,{"empty":true}]`,
		},
		{
			name: "timestamp infinity", oid: schema.OIDTimestampArray, text: "{infinity,\"2026-10-18 12:00:00\"}",
			want: `["infinity","2026-10-18T12:00:00"]`,
		},
		{name: "numeric", oid: schema.OIDNumericArray, text: "{1.5,NaN}", want: `["1.5","NaN"]`},
		{
			name: "float8 specials", oid: schema.OIDFloat8Array, text: "{1.5,-Infinity,NaN}",
			want: `[1.5,"-Infinity","NaN"]`,
		},
		{
			name: "box semicolon", oid: schema.OIDBoxArray, text: "{(3,4),(1,2);(1,1),(0,0)}",
			want: `["(3,4),(1,2)","(1,1),(0,0)"]`,
		},
		{name: "point", oid: schema.OIDPointArray, text: `{"(1,2)"}`, want: `["(1,2)"]`},
		{name: "bool", oid: schema.OIDBoolArray, text: "{t,f,NULL}", want: `[true,false,null]`},
		{
			name: "uuid", oid: schema.OIDUUIDArray, text: "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
			want: `["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]`,
		},
		{name: "interval", oid: schema.OIDIntervalArray, text: `{"1 day","-00:00:01"}`, want: `["P1D","PT-1S"]`},
		{
			name: "date multirange", oid: schema.OIDDateMultirangeArray, text: `{"{[2020-01-01,2020-02-01)}"}`,
			want: `[[` + rng(`"2020-01-01"`, `"2020-02-01"`, true, false) + `]]`,
		},
	})
}

func TestAppendJSON_Ranges(t *testing.T) {
	t.Parallel()
	runMappingCases(t, []mappingCase{
		{name: "int4range", oid: schema.OIDInt4Range, text: "[1,10)", want: rng(`1`, `10`, true, false)},
		{
			name: "int8range big", oid: schema.OIDInt8Range, text: "[9007199254740993,9223372036854775807)",
			want: rng(`9007199254740993`, `9223372036854775807`, true, false),
		},
		{
			name: "numrange unbounded lower", oid: schema.OIDNumRange, text: "(,5.5]",
			want: rng(`null`, `"5.5"`, false, true),
		},
		{
			name: "numrange unbounded upper", oid: schema.OIDNumRange, text: "[1.5,)",
			want: rng(`"1.5"`, `null`, true, false),
		},
		{name: "unbounded both", oid: schema.OIDInt8Range, text: "(,)", want: rng(`null`, `null`, false, false)},
		{name: "empty", oid: schema.OIDDateRange, text: "empty", want: `{"empty":true}`},
		{
			name: "daterange", oid: schema.OIDDateRange, text: "[2020-01-01,2020-01-02)",
			want: rng(`"2020-01-01"`, `"2020-01-02"`, true, false),
		},
		{
			name: "tstzrange infinity bound", oid: schema.OIDTSTZRange, text: `["2026-01-01 00:00:00+00",infinity)`,
			want: rng(`"2026-01-01T00:00:00+00:00"`, `"infinity"`, true, false),
		},
		{
			name: "tsrange quoted", oid: schema.OIDTSRange, text: `["2020-01-01 00:00:00",)`,
			want: rng(`"2020-01-01T00:00:00"`, `null`, true, false),
		},
		{
			name: "doubled quote and escape", oid: schema.OIDNumRange, text: `["1""",\2]`,
			want: rng(`"1\""`, `"2"`, true, true),
		},
		{
			name: "int4multirange", oid: schema.OIDInt4Multirange, text: "{[1,3),[5,7)}",
			want: `[` + rng(`1`, `3`, true, false) + `,` + rng(`5`, `7`, true, false) + `]`,
		},
		{
			name: "multirange spaces", oid: schema.OIDInt4Multirange, text: "{[1,3), [5,7)}",
			want: `[` + rng(`1`, `3`, true, false) + `,` + rng(`5`, `7`, true, false) + `]`,
		},
		{name: "empty multirange", oid: schema.OIDInt4Multirange, text: "{}", want: `[]`},
		{
			name: "tstzmultirange", oid: schema.OIDTSTZMultirange, text: `{["2020-01-01 10:00:00+00","2020-01-01 11:00:00+00")}`,
			want: `[` + rng(`"2020-01-01T10:00:00+00:00"`, `"2020-01-01T11:00:00+00:00"`, true, false) + `]`,
		},
	})
}

func TestAppendJSON_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		oid  uint32
		text string
	}{
		{name: "int fraction", oid: schema.OIDInt4, text: "1.5"},
		{name: "int leading zero", oid: schema.OIDInt8, text: "007"},
		{name: "int empty", oid: schema.OIDInt8, text: ""},
		{name: "int plus", oid: schema.OIDInt2, text: "+1"},
		{name: "int minus only", oid: schema.OIDInt2, text: "-"},
		{name: "float exponent only", oid: schema.OIDFloat8, text: "1e"},
		{name: "float word", oid: schema.OIDFloat8, text: "abc"},
		{name: "float leading dot", oid: schema.OIDFloat8, text: ".5"},
		{name: "float trailing dot", oid: schema.OIDFloat8, text: "5."},
		{name: "bool", oid: schema.OIDBool, text: "x"},
		{name: "bytea odd hex", oid: schema.OIDBytea, text: `\x0`},
		{name: "bytea bad hex", oid: schema.OIDBytea, text: `\xzz`},
		{name: "bytea bad escape", oid: schema.OIDBytea, text: `\9`},
		{name: "bytea octal overflow", oid: schema.OIDBytea, text: `\477`},
		{name: "json", oid: schema.OIDJSON, text: "{"},
		{name: "date slashes", oid: schema.OIDDate, text: "10/18/2026"},
		{name: "date month", oid: schema.OIDDate, text: "2026-13-01"},
		{name: "date short year", oid: schema.OIDDate, text: "26-01-01"},
		{name: "date year zero", oid: schema.OIDDate, text: "0000-01-01"},
		{name: "date trailing", oid: schema.OIDDate, text: "2026-01-01x"},
		{name: "timestamp no time", oid: schema.OIDTimestamp, text: "2026-10-18"},
		{name: "timestamp bad hour", oid: schema.OIDTimestamp, text: "2026-10-18 25:00:00"},
		{name: "timestamp with offset", oid: schema.OIDTimestamp, text: "2026-10-18 12:00:00+01"},
		{name: "timestamp empty fraction", oid: schema.OIDTimestamp, text: "2026-10-18 12:00:00."},
		{name: "timestamptz no offset", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:00:00"},
		{name: "timestamptz short offset", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:00:00+1"},
		{name: "timestamptz offset minutes", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:00:00+01:60"},
		{name: "timestamptz offset too long", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:00:00+01:00:00:00"},
		{name: "timestamptz offset separator", oid: schema.OIDTimestampTZ, text: "2026-10-18 12:00:00+01-00"},
		{name: "time short", oid: schema.OIDTime, text: "12:00"},
		{name: "time trailing", oid: schema.OIDTime, text: "12:00:00x"},
		{name: "timetz no offset", oid: schema.OIDTimeTZ, text: "12:00:00"},
		{name: "interval unit", oid: schema.OIDInterval, text: "1 fortnight"},
		{name: "interval repeated unit", oid: schema.OIDInterval, text: "1 year 2 years"},
		{name: "interval verbose", oid: schema.OIDInterval, text: "@ 1 year"},
		{name: "interval sql standard", oid: schema.OIDInterval, text: "1-2"},
		{name: "interval number only", oid: schema.OIDInterval, text: "1"},
		{name: "interval two times", oid: schema.OIDInterval, text: "01:00:00 02:00:00"},
		{name: "interval bad time", oid: schema.OIDInterval, text: "1:60:00"},
		{name: "interval empty", oid: schema.OIDInterval, text: ""},
		{name: "interval iso short", oid: schema.OIDInterval, text: "P"},
		{name: "interval iso chars", oid: schema.OIDInterval, text: "P1X"},
		{name: "interval overflow", oid: schema.OIDInterval, text: "99999999999999999999 days"},
		{name: "array unterminated", oid: schema.OIDInt4Array, text: "{1,2"},
		{name: "array no braces", oid: schema.OIDInt4Array, text: "1,2"},
		{name: "array empty element", oid: schema.OIDInt4Array, text: "{1,}"},
		{name: "array unterminated quote", oid: schema.OIDTextArray, text: `{"a}`},
		{name: "array trailing", oid: schema.OIDInt4Array, text: "{1}x"},
		{name: "array bad dims", oid: schema.OIDInt4Array, text: "[1:2]{1,2}"},
		{name: "array bad element", oid: schema.OIDInt4Array, text: "{a}"},
		{name: "array quote inside unquoted", oid: schema.OIDTextArray, text: `{a"b}`},
		{name: "array trailing backslash", oid: schema.OIDTextArray, text: `{a\`},
		{name: "array quoted trailing backslash", oid: schema.OIDTextArray, text: `{"a\`},
		{name: "array too deep", oid: schema.OIDInt4Array, text: "{{{{{{{1}}}}}}}"},
		{name: "array garbage after element", oid: schema.OIDTextArray, text: `{"a"b}`},
		{name: "range unterminated", oid: schema.OIDInt4Range, text: "[1,2"},
		{name: "range no bracket", oid: schema.OIDInt4Range, text: "1,2)"},
		{name: "range no comma", oid: schema.OIDInt4Range, text: "[1;2)"},
		{name: "range bad bound", oid: schema.OIDInt4Range, text: "[a,2)"},
		{name: "range bad upper", oid: schema.OIDInt4Range, text: "[1,b)"},
		{name: "range trailing", oid: schema.OIDInt4Range, text: "[1,2)x"},
		{name: "range open quote", oid: schema.OIDNumRange, text: `["1,2)`},
		{name: "range trailing backslash", oid: schema.OIDNumRange, text: `[1\`},
		{name: "multirange unterminated", oid: schema.OIDInt4Multirange, text: "{[1,2)"},
		{name: "multirange no comma", oid: schema.OIDInt4Multirange, text: "{[1,2) [3,4)}"},
		{name: "multirange bad range", oid: schema.OIDInt4Multirange, text: "{[1,x)}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dst := []byte("prefix")
			got, err := schema.AppendJSON(dst, tc.oid, []byte(tc.text))
			require.ErrorIs(t, err, schema.ErrInvalidValue)
			assert.Equal(t, "prefix", string(got), "dst must be returned unchanged on error")
			var ve *schema.ValueError
			require.ErrorAs(t, err, &ve)
			assert.Equal(t, tc.oid, ve.TypeOID)
			assert.Contains(t, err.Error(), "schema: invalid ")
		})
	}
}

func TestValueError_Error_TruncatesText(t *testing.T) {
	t.Parallel()
	long := make([]byte, 1000)
	for i := range long {
		long[i] = 'x'
	}
	_, err := schema.AppendJSON(nil, schema.OIDInt8, long)
	var ve *schema.ValueError
	require.ErrorAs(t, err, &ve)
	assert.Len(t, ve.Text, 64)
	unknown := &schema.ValueError{TypeOID: 99999, Text: "v", Reason: "r"}
	assert.Equal(t, `schema: invalid oid:99999 value "v": r`, unknown.Error())
}

func TestIsKnownType_Table(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		oid      uint32
		known    bool
		typeName string
		elem     uint32
	}{
		{name: "int8", oid: schema.OIDInt8, known: true, typeName: "int8"},
		{name: "text array", oid: schema.OIDTextArray, known: true, typeName: "text[]", elem: schema.OIDText},
		{name: "box array", oid: schema.OIDBoxArray, known: true, typeName: "box[]", elem: schema.OIDBox},
		{
			name: "tstzrange", oid: schema.OIDTSTZRange, known: true, typeName: "tstzrange",
			elem: schema.OIDTimestampTZ,
		},
		{
			name: "datemultirange", oid: schema.OIDDateMultirange, known: true, typeName: "datemultirange",
			elem: schema.OIDDateRange,
		},
		{name: "enum", oid: 16387, typeName: ""},
		{name: "unknown builtin", oid: 705, typeName: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.known, schema.IsKnownType(tc.oid))
			name, ok := schema.TypeName(tc.oid)
			assert.Equal(t, tc.known, ok)
			assert.Equal(t, tc.typeName, name)
			elem, ok := schema.ElementType(tc.oid)
			assert.Equal(t, tc.elem != 0, ok)
			assert.Equal(t, tc.elem, elem)
		})
	}
	assert.Equal(t, "oid:16387", schema.FallbackTypeName(16387))
}

func rng(lower, upper string, lowerInc, upperInc bool) string {
	return fmt.Sprintf(`{"lower":%s,"upper":%s,"lower_inc":%t,"upper_inc":%t}`, lower, upper, lowerInc, upperInc)
}
