// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/thnhdatle/wake/pkg/event"
)

const exampleCreate = `{"id":"0/16B3748-3","source":{"db":"app","schema":"public","table":"ledger"},"op":"c",` +
	`"ts_ms":1760600000000,"lsn":"0/16B3748","tx_id":7812,"key":{"id":9223372036854775807},"before":null,` +
	`"after":{"id":9223372036854775807,"amount":9007199254740993,"note":"a<b"},"schema_version":3}`

func ExampleDecode() {
	e, err := event.Decode([]byte(exampleCreate))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	amount, _ := e.After.Value("amount")
	n, _ := amount.Int64()
	fmt.Println(e.Op, e.Source.Table, n)

	_, err = event.Decode(nil)
	fmt.Println(errors.Is(err, event.ErrTombstone))

	_, err = event.Decode([]byte(`{"id":"bad"}`))
	fmt.Println(errors.Is(err, event.ErrInvalid))
	// Output:
	// c ledger 9007199254740993
	// true
	// true
}

func ExampleDecodeSchemaChange() {
	data := `{"type":"schema_change","id":"0/16C0000-s0","source":{"db":"app","schema":"public","table":"users"},` +
		`"lsn":"0/16C0000","ts_ms":1760600100000,"schema_version":4,"columns_before":null,` +
		`"columns_after":[{"name":"id","type":"int8"}]}`
	sc, err := event.DecodeSchemaChange([]byte(data))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(sc.SchemaVersion, sc.ColumnsBefore == nil, sc.ColumnsAfter[0].Type)
	// Output: 4 true int8
}

func ExampleDecodeMessage() {
	data := `{"type":"message","id":"0/16B3748-m0","lsn":"0/16B3748","ts_ms":1760600000000,` +
		`"transactional":true,"prefix":"app.audit","content_base64":"aGVsbG8="}`
	m, err := event.DecodeMessage([]byte(data))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(m.Prefix, string(m.Content))
	// Output: app.audit hello
}

func ExampleKindOf() {
	for _, data := range []string{
		exampleCreate,
		`{"type":"schema_change"}`,
		`{"type":"message"}`,
	} {
		kind, err := event.KindOf([]byte(data))
		fmt.Println(kind, err)
	}
	// Output:
	// event <nil>
	// schema_change <nil>
	// message <nil>
}

func ExampleOp_Known() {
	fmt.Println(event.OpUpdate.Known(), event.Op("x").Known())
	// Output: true false
}

func ExampleRow_Has() {
	row := event.Row{"id": json.RawMessage("1"), "gone": json.RawMessage("null")}
	fmt.Println(row.Has("id"), row.Has("gone"), row.Has("email"))
	// Output: true true false
}

func ExampleRow_Value() {
	row := event.Row{"id": json.RawMessage("42")}
	v, ok := row.Value("id")
	fmt.Println(string(v.Raw()), ok)
	_, ok = row.Value("email")
	fmt.Println(ok)
	// Output:
	// 42 true
	// false
}

func ExampleValue_Int64() {
	n, err := event.Value("9007199254740993").Int64()
	fmt.Println(n, err)
	_, err = event.Value(`"7"`).Int64()
	fmt.Println(errors.Is(err, event.ErrValueType))
	// Output:
	// 9007199254740993 <nil>
	// true
}

func ExampleValue_Uint64() {
	n, err := event.Value("18446744073709551615").Uint64()
	fmt.Println(n, err)
	// Output: 18446744073709551615 <nil>
}

func ExampleValue_BigInt() {
	n, err := event.Value("123456789012345678901234567890").BigInt()
	fmt.Println(n, err)
	// Output: 123456789012345678901234567890 <nil>
}

func ExampleValue_Number() {
	n, err := event.Value("12.50").Number()
	fmt.Println(n, err)
	// Output: 12.50 <nil>
}

func ExampleValue_String() {
	s, err := event.Value(`"a\"b"`).String()
	fmt.Println(s, err)
	// Output: a"b <nil>
}

func ExampleValue_Bool() {
	b, err := event.Value("true").Bool()
	fmt.Println(b, err)
	// Output: true <nil>
}

func ExampleValue_IsNull() {
	fmt.Println(event.Value("null").IsNull(), event.Value("0").IsNull())
	// Output: true false
}

func ExampleValue_Raw() {
	fmt.Println(string(event.Value(` {"a":1} `).Raw()))
	// Output: {"a":1}
}

func ExampleValue_Text() {
	for _, v := range []string{"9223372036854775807", `"a<b"`, "true", "null"} {
		text, err := event.Value(v).Text()
		fmt.Printf("%q %v\n", text, err != nil)
	}
	// Output:
	// "9223372036854775807" false
	// "a<b" false
	// "true" false
	// "" true
}

func ExampleParseLSN() {
	lsn, err := event.ParseLSN("1/0")
	fmt.Println(lsn, err)
	// Output: 4294967296 <nil>
}

func ExampleFormatLSN() {
	fmt.Println(event.FormatLSN(0x16B3748))
	// Output: 0/16B3748
}

func ExampleParseSeq() {
	for _, id := range []string{"0/16B3748-3", "0/16D0010-r2-10000", "0/16C0000-s0"} {
		seq, err := event.ParseSeq(id)
		fmt.Printf("%+v %v\n", seq, err)
	}
	// Output:
	// {LSN:23803720 K:3 Schema:false} <nil>
	// {LSN:23920656 K:10000 Schema:false} <nil>
	// {LSN:23855104 K:0 Schema:true} <nil>
}

func ExampleCompare() {
	a, _ := event.ParseSeq("0/A-0")
	b, _ := event.ParseSeq("0/9-0")
	fmt.Println(event.Compare(a, b)) // numeric, although "0/A" < "0/9" as strings is false

	schema, _ := event.ParseSeq("0/A-s0")
	fmt.Println(event.Compare(schema, a))
	// Output:
	// 1
	// -1
}

func ExampleEvent_Seq() {
	e, _ := event.Decode([]byte(exampleCreate))
	seq, err := e.Seq()
	fmt.Println(seq.K, err)
	// Output: 3 <nil>
}

func ExampleSchemaChange_Seq() {
	sc := &event.SchemaChange{ID: "0/16C0000-s2"}
	seq, err := sc.Seq()
	fmt.Println(seq.Schema, seq.K, err)
	// Output: true 2 <nil>
}

func ExampleEvent_KeyChanged() {
	e := &event.Event{
		Op:     event.OpUpdate,
		Key:    event.Row{"id": json.RawMessage("43")},
		Before: event.Row{"id": json.RawMessage("42")},
	}
	fmt.Println(e.KeyChanged())
	// Output: true
}

func ExampleIsKafkaTombstone() {
	fmt.Println(event.IsKafkaTombstone(nil), event.IsKafkaTombstone([]byte(`{}`)))
	// Output: true false
}

func ExampleMergeUnchanged() {
	e := &event.Event{
		Op:               event.OpUpdate,
		After:            event.Row{"id": json.RawMessage("7")},
		UnchangedColumns: []string{"body", "blob"},
	}
	prev := event.Row{"id": json.RawMessage("7"), "body": json.RawMessage(`"old text"`)}
	event.MergeUnchanged(prev, e)
	fmt.Println(string(e.After["body"]), e.UnchangedColumns)
	// Output: "old text" [blob]
}
