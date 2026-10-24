// SPDX-License-Identifier: Apache-2.0

package change_test

import (
	"strings"
	"testing"

	"github.com/thnhdatle/wake/internal/domain/change"
)

func benchEvents() []struct {
	name string
	ev   change.Event
} {
	id := change.StreamID(0x16B3748, 0)
	narrow := change.Event{
		ID: id, Source: change.Source{DB: "app", Schema: "wak05", Table: "t1"}, Op: change.OpCreate,
		TsMs: 1_792_308_604_187, TxID: 900, SchemaVersion: 1,
		Key: change.Row{{Name: "id", Value: []byte("1234567")}},
		After: change.Row{
			{Name: "id", Value: []byte("1234567")},
			{Name: "payload", Value: []byte(`"` + strings.Repeat("x", 168) + `"`)},
		},
	}
	key := change.Row{{Name: "id", Value: []byte("4242424242")}}
	wide := change.Event{
		ID: id, Source: change.Source{DB: "app", Schema: "public", Table: "orders"}, Op: change.OpUpdate,
		TsMs: 1_792_308_604_187, TxID: 901, SchemaVersion: 3, Key: key, Before: key,
		After: change.Row{
			{Name: "id", Value: []byte("4242424242")},
			{Name: "customer", Value: []byte(`"Ada Lovelace"`)},
			{Name: "amount", Value: []byte(`"12345.6789"`)},
			{Name: "qty", Value: []byte("3")},
			{Name: "paid", Value: []byte("true")},
			{Name: "created_at", Value: []byte(`"2026-10-18T12:34:56.123456Z"`)},
			{Name: "ref", Value: []byte(`"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"`)},
			{Name: "attrs", Value: []byte(`{"color":"red","sizes":[1,2,3],"gift":{"wrap":true}}`)},
			{Name: "note", Value: []byte(`"leave at door"`)},
		},
		Unchanged: []string{"doc"},
	}
	return []struct {
		name string
		ev   change.Event
	}{{"narrow_insert", narrow}, {"wide_update_toast", wide}}
}

func BenchmarkEvent_AppendJSON(b *testing.B) {
	for _, bc := range benchEvents() {
		b.Run(bc.name, func(b *testing.B) {
			buf := bc.ev.AppendJSON(nil)
			b.ReportAllocs()
			b.SetBytes(int64(len(buf)))
			for b.Loop() {
				buf = bc.ev.AppendJSON(buf[:0])
			}
		})
	}
}

func BenchmarkAppendString_Payload(b *testing.B) {
	s := strings.Repeat("x", 168)
	buf := change.AppendString(nil, s)
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	for b.Loop() {
		buf = change.AppendString(buf[:0], s)
	}
}
