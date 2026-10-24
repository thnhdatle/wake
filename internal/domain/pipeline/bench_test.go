// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"strings"
	"testing"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func BenchmarkRecord_NewEventRecord(b *testing.B) {
	key := change.Row{{Name: "id", Value: []byte("4242424242")}}
	cases := []struct {
		name string
		ev   change.Event
	}{
		{"narrow_insert", change.Event{
			ID: change.StreamID(0x16B3748, 0), Source: change.Source{DB: "app", Schema: "wak05", Table: "t1"},
			Op: change.OpCreate, TsMs: 1_792_308_604_187, TxID: 900, SchemaVersion: 1,
			Key: change.Row{{Name: "id", Value: []byte("1234567")}},
			After: change.Row{
				{Name: "id", Value: []byte("1234567")},
				{Name: "payload", Value: []byte(`"` + strings.Repeat("x", 168) + `"`)},
			},
		}},
		{"wide_update_toast", change.Event{
			ID: change.StreamID(0x16B3748, 7), Source: change.Source{DB: "app", Schema: "public", Table: "orders"},
			Op: change.OpUpdate, TsMs: 1_792_308_604_187, TxID: 901, SchemaVersion: 3, Key: key, Before: key,
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
		}},
	}
	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				rec := pipeline.NewEventRecord(&bc.ev)
				if len(rec.Key) == 0 {
					b.Fatal("no key")
				}
			}
		})
	}
}
