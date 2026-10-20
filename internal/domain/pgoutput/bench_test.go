// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

// Relation OIDs of the benchmark shapes.
const (
	oidBenchNarrow uint32 = 30001
	oidBenchWide   uint32 = 30002
)

// benchCase is one per-event hot-path input: a row message in wire form.
type benchCase struct {
	name string
	wire []byte
}

// benchDecoder returns a decoder that knows the benchmark relations: the
// throughput acceptance test row (bigint key, ~170-byte text payload) and a wide row of ten
// columns of mixed types (numeric, timestamptz, jsonb, a TOASTable text).
func benchDecoder(tb testing.TB) *pgoutput.Decoder {
	tb.Helper()
	dec := pgoutput.NewDecoder()
	for _, rel := range []*pgoutput.Relation{
		relationMsg(oidBenchNarrow, "t1", schema.IdentityDefault,
			col("id", schema.OIDInt8, true), col("payload", schema.OIDText, false)),
		relationMsg(oidBenchWide, "orders", schema.IdentityDefault,
			col("id", schema.OIDInt8, true), col("customer", schema.OIDText, false),
			col("amount", schema.OIDNumeric, false), col("qty", schema.OIDInt4, false),
			col("paid", schema.OIDBool, false), col("created_at", schema.OIDTimestampTZ, false),
			col("ref", schema.OIDUUID, false), col("attrs", schema.OIDJSONB, false),
			col("note", schema.OIDVarchar, false), col("doc", schema.OIDText, false)),
	} {
		_, err := dec.Decode(encode(rel))
		require.NoError(tb, err)
	}
	return dec
}

// wideValues returns the wide row; doc is a TOAST marker when unchanged.
func wideValues(unchanged bool) pgoutput.Tuple {
	doc := txt(strings.Repeat("lorem ipsum ", 25))
	if unchanged {
		doc = toast()
	}
	return pgoutput.Tuple{
		txt("4242424242"), txt("Ada Lovelace"), txt("12345.6789"), txt("3"), txt("t"),
		txt("2026-10-18 12:34:56.123456+00"), txt("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
		txt(`{"color": "red", "sizes": [1, 2, 3], "gift": {"wrap": true}}`), txt("leave at door"), doc,
	}
}

func benchCases() []benchCase {
	narrow := pgoutput.Tuple{txt("1234567"), txt(strings.Repeat("x", 168))}
	return []benchCase{
		{"narrow_insert", encode(&pgoutput.Insert{RelationOID: oidBenchNarrow, New: narrow})},
		{"wide_insert", encode(&pgoutput.Insert{RelationOID: oidBenchWide, New: wideValues(false)})},
		{"wide_update_toast", encode(&pgoutput.Update{RelationOID: oidBenchWide, New: wideValues(true)})},
	}
}

// BenchmarkDecoder_Decode parses one row message from wire bytes.
func BenchmarkDecoder_Decode(b *testing.B) {
	for _, bc := range benchCases() {
		b.Run(bc.name, func(b *testing.B) {
			dec := benchDecoder(b)
			b.ReportAllocs()
			b.SetBytes(int64(len(bc.wire)))
			for b.Loop() {
				if _, err := dec.Decode(bc.wire); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecoder_RowImages converts one parsed row message to CDC event row
// images (type mapping included).
func BenchmarkDecoder_RowImages(b *testing.B) {
	for _, bc := range benchCases() {
		b.Run(bc.name, func(b *testing.B) {
			dec := benchDecoder(b)
			m, err := dec.Decode(bc.wire)
			require.NoError(b, err)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := dec.RowImages(m); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
