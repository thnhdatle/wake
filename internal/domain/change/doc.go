// SPDX-License-Identifier: Apache-2.0

// Package change holds Wake's change model in CDC event terms: Event (one
// row change), SchemaChange, Message (a forwarded logical message), their
// identifiers (ID, with the stream, snapshot, schema and message forms), the
// total order Seq, and the canonical JSON encoding of keys.
//
// Column values are carried as compact JSON produced by the type mapping, so
// encoding an event never re-interprets values (int8 above 2^53 stays exact).
//
//	ev := change.Event{
//		ID:     change.StreamID(commitLSN, 0),
//		Source: change.Source{DB: "app", Schema: "public", Table: "users"},
//		Op:     change.OpCreate,
//		Key:    change.Row{{Name: "id", Value: []byte("42")}},
//		After:  change.Row{{Name: "id", Value: []byte("42")}},
//	}
//	body := ev.AppendJSON(nil)
//	kafkaKey := change.AppendCanonicalKey(nil, ev.Key)
package change
