// SPDX-License-Identifier: Apache-2.0

// Package event is the public Go form of Wake's CDC event contract: the types of
// the events Wake emits, a decoder that keeps integers exact, and the helpers a
// consumer needs to order and deduplicate them.
//
// The main types are Event (a row change), SchemaChange (a relation change) and
// Message (a forwarded logical message), each with a Decode function that
// validates the payload against the JSON Schemas in api/schema. Column values
// stay raw JSON in a Row, so an int8 above 2^53 is never rounded; read them with
// the typed accessors of Value (Int64, BigInt, Number, Text, ...) and never
// through float64. Seq, ParseSeq and Compare give the total order of a source
// (LSNs are compared numerically); IsKafkaTombstone, MergeUnchanged and
// Event.KeyChanged cover the transport and image rules of the contract.
//
// A consumer typically decodes, skips what it cannot use, and applies the rest:
//
//	e, err := event.Decode(record.Value)
//	switch {
//	case errors.Is(err, event.ErrTombstone):
//		return nil // Kafka tombstone: skip and count
//	case err != nil:
//		return err
//	case !e.Op.Known():
//		return nil // unknown op: ignore and count
//	}
//	id, _ := e.Key.Value("id")
//	text, err := id.Text() // exact decimal for integers
//
// Webhook bodies are arrays that may mix kinds; classify each element with
// KindOf and pass it to Decode, DecodeSchemaChange or DecodeMessage.
//
// The package depends on the standard library only and is safe for concurrent
// use; decoded values are plain data owned by the caller.
package event
