// SPDX-License-Identifier: Apache-2.0

// Package pgoutput decodes the Postgres pgoutput logical replication
// protocol, version 1, as requested by Wake with
// proto_version '1', publication_names '…' and messages 'true' and without
// streaming of in-progress transactions or binary values. It is pure: it
// works on message bytes and never performs I/O.
//
// Parse decodes one message into *Begin, *Commit, *Origin, *Relation,
// *TypeMessage, *Insert, *Update, *Delete, *Truncate or *LogicalMessage.
// Stream messages ('S', 'E', 'c', 'A') and protocol version 3 two-phase
// messages are rejected with *UnsupportedMessageError (stream types match
// ErrStreaming). Every length is checked against the message, so malformed
// input yields ErrMalformed instead of a panic or a large allocation. Tuple
// data and logical message content are sub-slices of the input buffer:
// they stay valid only while the caller keeps that buffer unchanged.
//
// Decoder adds the relation cache keyed by relation OID (and a cache of Type
// messages used to name user-defined column types) and converts row
// messages to CDC event row images with RowImages: key, before, after and
// unchanged_columns, values mapped by schema.AppendJSON. Under REPLICA
// IDENTITY FULL the Relation message flags every column as key, so the
// caller sets the table's primary key from the catalog with SetKeyColumns;
// without it the key is every column (FULL table without primary key).
//
// Begin and Commit timestamps (microseconds since 2000-01-01 UTC on the
// wire) are converted to Unix milliseconds (TsMs).
//
//	dec := pgoutput.NewDecoder()
//	msg, err := dec.Decode(xlogData)
//	if err != nil { ... }
//	switch m := msg.(type) {
//	case *pgoutput.Relation:
//		obs := registry.Observe(&m.Relation)
//	case *pgoutput.Insert, *pgoutput.Update, *pgoutput.Delete:
//		img, err := dec.RowImages(m)
//		...
//	case *pgoutput.LogicalMessage:
//		if m.IsInternal() { ... }
//	}
package pgoutput
