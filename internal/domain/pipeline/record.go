// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"github.com/thnhdatle/wake/internal/domain/change"
)

// RecordKind tells what a Record carries.
type RecordKind uint8

const (
	// RecordKindEvent is a CDC row change event.
	RecordKindEvent RecordKind = iota + 1
	// RecordKindSchemaChange is a schema-change event.
	RecordKindSchemaChange
	// RecordKindMessage is a forwarded logical message.
	RecordKindMessage
)

// Destination suffixes of records that do not go to a table's destination.
const (
	// DestinationSchema receives schema-change events (`<prefix>._schema`).
	DestinationSchema = "_schema"
	// DestinationMessages receives forwarded messages (`<prefix>._messages`).
	DestinationMessages = "_messages"
)

// Record is what the router hands to a sink: one event, schema change or
// forwarded message, already encoded. Payload and Key are shared by every
// sink the record is routed to and must not be modified.
type Record struct {
	Kind RecordKind
	ID   change.ID
	// Op is the event operation; empty for other kinds.
	Op change.Op
	// Source is the table of an event or schema change; zero for messages.
	Source change.Source
	// Destination is the topic or subject suffix: "<schema>.<table>",
	// DestinationSchema or DestinationMessages.
	Destination string
	// Key is the transport key (Kafka record key): the canonical key JSON;
	// the id for events of unkeyed tables (key: null); the canonical JSON of
	// {"schema","table"} for schema changes; nil for `t` events and messages.
	Key []byte
	// Barrier marks records without a partition key (schema changes, `t`
	// events, messages): lanes flush before them and resume after them.
	Barrier bool
	// Payload is the record's JSON encoding.
	Payload []byte
	// SchemaVersion is the table's schema version; zero for messages.
	SchemaVersion uint64
	// TsMs is the record's ts_ms, for the end-to-end latency metric.
	TsMs int64
	// Ack acknowledges this record's delivery to the sink that received it.
	Ack AckHandle
}

// NewEventRecord encodes a row change event. Its partition key is the
// canonical key JSON, or the id when the key is null; a `t` event is a
// barrier.
func NewEventRecord(ev *change.Event) Record {
	buf := ev.AppendJSON(make([]byte, 0, 256))
	n := len(buf)
	barrier := ev.Op == change.OpTruncate
	switch {
	case barrier:
	case ev.Key == nil:
		buf = ev.ID.AppendText(buf)
	default:
		buf = change.AppendCanonicalKey(buf, ev.Key)
	}
	rec := Record{
		Kind:          RecordKindEvent,
		ID:            ev.ID,
		Op:            ev.Op,
		Source:        ev.Source,
		Destination:   ev.Source.QualifiedName(),
		Barrier:       barrier,
		Payload:       buf[:n:n],
		SchemaVersion: ev.SchemaVersion,
		TsMs:          ev.TsMs,
	}
	if len(buf) > n {
		rec.Key = buf[n:]
	}
	return rec
}

// NewSchemaChangeRecord encodes a schema-change event: a barrier keyed by
// the canonical JSON of {"schema","table"} for its Kafka record.
func NewSchemaChangeRecord(sc *change.SchemaChange) Record {
	buf := sc.AppendJSON(make([]byte, 0, 256))
	n := len(buf)
	buf = append(buf, `{"schema":`...)
	buf = change.AppendString(buf, sc.Source.Schema)
	buf = append(buf, `,"table":`...)
	buf = change.AppendString(buf, sc.Source.Table)
	buf = append(buf, '}')
	return Record{
		Kind:          RecordKindSchemaChange,
		ID:            sc.ID,
		Source:        sc.Source,
		Destination:   DestinationSchema,
		Key:           buf[n:],
		Barrier:       true,
		Payload:       buf[:n:n],
		SchemaVersion: sc.SchemaVersion,
		TsMs:          sc.TsMs,
	}
}

// NewMessageRecord encodes a forwarded logical message, a barrier.
func NewMessageRecord(m *change.Message) Record {
	return Record{
		Kind:        RecordKindMessage,
		ID:          m.ID,
		Destination: DestinationMessages,
		Barrier:     true,
		Payload:     m.AppendJSON(nil),
		TsMs:        m.TsMs,
	}
}

// PartitionKey returns the key that selects the record's partition, subject
// or lane, or nil for a barrier.
func (r *Record) PartitionKey() []byte {
	if r.Barrier {
		return nil
	}
	return r.Key
}

// Table returns the qualified table name used for routing, or "" for
// messages.
func (r *Record) Table() string {
	if r.Kind == RecordKindMessage {
		return ""
	}
	return r.Source.QualifiedName()
}
