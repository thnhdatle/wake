// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"github.com/thnhdatle/wake/internal/domain/change"
)

type RecordKind uint8

const (
	RecordKindEvent RecordKind = iota + 1
	RecordKindSchemaChange
	RecordKindMessage
)

const (
	DestinationSchema   = "_schema"
	DestinationMessages = "_messages"
)

// Record is one routed event, schema change or message.
type Record struct {
	Kind          RecordKind
	ID            change.ID
	Op            change.Op
	Source        change.Source
	Destination   string
	Key           []byte
	Barrier       bool
	Payload       []byte
	SchemaVersion uint64
	TsMs          int64
	Ack           AckHandle
}

// NewEventRecord wraps a change event in a Record.
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

func (r *Record) PartitionKey() []byte {
	if r.Barrier {
		return nil
	}
	return r.Key
}

func (r *Record) Table() string {
	if r.Kind == RecordKindMessage {
		return ""
	}
	return r.Source.QualifiedName()
}
