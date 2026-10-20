// SPDX-License-Identifier: Apache-2.0

package change

import (
	"regexp"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// Op is the operation of a CDC event.
type Op string

// CDC event operations.
const (
	OpCreate   Op = "c"
	OpUpdate   Op = "u"
	OpDelete   Op = "d"
	OpRead     Op = "r"
	OpTruncate Op = "t"
)

// Source names the table an event belongs to. Schema and Table are the
// (possibly mapped) CDC event identifiers.
type Source struct {
	DB     string
	Schema string
	Table  string
}

// QualifiedName returns "<schema>.<table>".
func (s Source) QualifiedName() string { return s.Schema + "." + s.Table }

// Column is one column value of a row image. Value is compact JSON produced
// by the type mapping and is never re-encoded.
type Column struct {
	Name  string
	Value []byte
}

// Row is a row image in table column order. A nil Row encodes as JSON null;
// an empty non-nil Row encodes as {}.
type Row []Column

// Lookup returns the value of the named column.
func (r Row) Lookup(name string) ([]byte, bool) {
	for i := range r {
		if r[i].Name == name {
			return r[i].Value, true
		}
	}
	return nil, false
}

// Event is one row change in CDC event form.
type Event struct {
	ID     ID
	Source Source
	Op     Op
	TsMs   int64
	// TxID is the Postgres xid of the commit; zero (InvalidTransactionId,
	// never a committed xid) omits tx_id, as for snapshot reads.
	TxID          uint32
	Key           Row
	Before        Row
	After         Row
	Unchanged     []string
	SchemaVersion uint64
}

// LSN returns the event's lsn, which is always the LSN of its id.
func (e *Event) LSN() lsn.LSN { return e.ID.LSN }

// Seq returns the event's sequence.
func (e *Event) Seq() Seq { return e.ID.Seq() }

// ColumnType names a column and its Postgres type in schema-change events.
type ColumnType struct {
	Name string
	Type string
}

// SchemaChange is a schema-change event (api/schema/schema-change.v1.json).
// A nil ColumnsBefore encodes as null.
type SchemaChange struct {
	ID            ID
	Source        Source
	TsMs          int64
	SchemaVersion uint64
	ColumnsBefore []ColumnType
	ColumnsAfter  []ColumnType
}

// Message is a forwarded logical message (api/schema/logical-message.v1.json).
type Message struct {
	ID            ID
	TsMs          int64
	Transactional bool
	Prefix        string
	Content       []byte
}

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ValidIdentifier reports whether s may be used as a CDC event schema, table or
// prefix name.
func ValidIdentifier(s string) bool { return identifierRE.MatchString(s) }
