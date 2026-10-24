// SPDX-License-Identifier: Apache-2.0

package change

import (
	"regexp"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type Op string

const (
	OpCreate   Op = "c"
	OpUpdate   Op = "u"
	OpDelete   Op = "d"
	OpRead     Op = "r"
	OpTruncate Op = "t"
)

type Source struct {
	DB     string
	Schema string
	Table  string
}

func (s Source) QualifiedName() string { return s.Schema + "." + s.Table }

type Column struct {
	Name  string
	Value []byte
}

type Row []Column

func (r Row) Lookup(name string) ([]byte, bool) {
	for i := range r {
		if r[i].Name == name {
			return r[i].Value, true
		}
	}
	return nil, false
}

// Event is one row change.
type Event struct {
	ID            ID
	Source        Source
	Op            Op
	TsMs          int64
	TxID          uint32
	Key           Row
	Before        Row
	After         Row
	Unchanged     []string
	SchemaVersion uint64
}

func (e *Event) LSN() lsn.LSN { return e.ID.LSN }

func (e *Event) Seq() Seq { return e.ID.Seq() }

type ColumnType struct {
	Name string
	Type string
}

// SchemaChange is a change to a table's columns.
type SchemaChange struct {
	ID            ID
	Source        Source
	TsMs          int64
	SchemaVersion uint64
	ColumnsBefore []ColumnType
	ColumnsAfter  []ColumnType
}

type Message struct {
	ID            ID
	TsMs          int64
	Transactional bool
	Prefix        string
	Content       []byte
}

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func ValidIdentifier(s string) bool { return identifierRE.MatchString(s) }
