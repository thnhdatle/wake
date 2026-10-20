// SPDX-License-Identifier: Apache-2.0

package event

import (
	"encoding/json"
	"fmt"
)

// Column is one column of a table definition in a schema-change event.
type Column struct {
	// Name is the column name.
	Name string `json:"name"`
	// Type is the Postgres type name.
	Type string `json:"type"`
}

// schemaChangeType is the value of the "type" member of schema-change events.
const schemaChangeType = "schema_change"

// SchemaChange is a CDC schema-change event, emitted before the first data event
// that uses a new relation definition. Its JSON form carries "type":
// "schema_change".
type SchemaChange struct {
	// ID has the form "<lsn>-s<k>".
	ID string `json:"id"`
	// Source is the table that changed.
	Source Source `json:"source"`
	// LSN is the commit LSN of the data event that first uses the new relation.
	LSN string `json:"lsn"`
	// TsMs is the commit time in Unix milliseconds.
	TsMs int64 `json:"ts_ms"`
	// SchemaVersion is the new per-table schema counter.
	SchemaVersion int64 `json:"schema_version"`
	// ColumnsBefore is nil when the table was not known before (JSON null).
	ColumnsBefore []Column `json:"columns_before"`
	// ColumnsAfter is the new column set.
	ColumnsAfter []Column `json:"columns_after"`
}

// schemaChangeAlias drops the methods of SchemaChange to avoid recursion.
type schemaChangeAlias SchemaChange

// schemaChangeWire is the JSON form: the alias plus the constant type member.
type schemaChangeWire struct {
	Type string `json:"type"`
	schemaChangeAlias
}

var (
	schemaChangeRequired = []string{
		fieldType, fieldID, fieldSource, fieldLSN, fieldTsMs, fieldSchemaVersion, fieldColsBefore, fieldColsAfter,
	}
	schemaChangeNonNull = []string{
		fieldType, fieldID, fieldSource, fieldLSN, fieldTsMs, fieldSchemaVersion, fieldColsAfter,
	}
)

// DecodeSchemaChange decodes and validates a CDC schema-change event. Errors
// follow the rules of Decode.
func DecodeSchemaChange(data []byte) (*SchemaChange, error) {
	if err := probe(data, schemaChangeRequired, schemaChangeNonNull); err != nil {
		return nil, err
	}
	var w schemaChangeWire
	if err := unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.Type != schemaChangeType {
		return nil, fmt.Errorf("%w: type %q is not %q", ErrInvalid, w.Type, schemaChangeType)
	}
	s := SchemaChange(w.schemaChangeAlias)
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// validate checks the id, the shared fields and the columns.
func (s *SchemaChange) validate() error {
	seq, err := ParseSeq(s.ID)
	if err != nil {
		return err
	}
	if !seq.Schema {
		return fmt.Errorf("%w: schema-change id %q needs the s<k> suffix", ErrInvalid, s.ID)
	}
	if err = checkCommon(s.LSN, s.TsMs, s.SchemaVersion, s.Source); err != nil {
		return err
	}
	for _, cols := range [][]Column{s.ColumnsBefore, s.ColumnsAfter} {
		for _, c := range cols {
			if c.Name == "" || c.Type == "" {
				return fmt.Errorf("%w: column needs name and type", ErrInvalid)
			}
		}
	}
	return nil
}

// Seq returns the position of the event in the order of its source; Schema is
// true so it sorts before the data event with the same LSN and K.
func (s *SchemaChange) Seq() (Seq, error) {
	return ParseSeq(s.ID)
}

// MarshalJSON encodes the event with its "type": "schema_change" member.
func (s SchemaChange) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(schemaChangeWire{Type: schemaChangeType, schemaChangeAlias: schemaChangeAlias(s)})
	if err != nil {
		return nil, fmt.Errorf("event: marshal schema change: %w", err)
	}
	return b, nil
}
