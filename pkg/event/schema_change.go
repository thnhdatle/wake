// SPDX-License-Identifier: Apache-2.0

package event

import (
	"encoding/json"
	"fmt"
)

// Column is one column of a table definition in a schema-change event.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

const schemaChangeType = "schema_change"

// SchemaChange is a CDC schema-change event.
type SchemaChange struct {
	// ID has the form "<lsn>-s<k>".
	ID            string `json:"id"`
	Source        Source `json:"source"`
	LSN           string `json:"lsn"`
	TsMs          int64  `json:"ts_ms"`
	SchemaVersion int64  `json:"schema_version"`
	// ColumnsBefore is nil when the table was not known before.
	ColumnsBefore []Column `json:"columns_before"`
	ColumnsAfter  []Column `json:"columns_after"`
}

type schemaChangeAlias SchemaChange

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

// DecodeSchemaChange decodes and validates a CDC schema-change event.
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

// Seq returns the position of the event in the order of its source.
func (s *SchemaChange) Seq() (Seq, error) {
	return ParseSeq(s.ID)
}

// MarshalJSON encodes the event with its type member.
func (s SchemaChange) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(schemaChangeWire{Type: schemaChangeType, schemaChangeAlias: schemaChangeAlias(s)})
	if err != nil {
		return nil, fmt.Errorf("event: marshal schema change: %w", err)
	}
	return b, nil
}
