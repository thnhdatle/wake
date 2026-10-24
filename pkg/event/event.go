// SPDX-License-Identifier: Apache-2.0

package event

import (
	"bytes"
	"fmt"
	"slices"
)

// Event is a CDC data event: one row change of one table.
type Event struct {
	// ID is the unique idempotency key of the event.
	ID     string `json:"id"`
	Source Source `json:"source"`
	Op     Op     `json:"op"`
	// TsMs is the commit time in Unix milliseconds.
	TsMs int64  `json:"ts_ms"`
	LSN  string `json:"lsn"`
	// TxID is the Postgres transaction id, nil for snapshot reads.
	TxID *uint64 `json:"tx_id,omitempty"`
	// Key holds the primary-key columns, nil for truncates and keyless tables.
	Key    Row `json:"key"`
	Before Row `json:"before"`
	After  Row `json:"after"`
	// UnchangedColumns lists columns omitted from After as unchanged TOAST values.
	UnchangedColumns []string `json:"unchanged_columns,omitempty"`
	SchemaVersion    int64    `json:"schema_version"`
}

var (
	eventRequired = []string{
		fieldID, fieldSource, fieldOp, fieldTsMs, fieldLSN, fieldKey, fieldBefore, fieldAfter, fieldSchemaVersion,
	}
	eventNonNull = []string{fieldID, fieldSource, fieldOp, fieldTsMs, fieldLSN, fieldSchemaVersion}
)

// Decode decodes and validates a CDC data event.
func Decode(data []byte) (*Event, error) {
	if err := probe(data, eventRequired, eventNonNull); err != nil {
		return nil, err
	}
	var e Event
	if err := unmarshal(data, &e); err != nil {
		return nil, err
	}
	if err := e.validate(); err != nil {
		return nil, err
	}
	return &e, nil
}

func (e *Event) validate() error {
	seq, err := ParseSeq(e.ID)
	if err != nil {
		return err
	}
	if seq.Schema {
		return fmt.Errorf("%w: data event id %q has a schema-change suffix", ErrInvalid, e.ID)
	}
	if err = checkCommon(e.LSN, e.TsMs, e.SchemaVersion, e.Source); err != nil {
		return err
	}
	switch e.Op {
	case OpCreate, OpRead:
		if e.Before != nil || e.After == nil {
			return fmt.Errorf("%w: op %q needs before null and after object", ErrInvalid, e.Op)
		}
	case OpUpdate:
		if e.After == nil {
			return fmt.Errorf("%w: op %q needs after object", ErrInvalid, e.Op)
		}
	case OpDelete:
		if e.After != nil {
			return fmt.Errorf("%w: op %q needs after null", ErrInvalid, e.Op)
		}
	case OpTruncate:
	}
	return nil
}

// Seq returns the position of the event in the order of its source.
func (e *Event) Seq() (Seq, error) {
	return ParseSeq(e.ID)
}

// KeyChanged reports whether an update moved the row to another key.
func (e *Event) KeyChanged() bool {
	if e.Op != OpUpdate {
		return false
	}
	for col, now := range e.Key {
		if was, ok := e.Before[col]; ok && !bytes.Equal(bytes.TrimSpace(was), bytes.TrimSpace(now)) {
			return true
		}
	}
	return false
}

// MergeUnchanged fills the columns listed in UnchangedColumns from prev, the previous image of the
// row.
func MergeUnchanged(prev Row, e *Event) {
	if e == nil || e.After == nil || len(e.UnchangedColumns) == 0 {
		return
	}
	var left []string
	for _, col := range e.UnchangedColumns {
		if e.After.Has(col) {
			continue
		}
		v, ok := prev[col]
		if !ok {
			left = append(left, col)
			continue
		}
		e.After[col] = slices.Clone(v)
	}
	e.UnchangedColumns = left
}

// IsKafkaTombstone reports whether a Kafka record value is a tombstone.
func IsKafkaTombstone(value []byte) bool {
	return len(value) == 0
}
