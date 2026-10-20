// SPDX-License-Identifier: Apache-2.0

package event

import (
	"bytes"
	"fmt"
	"slices"
)

// Event is a CDC data event: one row change of one table.
//
// Key, Before and After are nil when the JSON value is null; an empty object
// decodes to a non-nil empty Row.
type Event struct {
	// ID is the unique idempotency key of the event; see ParseSeq.
	ID string `json:"id"`
	// Source is the table the change belongs to.
	Source Source `json:"source"`
	// Op is the operation code; check Op.Known before relying on it.
	Op Op `json:"op"`
	// TsMs is the commit time in Unix milliseconds.
	TsMs int64 `json:"ts_ms"`
	// LSN is the commit LSN in "X/X" form.
	LSN string `json:"lsn"`
	// TxID is the Postgres transaction id; nil for snapshot reads.
	TxID *uint64 `json:"tx_id,omitempty"`
	// Key holds the primary-key (or replica-identity) columns; nil for
	// truncates and keyless tables.
	Key Row `json:"key"`
	// Before is the old row image of updates and deletes.
	Before Row `json:"before"`
	// After is the new row image of creates, updates and snapshot reads.
	After Row `json:"after"`
	// UnchangedColumns lists columns omitted from After because Postgres sent an
	// unchanged-TOAST marker; see MergeUnchanged.
	UnchangedColumns []string `json:"unchanged_columns,omitempty"`
	// SchemaVersion is the per-table schema counter.
	SchemaVersion int64 `json:"schema_version"`
}

var (
	eventRequired = []string{
		fieldID, fieldSource, fieldOp, fieldTsMs, fieldLSN, fieldKey, fieldBefore, fieldAfter, fieldSchemaVersion,
	}
	eventNonNull = []string{fieldID, fieldSource, fieldOp, fieldTsMs, fieldLSN, fieldSchemaVersion}
)

// Decode decodes and validates a CDC data event. Integers inside Key, Before and
// After stay exact because the values are kept as raw JSON.
//
// A nil or zero-length input (a Kafka tombstone value) yields ErrTombstone; any
// other violation of the contract yields an error wrapping ErrInvalid. An
// unknown Op is not an error: callers count and ignore it (see Op.Known).
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

// validate checks the id, the shared fields and the op-specific image rules.
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

// KeyChanged reports whether an update moved the row to another key: true only
// for op u when a column of Key has a different value in Before. Consumers then
// invalidate or delete the old key before applying the new one. Values are
// compared as raw JSON. Columns missing from Before are not compared.
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

// MergeUnchanged fills the columns that e lists in UnchangedColumns from prev,
// the previous image of the same row. A column is resolved when prev has it
// (its value is copied into e.After) or when e.After already has it. Resolved
// columns are removed from UnchangedColumns; the others stay listed, and the
// field becomes nil once every column is resolved. It does nothing when e.After
// is nil.
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

// IsKafkaTombstone reports whether a Kafka record value is a tombstone: nil or
// zero-length. Wake writes one after each delete on compacted topics.
func IsKafkaTombstone(value []byte) bool {
	return len(value) == 0
}
