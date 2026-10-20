// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// StateFormatVersion is the state file format written by this version.
const StateFormatVersion = 1

// SnapshotKind tells how a snapshot run reads its rows.
type SnapshotKind string

// Snapshot run kinds.
const (
	// SnapshotKindInitial reads at the slot's exported snapshot.
	SnapshotKindInitial SnapshotKind = "initial"
	// SnapshotKindTable reads through watermarks.
	SnapshotKindTable SnapshotKind = "table"
)

// State is the content of the state file.
type State struct {
	FormatVersion int `json:"format_version"`
	// SlotName and SystemID identify the slot and Postgres system the file
	// belongs to (ownership check).
	SlotName string `json:"slot_name"`
	SystemID string `json:"system_id"`
	// ConsistentPoint is the slot's consistent point; InvalidLSN means slot
	// creation never completed, so nothing was emitted.
	ConsistentPoint lsn.LSN `json:"consistent_point"`
	// ConsistentPointTsMs is the clock_timestamp() read before slot
	// creation: the initial snapshot's ts_ms.
	ConsistentPointTsMs int64 `json:"consistent_point_ts_ms"`
	// RunCounter is the last snapshot run_id assigned.
	RunCounter uint64 `json:"run_counter"`
	// Schemas maps "<schema>.<table>" to the table's schema version.
	Schemas map[string]TableSchema `json:"schemas"`
	// SchemaFloor is the lowest schema version a table not seen before
	// receives; --reinit-state raises it to the current Unix time so that
	// versions never decrease (schema.RegistryState.Floor).
	SchemaFloor uint64        `json:"schema_floor,omitempty"`
	Snapshot    SnapshotState `json:"snapshot"`
}

// TableSchema is the persisted schema of one table.
type TableSchema struct {
	Version uint64 `json:"version"`
	// ReplicaIdentity is d, n, f or i (pg_class.relreplident).
	ReplicaIdentity string         `json:"replica_identity"`
	Columns         []SchemaColumn `json:"columns"`
}

// SchemaColumn is one persisted column.
type SchemaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// TypeOID and TypeMod are the column's type and type modifier; with the
	// name and Key they define the schema version shape that a restarted
	// process compares with the first Relation message.
	TypeOID uint32 `json:"type_oid,omitempty"`
	TypeMod int32  `json:"type_mod,omitempty"`
	// Key marks a key column.
	Key bool `json:"key,omitempty"`
}

// SnapshotState holds unfinished snapshot runs and queued requests.
type SnapshotState struct {
	// Runs are the unfinished runs, oldest first; v0.1 executes one table
	// snapshot at a time.
	Runs []SnapshotRun `json:"runs"`
	// Queue holds tables ("<schema>.<table>") waiting for a table snapshot.
	Queue []string `json:"queue"`
	// TriggerLSN is the commit LSN of the latest wake.snapshot request
	// already queued; requests at or before it are replays after a restart
	// and are ignored.
	TriggerLSN lsn.LSN `json:"trigger_lsn,omitempty"`
}

// SnapshotRun is one initial snapshot or table snapshot.
type SnapshotRun struct {
	ID   uint64       `json:"id"`
	Kind SnapshotKind `json:"kind"`
	// ChunkRows is the run's chunk size, fixed for the run because ids use
	// n = c × chunk_rows + i.
	ChunkRows int `json:"chunk_rows"`
	// NextChunk is the chunk counter c, persisted before a chunk is emitted.
	NextChunk uint64          `json:"next_chunk"`
	Tables    []TableProgress `json:"tables"`
}

// TableProgress is one table's progress in a run.
type TableProgress struct {
	// Table is "<schema>.<table>" (Postgres names).
	Table string `json:"table"`
	// Position is the last acknowledged position (ChunkRequest.After); nil
	// when no chunk is complete yet.
	Position json.RawMessage `json:"position,omitempty"`
	// Rows counts acknowledged rows.
	Rows uint64 `json:"rows"`
	Done bool   `json:"done"`
}

// NewState returns an empty state owned by the slot and system.
func NewState(slot, systemID string) State {
	return State{
		FormatVersion: StateFormatVersion,
		SlotName:      slot,
		SystemID:      systemID,
		Schemas:       map[string]TableSchema{},
	}
}

// CheckOwnership returns ErrStateMismatch unless the state belongs to the
// slot and system.
func (s *State) CheckOwnership(slot, systemID string) error {
	if s.SlotName != slot || s.SystemID != systemID {
		return fmt.Errorf("%w: state has slot %q system %q, source has slot %q system %q",
			ErrStateMismatch, s.SlotName, s.SystemID, slot, systemID)
	}
	return nil
}

// HasConsistentPoint reports whether slot creation completed.
func (s *State) HasConsistentPoint() bool { return s.ConsistentPoint != lsn.InvalidLSN }

// NextRunID increments the run counter and returns the new run_id.
func (s *State) NextRunID() uint64 {
	s.RunCounter++
	return s.RunCounter
}

// Clone returns a deep copy.
func (s *State) Clone() State {
	c := *s
	if s.Schemas != nil {
		c.Schemas = make(map[string]TableSchema, len(s.Schemas))
		for k, v := range maps.All(s.Schemas) {
			v.Columns = slices.Clone(v.Columns)
			c.Schemas[k] = v
		}
	}
	c.Snapshot.Queue = slices.Clone(s.Snapshot.Queue)
	if s.Snapshot.Runs != nil {
		c.Snapshot.Runs = make([]SnapshotRun, len(s.Snapshot.Runs))
		for i, r := range s.Snapshot.Runs {
			r.Tables = slices.Clone(r.Tables)
			for j := range r.Tables {
				r.Tables[j].Position = bytes.Clone(r.Tables[j].Position)
			}
			c.Snapshot.Runs[i] = r
		}
	}
	return c
}
