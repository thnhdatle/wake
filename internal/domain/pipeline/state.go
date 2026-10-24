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

const StateFormatVersion = 1

type SnapshotKind string

const (
	SnapshotKindInitial SnapshotKind = "initial"
	SnapshotKindTable   SnapshotKind = "table"
)

// State is the saved progress of a replication slot.
type State struct {
	FormatVersion       int                    `json:"format_version"`
	SlotName            string                 `json:"slot_name"`
	SystemID            string                 `json:"system_id"`
	ConsistentPoint     lsn.LSN                `json:"consistent_point"`
	ConsistentPointTsMs int64                  `json:"consistent_point_ts_ms"`
	RunCounter          uint64                 `json:"run_counter"`
	Schemas             map[string]TableSchema `json:"schemas"`
	SchemaFloor         uint64                 `json:"schema_floor,omitempty"`
	Snapshot            SnapshotState          `json:"snapshot"`
}

type TableSchema struct {
	Version         uint64         `json:"version"`
	ReplicaIdentity string         `json:"replica_identity"`
	Columns         []SchemaColumn `json:"columns"`
}

type SchemaColumn struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	TypeOID uint32 `json:"type_oid,omitempty"`
	TypeMod int32  `json:"type_mod,omitempty"`
	Key     bool   `json:"key,omitempty"`
}

type SnapshotState struct {
	Runs       []SnapshotRun `json:"runs"`
	Queue      []string      `json:"queue"`
	TriggerLSN lsn.LSN       `json:"trigger_lsn,omitempty"`
}

type SnapshotRun struct {
	ID        uint64          `json:"id"`
	Kind      SnapshotKind    `json:"kind"`
	ChunkRows int             `json:"chunk_rows"`
	NextChunk uint64          `json:"next_chunk"`
	Tables    []TableProgress `json:"tables"`
}

type TableProgress struct {
	Table    string          `json:"table"`
	Position json.RawMessage `json:"position,omitempty"`
	Rows     uint64          `json:"rows"`
	Done     bool            `json:"done"`
}

// NewState returns an empty state for the slot and system.
func NewState(slot, systemID string) State {
	return State{
		FormatVersion: StateFormatVersion,
		SlotName:      slot,
		SystemID:      systemID,
		Schemas:       map[string]TableSchema{},
	}
}

func (s *State) CheckOwnership(slot, systemID string) error {
	if s.SlotName != slot || s.SystemID != systemID {
		return fmt.Errorf("%w: state has slot %q system %q, source has slot %q system %q",
			ErrStateMismatch, s.SlotName, s.SystemID, slot, systemID)
	}
	return nil
}

func (s *State) HasConsistentPoint() bool { return s.ConsistentPoint != lsn.InvalidLSN }

func (s *State) NextRunID() uint64 {
	s.RunCounter++
	return s.RunCounter
}

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
