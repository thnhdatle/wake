// SPDX-License-Identifier: Apache-2.0

package main

import (
	"time"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// statusDoc is the GET /v1/status document (api/openapi.yaml Status) and the
// output of `wake status`.
type statusDoc struct {
	// Origin is "admin" when served by the running instance and
	// "state_file" for the `wake status` fallback.
	Origin string  `json:"origin"`
	Source string  `json:"source"`
	Slot   slotDoc `json:"slot"`
	// ConfirmedLSN is the ack tracker's confirmed LSN (admin) or the slot's
	// confirmed_flush_lsn (fallback).
	ConfirmedLSN  string          `json:"confirmed_lsn"`
	StatusSentLSN string          `json:"status_sent_lsn,omitempty"`
	Replication   *replicationDoc `json:"replication,omitempty"`
	Snapshot      snapshotDoc     `json:"snapshot"`
	Sinks         []sinkDoc       `json:"sinks,omitempty"`
	// Schemas maps "<schema>.<table>" to the persisted schema version.
	Schemas map[string]uint64 `json:"schemas"`
}

type slotDoc struct {
	Name              string `json:"name"`
	Exists            bool   `json:"exists"`
	ActivePID         int32  `json:"active_pid,omitempty"`
	WALStatus         string `json:"wal_status,omitempty"`
	ConfirmedFlushLSN string `json:"confirmed_flush_lsn,omitempty"`
	LagBytes          int64  `json:"lag_bytes"`
	Error             string `json:"error,omitempty"`
}

type replicationDoc struct {
	Streaming    bool       `json:"streaming"`
	LastActivity *time.Time `json:"last_activity,omitempty"`
}

type snapshotDoc struct {
	ConsistentPoint string   `json:"consistent_point,omitempty"`
	Runs            []runDoc `json:"runs"`
	Queue           []string `json:"queue"`
}

// runDoc is a snapshot run without chunk positions (they hold key values).
type runDoc struct {
	ID        uint64        `json:"id"`
	Kind      string        `json:"kind"`
	ChunkRows int           `json:"chunk_rows"`
	NextChunk uint64        `json:"next_chunk"`
	Tables    []progressDoc `json:"tables"`
}

type progressDoc struct {
	Table string `json:"table"`
	Rows  uint64 `json:"rows"`
	Done  bool   `json:"done"`
}

type sinkDoc struct {
	Name   string     `json:"name"`
	Type   string     `json:"type"`
	State  string     `json:"state"`
	Code   string     `json:"code,omitempty"`
	Reason string     `json:"reason,omitempty"`
	Since  *time.Time `json:"since,omitempty"`
}

// newStatusDoc builds the parts of the status document that come from the
// slot row and the state file.
func newStatusDoc(cfg *config.Config, slot pipeline.SlotStatus, slotErr error, st pipeline.State) *statusDoc {
	doc := &statusDoc{
		Origin:  "admin",
		Source:  cfg.Source.Name,
		Slot:    slotDoc{Name: slotName(cfg)},
		Schemas: make(map[string]uint64, len(st.Schemas)),
		Snapshot: snapshotDoc{
			Runs:  make([]runDoc, 0, len(st.Snapshot.Runs)),
			Queue: append([]string{}, st.Snapshot.Queue...),
		},
	}
	if slotErr != nil {
		doc.Slot.Error = slotErr.Error()
	} else {
		doc.Slot.Exists = slot.Exists
		doc.Slot.ActivePID = slot.ActivePID
		doc.Slot.WALStatus = slot.WALStatus
		doc.Slot.LagBytes = slot.LagBytes
		if slot.Exists {
			doc.Slot.ConfirmedFlushLSN = slot.ConfirmedFlush.String()
		}
	}
	if st.HasConsistentPoint() {
		doc.Snapshot.ConsistentPoint = st.ConsistentPoint.String()
	}
	for _, r := range st.Snapshot.Runs {
		rd := runDoc{
			ID: r.ID, Kind: string(r.Kind), ChunkRows: r.ChunkRows, NextChunk: r.NextChunk,
			Tables: make([]progressDoc, len(r.Tables)),
		}
		for i, t := range r.Tables {
			rd.Tables[i] = progressDoc{Table: t.Table, Rows: t.Rows, Done: t.Done}
		}
		doc.Snapshot.Runs = append(doc.Snapshot.Runs, rd)
	}
	for table, ts := range st.Schemas {
		doc.Schemas[table] = ts.Version
	}
	return doc
}
