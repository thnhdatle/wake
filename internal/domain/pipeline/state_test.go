// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func sampleState() pipeline.State {
	s := pipeline.NewState("wake_app", "7300000000000000001")
	s.ConsistentPoint = 0x16B3748
	s.ConsistentPointTsMs = 1760600000000
	s.Schemas["public.users"] = pipeline.TableSchema{
		Version: 3, ReplicaIdentity: "d",
		Columns: []pipeline.SchemaColumn{
			{Name: "id", Type: "int8", TypeOID: 20, TypeMod: -1, Key: true},
			{Name: "email", Type: "text", TypeOID: 25, TypeMod: -1},
		},
	}
	s.Snapshot = pipeline.SnapshotState{
		Runs: []pipeline.SnapshotRun{{
			ID: 1, Kind: pipeline.SnapshotKindInitial, ChunkRows: 10000, NextChunk: 4,
			Tables: []pipeline.TableProgress{{Table: "public.users", Position: json.RawMessage(`[42]`), Rows: 30000}},
		}},
		Queue:      []string{"public.orders"},
		TriggerLSN: 0x16B0000,
	}
	s.RunCounter = 1
	s.SchemaFloor = 1760000000
	return s
}

func TestNewState(t *testing.T) {
	t.Parallel()
	s := pipeline.NewState("wake_app", "1")
	assert.Equal(t, pipeline.StateFormatVersion, s.FormatVersion)
	assert.Equal(t, "wake_app", s.SlotName)
	assert.Equal(t, "1", s.SystemID)
	assert.NotNil(t, s.Schemas)
	assert.False(t, s.HasConsistentPoint())
}

func TestState_CheckOwnership(t *testing.T) {
	t.Parallel()
	s := pipeline.NewState("wake_app", "1")
	cases := []struct {
		name, slot, system string
		wantErr            bool
	}{
		{name: "same", slot: "wake_app", system: "1"},
		{name: "other slot", slot: "wake_other", system: "1", wantErr: true},
		{name: "other system", slot: "wake_app", system: "2", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := s.CheckOwnership(tc.slot, tc.system)
			if tc.wantErr {
				require.ErrorIs(t, err, pipeline.ErrStateMismatch)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestState_NextRunID(t *testing.T) {
	t.Parallel()
	s := pipeline.NewState("wake_app", "1")
	assert.Equal(t, uint64(1), s.NextRunID())
	assert.Equal(t, uint64(2), s.NextRunID())
	assert.Equal(t, uint64(2), s.RunCounter)
}

func TestState_Clone_Deep(t *testing.T) {
	t.Parallel()
	s := sampleState()
	c := s.Clone()
	require.Empty(t, cmp.Diff(s, c))
	c.Schemas["public.users"].Columns[0].Name = "changed"
	c.Schemas["public.x"] = pipeline.TableSchema{}
	c.Snapshot.Runs[0].Tables[0].Position[1] = '9'
	c.Snapshot.Runs[0].Tables[0].Rows = 1
	c.Snapshot.Queue[0] = "changed"
	assert.Equal(t, "id", s.Schemas["public.users"].Columns[0].Name)
	assert.Len(t, s.Schemas, 1)
	assert.JSONEq(t, `[42]`, string(s.Snapshot.Runs[0].Tables[0].Position))
	assert.Equal(t, uint64(30000), s.Snapshot.Runs[0].Tables[0].Rows)
	assert.Equal(t, "public.orders", s.Snapshot.Queue[0])
}

func TestState_Clone_Empty(t *testing.T) {
	t.Parallel()
	var s pipeline.State
	assert.Empty(t, cmp.Diff(s, s.Clone()))
}

func TestState_JSONRoundTrip(t *testing.T) {
	t.Parallel()
	s := sampleState()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"consistent_point":"0/16B3748"`)
	assert.Contains(t, string(b), `"position":[42]`)
	assert.Contains(t, string(b), `"trigger_lsn":"0/16B0000"`)
	assert.Contains(t, string(b), `"type_oid":20,"type_mod":-1`)
	var got pipeline.State
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Empty(t, cmp.Diff(s, got))
	assert.True(t, got.HasConsistentPoint())
	assert.Equal(t, lsn.LSN(0x16B3748), got.ConsistentPoint)
}
