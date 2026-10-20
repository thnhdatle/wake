// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

var users = change.Source{DB: "app", Schema: "public", Table: "users"}

func TestNewEventRecord(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		ev          change.Event
		wantKey     string
		wantBarrier bool
	}{
		{
			name: "keyed event uses canonical key json",
			ev: change.Event{
				ID: change.StreamID(0x16B3748, 3), Source: users, Op: change.OpCreate, TsMs: 1760600000000,
				TxID: 7812, SchemaVersion: 3,
				Key:   change.Row{{Name: "b", Value: []byte(`"<x>"`)}, {Name: "a", Value: []byte(`42`)}},
				After: change.Row{{Name: "a", Value: []byte(`42`)}},
			},
			wantKey: `{"a":42,"b":"<x>"}`,
		},
		{
			name: "unkeyed event is partitioned by id",
			ev: change.Event{
				ID: change.StreamID(0x10, 7), Source: users, Op: change.OpCreate,
				After: change.Row{{Name: "a", Value: []byte(`1`)}},
			},
			wantKey: "0/10-7",
		},
		{
			name:        "truncate is a barrier without key",
			ev:          change.Event{ID: change.StreamID(0x10, 0), Source: users, Op: change.OpTruncate},
			wantBarrier: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := pipeline.NewEventRecord(&tc.ev)
			assert.Equal(t, pipeline.RecordKindEvent, rec.Kind)
			assert.Equal(t, tc.ev.ID, rec.ID)
			assert.Equal(t, tc.ev.Op, rec.Op)
			assert.Equal(t, users, rec.Source)
			assert.Equal(t, "public.users", rec.Destination)
			assert.Equal(t, "public.users", rec.Table())
			assert.Equal(t, tc.ev.SchemaVersion, rec.SchemaVersion)
			assert.Equal(t, tc.ev.TsMs, rec.TsMs)
			assert.Equal(t, tc.wantBarrier, rec.Barrier)
			assert.Equal(t, string(tc.ev.AppendJSON(nil)), string(rec.Payload))
			assert.True(t, json.Valid(rec.Payload))
			if tc.wantBarrier {
				assert.Nil(t, rec.Key)
				assert.Nil(t, rec.PartitionKey())
			} else {
				assert.Equal(t, tc.wantKey, string(rec.Key))
				assert.Equal(t, tc.wantKey, string(rec.PartitionKey()))
			}
			assert.True(t, rec.Ack.IsZero())
		})
	}
}

func TestNewEventRecord_PayloadNotOverwrittenByAppend(t *testing.T) {
	t.Parallel()
	ev := change.Event{ID: change.StreamID(1, 0), Source: users, Op: change.OpCreate, Key: change.Row{
		{Name: "id", Value: []byte(`1`)},
	}}
	rec := pipeline.NewEventRecord(&ev)
	want := string(rec.Payload)
	_ = append(rec.Payload, "garbage"...)
	assert.Equal(t, want, string(rec.Payload))
	assert.JSONEq(t, `{"id":1}`, string(rec.Key))
}

func TestNewSchemaChangeRecord(t *testing.T) {
	t.Parallel()
	sc := change.SchemaChange{
		ID: change.SchemaID(0x16C0000, 0), Source: users, TsMs: 1760600100000, SchemaVersion: 4,
		ColumnsAfter: []change.ColumnType{{Name: "id", Type: "int8"}},
	}
	rec := pipeline.NewSchemaChangeRecord(&sc)
	assert.Equal(t, pipeline.RecordKindSchemaChange, rec.Kind)
	assert.Equal(t, sc.ID, rec.ID)
	assert.Equal(t, pipeline.DestinationSchema, rec.Destination)
	assert.Equal(t, "public.users", rec.Table())
	assert.True(t, rec.Barrier)
	assert.Nil(t, rec.PartitionKey())
	wantKey := []byte(`{"schema":"public","table":"users"}`)
	assert.True(t, bytes.Equal(wantKey, rec.Key), "exact canonical bytes: %s", rec.Key)
	assert.Equal(t, string(sc.AppendJSON(nil)), string(rec.Payload))
	assert.Equal(t, uint64(4), rec.SchemaVersion)
	assert.Equal(t, sc.TsMs, rec.TsMs)
	assert.Empty(t, rec.Op)
}

func TestNewMessageRecord(t *testing.T) {
	t.Parallel()
	m := change.Message{ID: change.MessageID(0x20, 1), TsMs: 5, Transactional: true, Prefix: "app", Content: []byte("hi")}
	rec := pipeline.NewMessageRecord(&m)
	assert.Equal(t, pipeline.RecordKindMessage, rec.Kind)
	assert.Equal(t, pipeline.DestinationMessages, rec.Destination)
	assert.Empty(t, rec.Table())
	assert.True(t, rec.Barrier)
	assert.Nil(t, rec.Key)
	assert.Nil(t, rec.PartitionKey())
	assert.Equal(t, string(m.AppendJSON(nil)), string(rec.Payload))
	assert.Equal(t, int64(5), rec.TsMs)
}

type recordingAcker struct {
	toks []pipeline.AckToken
	err  error
}

func (a *recordingAcker) Ack(tok pipeline.AckToken) error {
	a.toks = append(a.toks, tok)
	return a.err
}

func TestAckHandle_Ack(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		err     error
		wantErr bool
	}{
		{name: "forwards token"},
		{name: "returns acker error", err: assert.AnError, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := &recordingAcker{err: tc.err}
			tok := pipeline.AckToken{Tx: 3, Group: 2}
			h := pipeline.NewAckHandle(a, tok)
			assert.False(t, h.IsZero())
			assert.Equal(t, tok, h.Token())
			err := h.Ack()
			if tc.wantErr {
				require.ErrorIs(t, err, assert.AnError)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, []pipeline.AckToken{tok}, a.toks)
		})
	}
}

func TestAckHandle_Ack_ZeroIsNoop(t *testing.T) {
	t.Parallel()
	var h pipeline.AckHandle
	assert.True(t, h.IsZero())
	require.NoError(t, h.Ack())
	assert.Equal(t, pipeline.AckToken{}, h.Token())
}
