// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
)

func drawRecord(t *rapid.T) pipeline.Record {
	rec := pipeline.Record{
		Kind: pipeline.RecordKind(rapid.Uint8Range(1, 3).Draw(t, "kind")),
		Op:   change.Op(rapid.SampledFrom([]string{"", "c", "u", "d", "r", "t"}).Draw(t, "op")),
		ID: change.ID{
			LSN:  lsn.LSN(rapid.Uint64().Draw(t, "lsn")),
			Kind: change.IDKind(rapid.Uint8Range(0, 3).Draw(t, "idkind")),
			Run:  rapid.Uint64().Draw(t, "run"),
			N:    rapid.Uint64().Draw(t, "n"),
		},
		Source: change.Source{
			DB: rapid.String().Draw(t, "db"), Schema: rapid.String().Draw(t, "schema"),
			Table: rapid.String().Draw(t, "table"),
		},
		Destination:   rapid.String().Draw(t, "dest"),
		Barrier:       rapid.Bool().Draw(t, "barrier"),
		Payload:       rapid.SliceOf(rapid.Byte()).Draw(t, "payload"),
		SchemaVersion: rapid.Uint64().Draw(t, "version"),
		TsMs:          rapid.Int64().Draw(t, "ts"),
	}
	if rapid.Bool().Draw(t, "hasKey") {
		rec.Key = rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		if rec.Key == nil {
			rec.Key = []byte{}
		}
	}
	if len(rec.Payload) == 0 {
		rec.Payload = []byte{}
	}
	return rec
}

func TestSpillCodec_RoundTrip_Property(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		rec := drawRecord(t)
		data := appendSpillRecord(nil, &rec)
		got, err := decodeSpillRecord(data)
		require.NoError(t, err)
		assert.Equal(t, rec, got)
		cut := rapid.IntRange(0, len(data)-1).Draw(t, "cut")
		_, err = decodeSpillRecord(data[:cut])
		require.ErrorIs(t, err, ErrSpillCorrupt)
	})
}

func TestSpillCodec_Decode_Corrupt(t *testing.T) {
	t.Parallel()
	rec := pipeline.Record{Kind: pipeline.RecordKindEvent, Payload: []byte("{}")}
	good := appendSpillRecord(nil, &rec)
	cases := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "format", data: append([]byte{9}, good[1:]...)},
		{name: "trailing", data: append(append([]byte{}, good...), 0)},
		{name: "bad varint", data: append(append([]byte{}, good[:2]...), bytes.Repeat([]byte{0xFF}, 11)...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeSpillRecord(tc.data)
			require.ErrorIs(t, err, ErrSpillCorrupt)
		})
	}
}

func TestSchemas_Version(t *testing.T) {
	t.Parallel()
	s, err := NewSchemas(schema.RegistryState{Floor: 50, Tables: []schema.TableState{{Table: "public.a", Version: 0}}})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), s.Version("public.a"), "seeded version 0 reads as 1")
	assert.Equal(t, uint64(50), s.Version("public.new"), "unknown tables get the floor")
	rel := schema.Relation{Namespace: "public", Name: "new", Identity: schema.IdentityDefault}
	obs, first := s.Observe(&rel)
	assert.True(t, first)
	assert.Equal(t, uint64(50), obs.Version)
	_, first = s.Observe(&rel)
	assert.False(t, first)
	assert.Len(t, s.State().Tables, 2)
}

func TestNewSchemas_InvalidState(t *testing.T) {
	t.Parallel()
	_, err := NewSchemas(schema.RegistryState{Tables: []schema.TableState{{Table: ""}}})
	require.ErrorIs(t, err, schema.ErrInvalidState)
}

func TestRouter_OpenTx(t *testing.T) {
	t.Parallel()
	tr, err := NewAckTracker(0x100)
	require.NoError(t, err)
	sink := fakesink.New("main")
	r, err := NewRouter(tr, []SinkRoute{{Sink: sink, Tables: []string{"*"}}})
	require.NoError(t, err)
	ctx := context.Background()
	_, err = r.OpenTx(ctx, 0x50, 0x50, time.Unix(0, 0))
	require.ErrorIs(t, err, ErrOutOfOrder)
	tx, err := r.OpenTx(ctx, 0x100, 0x100, time.Unix(0, 0))
	require.NoError(t, err)
	g, err := tx.NewGroup(time.Unix(0, 0))
	require.NoError(t, err)
	ev := change.Event{
		ID: change.SnapshotID(0x100, 1, 0), Source: src("public", "users"), Op: change.OpRead,
		Key: change.Row{{Name: "id", Value: []byte("1")}},
	}
	n, err := g.Route(ctx, pipeline.NewEventRecord(&ev))
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.ErrorIs(t, tx.Seal(), ErrGroupOpen)
	require.NoError(t, g.Seal())
	require.NoError(t, tx.Seal())
	_, err = tx.NewGroup(time.Unix(0, 0))
	require.ErrorIs(t, err, ErrSealed)
	assert.Equal(t, lsn.LSN(0x100), tr.Confirmed())
	require.NoError(t, sink.AckAll())
	<-g.Done()
	assert.Equal(t, lsn.LSN(0x100), tr.Confirmed())
}
