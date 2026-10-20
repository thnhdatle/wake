// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func sampleRelation() *pgoutput.Relation {
	return &pgoutput.Relation{Relation: schema.Relation{
		OID: 16400, Namespace: "rec", Name: "docs", Identity: schema.IdentityDefault,
		Columns: []schema.Column{
			{Name: "id", TypeOID: schema.OIDInt4, TypeMod: -1, Key: true},
			{Name: "title", TypeOID: schema.OIDVarchar, TypeMod: 24},
			{Name: "body", TypeOID: schema.OIDText, TypeMod: -1},
		},
	}}
}

// sampleMessages returns one valid message of every supported type.
func sampleMessages() []struct {
	name string
	msg  pgoutput.Message
} {
	return []struct {
		name string
		msg  pgoutput.Message
	}{
		{name: "begin", msg: &pgoutput.Begin{FinalLSN: 0x16B3748, TsMs: 1_760_600_000_123, XID: 7812}},
		{name: "begin before 2000", msg: &pgoutput.Begin{FinalLSN: 1, TsMs: 1_000, XID: 1}},
		{name: "commit", msg: &pgoutput.Commit{CommitLSN: 0x16B3748, EndLSN: 0x16B3778, TsMs: 1_760_600_000_123}},
		{name: "origin", msg: &pgoutput.Origin{CommitLSN: 0x1_0000_0001, Name: "node_a"}},
		{name: "relation", msg: sampleRelation()},
		{name: "relation without columns", msg: &pgoutput.Relation{Relation: schema.Relation{
			OID: 1, Namespace: "", Name: "empty", Identity: schema.IdentityNothing, Columns: []schema.Column{},
		}}},
		{name: "type", msg: &pgoutput.TypeMessage{OID: 16387, Namespace: "rec", Name: "mood"}},
		{name: "insert", msg: &pgoutput.Insert{RelationOID: 16400, New: pgoutput.Tuple{txt("1"), null(), txt("")}}},
		{name: "update no old", msg: &pgoutput.Update{RelationOID: 16400, New: pgoutput.Tuple{txt("1"), txt("b"), toast()}}},
		{name: "update old key", msg: &pgoutput.Update{
			RelationOID: 16400, OldKind: pgoutput.OldKey,
			Old: pgoutput.Tuple{txt("2"), null(), null()}, New: pgoutput.Tuple{txt("3"), txt("t"), txt("x")},
		}},
		{name: "update old full", msg: &pgoutput.Update{
			RelationOID: 16400, OldKind: pgoutput.OldFull,
			Old: pgoutput.Tuple{txt("1"), txt("a"), txt("x")}, New: pgoutput.Tuple{txt("1"), txt("b"), toast()},
		}},
		{name: "delete key", msg: &pgoutput.Delete{
			RelationOID: 16400, OldKind: pgoutput.OldKey, Old: pgoutput.Tuple{txt("3"), null(), null()},
		}},
		{name: "delete full", msg: &pgoutput.Delete{
			RelationOID: 16400, OldKind: pgoutput.OldFull, Old: pgoutput.Tuple{txt("1"), txt("b"), txt("x")},
		}},
		{name: "binary column", msg: &pgoutput.Insert{RelationOID: 16400, New: pgoutput.Tuple{
			{Kind: pgoutput.TupleBinary, Data: []byte{0, 0, 0, 1}}, null(), null(),
		}}},
		{name: "truncate", msg: &pgoutput.Truncate{Options: 3, RelationOIDs: []uint32{16400, 16407}}},
		{name: "truncate none", msg: &pgoutput.Truncate{RelationOIDs: []uint32{}}},
		{name: "message transactional", msg: &pgoutput.LogicalMessage{
			Flags: 1, LSN: 0x197D6F0, Prefix: "app.audit", Content: []byte("in-tx"),
		}},
		{name: "message empty", msg: &pgoutput.LogicalMessage{LSN: 2, Prefix: "", Content: []byte{}}},
	}
}

func TestParse_RoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range sampleMessages() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := encode(tc.msg)
			got, err := pgoutput.Parse(data)
			require.NoError(t, err)
			assert.Equal(t, tc.msg, got)
			assert.Equal(t, pgoutput.MessageType(data[0]), got.MessageType())
		})
	}
}

// TestParse_TruncatedNeverPanics cuts every sample message at every length:
// each prefix is malformed (or, for an empty input, rejected) and never
// panics.
func TestParse_TruncatedNeverPanics(t *testing.T) {
	t.Parallel()
	for _, tc := range sampleMessages() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := encode(tc.msg)
			for n := range len(data) {
				_, err := pgoutput.Parse(data[:n])
				require.ErrorIs(t, err, pgoutput.ErrMalformed, "prefix of %d bytes", n)
			}
			_, err := pgoutput.Parse(append(data, 0))
			require.ErrorIs(t, err, pgoutput.ErrMalformed, "trailing byte")
		})
	}
}

func TestParse_Malformed(t *testing.T) {
	t.Parallel()
	rel := encodeRelation(&sampleRelation().Relation)
	cases := []struct {
		name string
		data []byte
	}{
		{name: "insert without N", data: wire{'I'}.u32(1).u8('K').tuple(pgoutput.Tuple{txt("1")})},
		{name: "update bad tag", data: wire{'U'}.u32(1).u8('X').tuple(pgoutput.Tuple{txt("1")})},
		{name: "update old then no N", data: wire{'U'}.u32(1).u8('K').tuple(pgoutput.Tuple{txt("1")}).u8('K')},
		{name: "delete bad tag", data: wire{'D'}.u32(1).u8('N').tuple(pgoutput.Tuple{txt("1")})},
		{name: "unknown column kind", data: wire{'I'}.u32(1).u8('N').u16(1).u8('x')},
		{name: "negative column length", data: wire{'I'}.u32(1).u8('N').u16(1).u8('t').u32(0xFFFFFFFF)},
		{name: "column length beyond message", data: wire{'I'}.u32(1).u8('N').u16(1).u8('t').u32(100).cstr("ab")},
		{name: "huge column count", data: wire{'I'}.u32(1).u8('N').u16(0xFFFF).u8('n')},
		{name: "relation bad identity", data: wire{'R'}.u32(1).cstr("s").cstr("t").u8('x').u16(0)},
		{name: "relation huge column count", data: wire{'R'}.u32(1).cstr("s").cstr("t").u8('d').u16(0xFFFF).u64(0)},
		{name: "relation missing NUL", data: wire{'R'}.u32(1).cstr("s")[:6]},
		{name: "relation trailing", data: append(rel, 1, 2)},
		{name: "truncate huge count", data: wire{'T'}.u32(0x7FFFFFFF).u8(0).u32(1)},
		{name: "message negative length", data: wire{'M'}.u8(0).u64(1).cstr("p").u32(0x80000000)},
		{name: "empty", data: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := pgoutput.Parse(tc.data)
			require.ErrorIs(t, err, pgoutput.ErrMalformed)
			assert.Nil(t, m)
			assert.Contains(t, err.Error(), "pgoutput: malformed message")
		})
	}
}

func TestParse_UnsupportedTypes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		typ       byte
		streaming bool
	}{
		{name: "stream start", typ: 'S', streaming: true},
		{name: "stream stop", typ: 'E', streaming: true},
		{name: "stream commit", typ: 'c', streaming: true},
		{name: "stream abort", typ: 'A', streaming: true},
		{name: "begin prepare", typ: 'b'},
		{name: "prepare", typ: 'P'},
		{name: "commit prepared", typ: 'K'},
		{name: "rollback prepared", typ: 'r'},
		{name: "stream prepare", typ: 'p'},
		{name: "garbage", typ: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pgoutput.Parse([]byte{tc.typ, 0, 0, 0, 1})
			var ue *pgoutput.UnsupportedMessageError
			require.ErrorAs(t, err, &ue)
			assert.Equal(t, tc.typ, ue.Type)
			assert.Equal(t, tc.streaming, errors.Is(err, pgoutput.ErrStreaming))
			assert.Contains(t, err.Error(), "pgoutput: unsupported message type")
		})
	}
}

func TestParse_TimestampsFloorToMilliseconds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		us   int64
		want int64
	}{
		{name: "epoch", us: 0, want: pgEpochUnixMs},
		{name: "sub-millisecond", us: 999, want: pgEpochUnixMs},
		{name: "before epoch floors", us: -1, want: pgEpochUnixMs - 1},
		{name: "before epoch exact", us: -1000, want: pgEpochUnixMs - 1},
		{name: "recorded", us: 845_623_804_187_082, want: 1_792_308_604_187},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := pgoutput.Parse(wire{'B'}.u64(1).u64(uint64(tc.us)).u32(1)) //nolint:gosec // G115: wire form
			require.NoError(t, err)
			assert.Equal(t, tc.want, m.(*pgoutput.Begin).TsMs) //nolint:forcetypeassert // Begin parsed above
		})
	}
}

func TestLogicalMessage_Flags(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		msg           pgoutput.LogicalMessage
		transactional bool
		internal      bool
	}{
		{
			name: "heartbeat", transactional: true, internal: true,
			msg: pgoutput.LogicalMessage{Flags: 1, Prefix: pgoutput.PrefixHeartbeat},
		},
		{
			name: "watermark", transactional: true, internal: true,
			msg: pgoutput.LogicalMessage{Flags: 1, Prefix: pgoutput.PrefixWatermark},
		},
		{name: "snapshot", msg: pgoutput.LogicalMessage{Prefix: pgoutput.PrefixSnapshot}, internal: true},
		{name: "app", msg: pgoutput.LogicalMessage{Prefix: "app.audit"}},
		{name: "lookalike", msg: pgoutput.LogicalMessage{Prefix: "wakeup"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.transactional, tc.msg.Transactional())
			assert.Equal(t, tc.internal, tc.msg.IsInternal())
			assert.Equal(t, tc.internal, pgoutput.IsInternal(tc.msg.Prefix))
		})
	}
}

func TestTruncate_Options(t *testing.T) {
	t.Parallel()
	m := pgoutput.Truncate{Options: pgoutput.TruncateCascade}
	assert.True(t, m.Cascade())
	assert.False(t, m.RestartIdentity())
	m.Options = pgoutput.TruncateRestartIdentity
	assert.False(t, m.Cascade())
	assert.True(t, m.RestartIdentity())
}

func TestParse_AliasesInput(t *testing.T) {
	t.Parallel()
	data := encode(&pgoutput.Insert{RelationOID: 1, New: pgoutput.Tuple{txt("abc")}})
	m, err := pgoutput.Parse(data)
	require.NoError(t, err)
	ins := m.(*pgoutput.Insert) //nolint:forcetypeassert // Insert parsed above
	data[len(data)-1] = 'X'
	assert.Equal(t, "abX", string(ins.New[0].Data), "tuple data aliases the input buffer")
}
