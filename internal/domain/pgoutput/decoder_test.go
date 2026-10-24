// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func mustDecode(t *testing.T, dec *pgoutput.Decoder, msgs ...pgoutput.Message) {
	t.Helper()
	for _, m := range msgs {
		_, err := dec.Decode(encode(m))
		require.NoError(t, err)
	}
}

func relationMsg(oid uint32, name string, identity schema.ReplicaIdentity, cols ...schema.Column) *pgoutput.Relation {
	return &pgoutput.Relation{Relation: schema.Relation{
		OID: oid, Namespace: "rec", Name: name, Identity: identity, Columns: cols,
	}}
}

func TestDecoder_Decode_ResolvesTypeNames(t *testing.T) {
	t.Parallel()
	dec := pgoutput.NewDecoder()
	rel := relationMsg(16393, "types", schema.IdentityDefault,
		schema.Column{Name: "id", TypeOID: schema.OIDInt8, TypeMod: -1, Key: true},
		schema.Column{Name: "tags", TypeOID: schema.OIDTextArray, TypeMod: -1},
		schema.Column{Name: "mood", TypeOID: 16387, TypeMod: -1},
		schema.Column{Name: "color", TypeOID: 16390, TypeMod: -1},
		schema.Column{Name: "cat", TypeOID: 16391, TypeMod: -1},
		schema.Column{Name: "ext", TypeOID: 16399, TypeMod: -1},
	)
	mustDecode(t, dec,
		&pgoutput.TypeMessage{OID: 16387, Namespace: "rec", Name: "mood"},
		&pgoutput.TypeMessage{OID: 16390, Namespace: "public", Name: "color"},
		&pgoutput.TypeMessage{OID: 16391, Namespace: "", Name: "catalog_type"},
	)
	m, err := dec.Decode(encode(rel))
	require.NoError(t, err)
	got := m.(*pgoutput.Relation) //nolint:forcetypeassert // Relation decoded above
	names := make([]string, len(got.Columns))
	for i, c := range got.Columns {
		names[i] = c.TypeName
	}
	assert.Equal(t, []string{"int8", "text[]", "rec.mood", "color", "catalog_type", "oid:16399"}, names)
	cached, ok := dec.Relation(16393)
	require.True(t, ok)
	assert.Equal(t, got.Relation, cached)
	got.Columns[0].Name = "mutated"
	cached, _ = dec.Relation(16393)
	assert.Equal(t, "id", cached.Columns[0].Name, "the cache keeps its own copy")
	_, ok = dec.Relation(1)
	assert.False(t, ok)
}

func TestDecoder_Decode_Error(t *testing.T) {
	t.Parallel()
	dec := pgoutput.NewDecoder()
	_, err := dec.Decode([]byte{'R', 0})
	require.ErrorIs(t, err, pgoutput.ErrMalformed)
}

func TestDecoder_KeyColumns_Identities(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		rel      *pgoutput.Relation
		override []string
		want     []string
	}{
		{
			name: "default pk",
			rel:  relationMsg(1, "t", schema.IdentityDefault, col("id", schema.OIDInt4, true), col("v", schema.OIDText, false)),
			want: []string{"id"},
		},
		{
			name: "default ignores override",
			rel: relationMsg(1, "t", schema.IdentityDefault,
				col("id", schema.OIDInt4, true), col("v", schema.OIDText, false)),
			override: []string{"v"},
			want:     []string{"id"},
		},
		{
			name: "full without pk",
			rel:  relationMsg(1, "t", schema.IdentityFull, col("a", schema.OIDInt4, true), col("b", schema.OIDText, true)),
			want: []string{"a", "b"},
		},
		{
			name:     "full with pk override",
			rel:      relationMsg(1, "t", schema.IdentityFull, col("a", schema.OIDInt4, true), col("b", schema.OIDText, true)),
			override: []string{"b"},
			want:     []string{"b"},
		},
		{
			name: "index",
			rel: relationMsg(1, "t", schema.IdentityIndex,
				col("a", schema.OIDInt4, true), col("b", schema.OIDInt4, true), col("c", schema.OIDText, false)),
			want: []string{"a", "b"},
		},
		{
			name: "keyless default",
			rel:  relationMsg(1, "t", schema.IdentityDefault, col("a", schema.OIDInt4, false)),
		},
		{
			name: "nothing",
			rel:  relationMsg(1, "t", schema.IdentityNothing, col("a", schema.OIDInt4, false)),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dec := pgoutput.NewDecoder()
			mustDecode(t, dec, tc.rel)
			require.NoError(t, dec.SetKeyColumns(1, tc.override))
			got, ok := dec.KeyColumns(1)
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDecoder_SetKeyColumns_Errors(t *testing.T) {
	t.Parallel()
	dec := pgoutput.NewDecoder()
	mustDecode(t, dec, relationMsg(1, "t", schema.IdentityFull,
		col("a", schema.OIDInt4, true), col("b", schema.OIDText, true)))
	require.ErrorIs(t, dec.SetKeyColumns(2, []string{"a"}), pgoutput.ErrUnknownRelation)
	require.ErrorIs(t, dec.SetKeyColumns(1, []string{"zz"}), pgoutput.ErrUnknownColumn)
	require.ErrorIs(t, dec.SetKeyColumns(1, []string{"a", "a"}), pgoutput.ErrUnknownColumn)
	_, ok := dec.KeyColumns(2)
	assert.False(t, ok)
	keys, _ := dec.KeyColumns(1)
	assert.Equal(t, []string{"a", "b"}, keys, "failed calls change nothing")
}

func TestDecoder_SetKeyColumns_SurvivesRelationResend(t *testing.T) {
	t.Parallel()
	dec := pgoutput.NewDecoder()
	full := relationMsg(1, "t", schema.IdentityFull, col("id", schema.OIDInt4, true), col("b", schema.OIDText, true))
	mustDecode(t, dec, full)
	require.NoError(t, dec.SetKeyColumns(1, []string{"id"}))

	added := relationMsg(1, "t", schema.IdentityFull,
		col("id", schema.OIDInt4, true), col("b", schema.OIDText, true), col("c", schema.OIDText, true))
	mustDecode(t, dec, added)
	keys, _ := dec.KeyColumns(1)
	assert.Equal(t, []string{"id"}, keys, "override kept while its columns exist")

	renamed := relationMsg(1, "t", schema.IdentityFull, col("key", schema.OIDInt4, true), col("b", schema.OIDText, true))
	mustDecode(t, dec, renamed)
	keys, _ = dec.KeyColumns(1)
	assert.Equal(t, []string{"key", "b"}, keys, "override dropped when a named column disappears")

	require.NoError(t, dec.SetKeyColumns(1, []string{"key"}))
	require.NoError(t, dec.SetKeyColumns(1, nil))
	keys, _ = dec.KeyColumns(1)
	assert.Equal(t, []string{"key", "b"}, keys, "empty names clear the override")
}
