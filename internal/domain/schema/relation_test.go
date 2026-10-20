// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func usersRelation() schema.Relation {
	return schema.Relation{
		OID: 16400, Namespace: "public", Name: "users", Identity: schema.IdentityDefault,
		Columns: []schema.Column{
			{Name: "id", TypeOID: schema.OIDInt8, TypeMod: -1, TypeName: "int8", Key: true},
			{Name: "email", TypeOID: schema.OIDText, TypeMod: -1, TypeName: "text"},
			{Name: "mood", TypeOID: 16387, TypeMod: -1, TypeName: "mood"},
			{Name: "mood2", TypeOID: 16387, TypeMod: -1, TypeName: "mood"},
		},
	}
}

func TestReplicaIdentity_Valid(t *testing.T) {
	t.Parallel()
	for _, ri := range []schema.ReplicaIdentity{'d', 'n', 'f', 'i'} {
		assert.True(t, ri.Valid(), string(rune(ri)))
	}
	for _, ri := range []schema.ReplicaIdentity{0, 'x', 'D'} {
		assert.False(t, ri.Valid(), string(rune(ri)))
	}
}

func TestRelation_Accessors(t *testing.T) {
	t.Parallel()
	rel := usersRelation()
	assert.Equal(t, "public.users", rel.QualifiedName())
	assert.Equal(t, []string{"id"}, rel.KeyColumnNames())
	assert.False(t, rel.IsKeyless())
	assert.Equal(t, 1, rel.ColumnIndex("email"))
	assert.Equal(t, -1, rel.ColumnIndex("missing"))
	assert.Equal(t, []uint32{16387}, rel.UnknownTypes())
	assert.Equal(t, []change.ColumnType{
		{Name: "id", Type: "int8"},
		{Name: "email", Type: "text"},
		{Name: "mood", Type: "mood"},
		{Name: "mood2", Type: "mood"},
	}, rel.ColumnTypes())

	catalog := schema.Relation{Name: "pg_class"}
	assert.Equal(t, "pg_catalog.pg_class", catalog.QualifiedName())
}

func TestRelation_IsKeyless_Identities(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(r *schema.Relation)
		keyless bool
		keys    []string
	}{
		{name: "default with pk", mutate: func(*schema.Relation) {}, keys: []string{"id"}},
		{name: "nothing", mutate: func(r *schema.Relation) {
			r.Identity = schema.IdentityNothing
			r.Columns[0].Key = false
		}, keyless: true},
		{name: "default without pk", mutate: func(r *schema.Relation) { r.Columns[0].Key = false }, keyless: true},
		{name: "full flags every column", mutate: func(r *schema.Relation) {
			r.Identity = schema.IdentityFull
			for i := range r.Columns {
				r.Columns[i].Key = true
			}
		}, keys: []string{"id", "email", "mood", "mood2"}},
		{
			name: "nothing with stale flag", mutate: func(r *schema.Relation) { r.Identity = schema.IdentityNothing },
			keyless: true, keys: []string{"id"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rel := usersRelation()
			tc.mutate(&rel)
			assert.Equal(t, tc.keyless, rel.IsKeyless())
			assert.Equal(t, tc.keys, rel.KeyColumnNames())
		})
	}
}

func TestRelation_SameSchema_Changes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(r *schema.Relation)
		same   bool
	}{
		{name: "identical", mutate: func(*schema.Relation) {}, same: true},
		{name: "oid and names ignored", mutate: func(r *schema.Relation) {
			r.OID, r.Namespace, r.Name = 1, "other", "renamed"
			r.Columns[1].TypeName = "string"
		}, same: true},
		{name: "column renamed", mutate: func(r *schema.Relation) { r.Columns[1].Name = "mail" }},
		{name: "type changed", mutate: func(r *schema.Relation) { r.Columns[1].TypeOID = schema.OIDVarchar }},
		{name: "typmod changed", mutate: func(r *schema.Relation) { r.Columns[1].TypeMod = 24 }},
		{name: "key flag changed", mutate: func(r *schema.Relation) { r.Columns[1].Key = true }},
		{name: "identity changed", mutate: func(r *schema.Relation) { r.Identity = schema.IdentityFull }},
		{name: "column added", mutate: func(r *schema.Relation) {
			r.Columns = append(r.Columns, schema.Column{Name: "x", TypeOID: schema.OIDText})
		}},
		{name: "column dropped", mutate: func(r *schema.Relation) { r.Columns = r.Columns[:1] }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := usersRelation()
			other := base.Clone()
			tc.mutate(&other)
			assert.Equal(t, tc.same, base.SameSchema(&other))
			assert.Equal(t, usersRelation(), base, "Clone must not share columns")
		})
	}
}
