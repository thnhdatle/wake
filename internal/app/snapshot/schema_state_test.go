// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func stSortTables(rs *schema.RegistryState) {
	slices.SortFunc(rs.Tables, func(a, b schema.TableState) int { return strings.Compare(a.Table, b.Table) })
}

func TestRegistryState_RoundTrip(t *testing.T) {
	t.Parallel()
	want := schema.RegistryState{
		Floor: 1_760_000_000,
		Tables: []schema.TableState{
			{
				Table: "public.items", Version: 3, Identity: schema.IdentityIndex,
				Columns: []schema.Column{
					{Name: "id", TypeOID: 23, TypeMod: -1, TypeName: "int4", Key: true},
					{Name: "price", TypeOID: 1700, TypeMod: 655366, TypeName: "numeric"},
				},
			},
			{Table: "public.orders", Version: 1<<63 + 5, Identity: schema.IdentityDefault, Columns: []schema.Column{}},
			{Table: "public.unknown", Version: 8}, // unknown shape: nil columns
			{Table: "public.unknown_identity", Version: 2, Identity: schema.IdentityNothing},
		},
	}
	st := pipeline.NewState(stSlot, stSystem)
	setRegistryState(&st, &want)

	assert.Equal(t, want.Floor, st.SchemaFloor)
	assert.Equal(t, uint64(8), st.Schemas["public.unknown"].Version)
	assert.Nil(t, st.Schemas["public.unknown"].Columns, "an unknown shape stays unknown in the state file")
	assert.Empty(t, st.Schemas["public.unknown"].ReplicaIdentity)
	items := st.Schemas["public.items"]
	assert.Equal(t, "i", items.ReplicaIdentity)
	assert.Equal(t, []pipeline.SchemaColumn{
		{Name: "id", Type: "int4", TypeOID: 23, TypeMod: -1, Key: true},
		{Name: "price", Type: "numeric", TypeOID: 1700, TypeMod: 655366},
	}, items.Columns)
	assert.NotNil(t, st.Schemas["public.orders"].Columns)

	got := RegistryState(&st)
	stSortTables(&got)
	assert.Equal(t, want, got)
	for _, ts := range got.Tables {
		if ts.Table == "public.unknown" || ts.Table == "public.unknown_identity" {
			assert.Nil(t, ts.Columns, ts.Table)
		}
	}
}

func TestRegistryState_Empty(t *testing.T) {
	t.Parallel()
	st := pipeline.NewState(stSlot, stSystem)
	got := RegistryState(&st)
	assert.Equal(t, schema.RegistryState{Tables: []schema.TableState{}}, got)

	st.SchemaFloor = 77
	setRegistryState(&st, &got)
	assert.Zero(t, st.SchemaFloor, "setRegistryState replaces the floor")
	assert.Empty(t, st.Schemas)
	assert.NotNil(t, st.Schemas)
}

func TestRegistryState_IdentityBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		repl string
		want schema.ReplicaIdentity
	}{
		{name: "default", repl: "d", want: schema.IdentityDefault},
		{name: "nothing", repl: "n", want: schema.IdentityNothing},
		{name: "full", repl: "f", want: schema.IdentityFull},
		{name: "index", repl: "i", want: schema.IdentityIndex},
		{name: "empty", repl: "", want: 0},
		{name: "too long", repl: "dd", want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := pipeline.NewState(stSlot, stSystem)
			st.Schemas["public.t"] = pipeline.TableSchema{Version: 1, ReplicaIdentity: tc.repl}
			got := RegistryState(&st)
			require.Len(t, got.Tables, 1)
			assert.Equal(t, tc.want, got.Tables[0].Identity)
		})
	}
}

func TestSetRegistryState_ReplacesAndDoesNotAlias(t *testing.T) {
	t.Parallel()
	st := pipeline.NewState(stSlot, stSystem)
	st.Schemas["public.old"] = pipeline.TableSchema{Version: 9}
	rs := schema.RegistryState{Tables: []schema.TableState{{
		Table: "public.new", Version: 1, Identity: schema.IdentityDefault,
		Columns: []schema.Column{{Name: "id", TypeName: "int4"}},
	}}}
	setRegistryState(&st, &rs)
	rs.Tables[0].Columns[0].Name = "mutated"
	assert.NotContains(t, st.Schemas, "public.old")
	assert.Equal(t, "id", st.Schemas["public.new"].Columns[0].Name)
}

func TestRegistryState_Seed_AcceptsTheStateFile(t *testing.T) {
	t.Parallel()
	st := pipeline.NewState(stSlot, stSystem)
	st.Schemas["public.t"] = pipeline.TableSchema{
		Version: 4, ReplicaIdentity: "d", Columns: []pipeline.SchemaColumn{{Name: "id", Type: "int4", TypeOID: 23}},
	}
	reg := schema.NewRegistry()
	require.NoError(t, reg.Seed(RegistryState(&st)))
	v, ok := reg.Version("public.t")
	assert.True(t, ok)
	assert.Equal(t, uint64(4), v)
}

func genStColumn() *rapid.Generator[schema.Column] {
	return rapid.Custom(func(t *rapid.T) schema.Column {
		return schema.Column{
			Name:     rapid.String().Draw(t, "name"),
			TypeOID:  rapid.Uint32().Draw(t, "oid"),
			TypeMod:  rapid.Int32().Draw(t, "mod"),
			TypeName: rapid.String().Draw(t, "type"),
			Key:      rapid.Bool().Draw(t, "key"),
		}
	})
}

func genStTable(name string) *rapid.Generator[schema.TableState] {
	return rapid.Custom(func(t *rapid.T) schema.TableState {
		ts := schema.TableState{Table: name, Version: rapid.Uint64().Draw(t, "version")}
		if rapid.Bool().Draw(t, "known") {
			ts.Identity = rapid.SampledFrom([]schema.ReplicaIdentity{
				schema.IdentityDefault, schema.IdentityNothing, schema.IdentityFull, schema.IdentityIndex,
			}).Draw(t, "identity")
			ts.Columns = rapid.SliceOfN(genStColumn(), 0, 5).Draw(t, "columns")
			if ts.Columns == nil {
				ts.Columns = []schema.Column{}
			}
		} else if rapid.Bool().Draw(t, "identityOnly") {
			ts.Identity = schema.IdentityFull // identity known, columns unknown
		}
		return ts
	})
}

func TestRegistryState_Property_SetThenGetIsIdentity(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		names := rapid.SliceOfNDistinct(
			rapid.StringMatching(`[a-z][a-z0-9_]{0,7}\.[a-z][a-z0-9_]{0,7}`), 0, 6, rapid.ID,
		).Draw(t, "names")
		rs := schema.RegistryState{
			Floor:  rapid.Uint64().Draw(t, "floor"),
			Tables: make([]schema.TableState, 0, len(names)),
		}
		for _, n := range names {
			rs.Tables = append(rs.Tables, genStTable(n).Draw(t, "table"))
		}
		st := pipeline.NewState(stSlot, stSystem)
		setRegistryState(&st, &rs)
		got := RegistryState(&st)
		stSortTables(&rs)
		stSortTables(&got)
		require.Equal(t, rs, got)
	})
}
