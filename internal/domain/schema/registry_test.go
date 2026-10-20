// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func evolveRelation(cols ...string) schema.Relation {
	rel := schema.Relation{OID: 16500, Namespace: "rec", Name: "evolve", Identity: schema.IdentityDefault}
	for i, c := range cols {
		rel.Columns = append(rel.Columns, schema.Column{
			Name: c, TypeOID: schema.OIDInt4, TypeMod: -1, TypeName: "int4", Key: i == 0,
		})
	}
	return rel
}

func TestRegistry_Observe_FirstSightingIsVersionOne(t *testing.T) {
	t.Parallel()
	reg := schema.NewRegistry()
	rel := evolveRelation("id", "a")
	obs := reg.Observe(&rel)
	assert.Equal(t, schema.Observation{Table: "rec.evolve", Version: 1}, obs)
	obs = reg.Observe(&rel)
	assert.False(t, obs.Changed)
	assert.Equal(t, uint64(1), obs.Version)
	v, ok := reg.Version("rec.evolve")
	assert.True(t, ok)
	assert.Equal(t, uint64(1), v)
	_, ok = reg.Version("rec.other")
	assert.False(t, ok)
}

func TestRegistry_Observe_ChangeIncrementsWithDiff(t *testing.T) {
	t.Parallel()
	reg := schema.NewRegistry()
	before := evolveRelation("id", "a")
	reg.Observe(&before)
	after := evolveRelation("id", "a", "b")
	obs := reg.Observe(&after)
	assert.Equal(t, schema.Observation{
		Table: "rec.evolve", Version: 2, Changed: true,
		Before: []change.ColumnType{{Name: "id", Type: "int4"}, {Name: "a", Type: "int4"}},
		After:  []change.ColumnType{{Name: "id", Type: "int4"}, {Name: "a", Type: "int4"}, {Name: "b", Type: "int4"}},
	}, obs)
	identity := after.Clone()
	identity.Identity = schema.IdentityFull
	obs = reg.Observe(&identity)
	assert.True(t, obs.Changed)
	assert.Equal(t, uint64(3), obs.Version)
}

func TestRegistry_Seed_RestartComparesStoredColumns(t *testing.T) {
	t.Parallel()
	stored := evolveRelation("id", "a")
	cases := []struct {
		name        string
		rel         schema.Relation
		wantVersion uint64
		wantChanged bool
	}{
		{name: "unchanged", rel: evolveRelation("id", "a"), wantVersion: 7},
		{name: "changed while down", rel: evolveRelation("id", "b"), wantVersion: 8, wantChanged: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := schema.NewRegistry()
			require.NoError(t, reg.Seed(schema.RegistryState{Tables: []schema.TableState{
				{Table: "rec.evolve", Version: 7, Identity: stored.Identity, Columns: stored.Columns},
			}}))
			obs := reg.Observe(&tc.rel)
			assert.Equal(t, tc.wantVersion, obs.Version)
			assert.Equal(t, tc.wantChanged, obs.Changed)
			if tc.wantChanged {
				assert.Equal(t, stored.ColumnTypes(), obs.Before)
			}
		})
	}
}

func TestRegistry_Seed_WithoutColumnsKeepsVersion(t *testing.T) {
	t.Parallel()
	reg := schema.NewRegistry()
	require.NoError(t, reg.Seed(schema.RegistryState{Tables: []schema.TableState{
		{Table: "rec.evolve", Version: 5}, {Table: "rec.zero"},
	}}))
	rel := evolveRelation("id")
	obs := reg.Observe(&rel)
	assert.Equal(t, uint64(5), obs.Version)
	assert.False(t, obs.Changed)
	zero := schema.Relation{Namespace: "rec", Name: "zero", Identity: schema.IdentityNothing}
	obs = reg.Observe(&zero)
	assert.Equal(t, uint64(1), obs.Version, "a seeded version 0 is raised to 1")
	obs = reg.Observe(&zero)
	assert.False(t, obs.Changed, "a table without columns has a known (empty) shape once observed")
}

func TestRegistry_Observe_SeededVersionZeroBecomesOne(t *testing.T) {
	t.Parallel()
	rel := evolveRelation("id", "x")
	reg := schema.NewRegistry()
	require.NoError(t, reg.Seed(schema.RegistryState{Tables: []schema.TableState{
		{Table: "rec.evolve", Version: 0, Identity: rel.Identity, Columns: rel.Columns},
	}}))
	obs := reg.Observe(&rel)
	assert.False(t, obs.Changed)
	assert.Equal(t, uint64(1), obs.Version)
}

func TestRegistry_Seed_MergesAndKeepsLiveShape(t *testing.T) {
	t.Parallel()
	reg := schema.NewRegistry()
	live := evolveRelation("id", "a")
	reg.Observe(&live)
	other := evolveRelation("id", "zzz")
	require.NoError(t, reg.Seed(schema.RegistryState{Floor: 3, Tables: []schema.TableState{
		{Table: "rec.evolve", Version: 4, Identity: other.Identity, Columns: other.Columns},
	}}))
	obs := reg.Observe(&live)
	assert.False(t, obs.Changed, "live shape wins over seeded shape")
	assert.Equal(t, uint64(4), obs.Version)
	require.NoError(t, reg.Seed(schema.RegistryState{Tables: []schema.TableState{{Table: "rec.evolve", Version: 2}}}))
	v, _ := reg.Version("rec.evolve")
	assert.Equal(t, uint64(4), v, "seeding never lowers a version")
	fresh := schema.Relation{Namespace: "rec", Name: "fresh", Identity: schema.IdentityDefault}
	assert.Equal(t, uint64(3), reg.Observe(&fresh).Version, "new tables start at the floor")
}

func TestRegistry_Seed_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		tables []schema.TableState
	}{
		{name: "empty name", tables: []schema.TableState{{Table: " "}}},
		{name: "duplicate", tables: []schema.TableState{{Table: "a.b"}, {Table: "a.b"}}},
		{name: "bad identity", tables: []schema.TableState{{Table: "a.b", Identity: 'x', Columns: []schema.Column{}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := schema.NewRegistry()
			err := reg.Seed(schema.RegistryState{Floor: 9, Tables: tc.tables})
			require.ErrorIs(t, err, schema.ErrInvalidState)
			assert.Equal(t, schema.RegistryState{Tables: []schema.TableState{}}, reg.State(), "nothing applied")
		})
	}
}

func TestRegistry_Reinit_VersionAtLeastNow(t *testing.T) {
	t.Parallel()
	const now = int64(1_760_600_000)
	reg := schema.NewRegistry()
	require.NoError(t, reg.Seed(schema.RegistryState{Tables: []schema.TableState{
		{Table: "rec.low", Version: 3}, {Table: "rec.high", Version: math.MaxUint64 - 1},
	}}))
	reg.Reinit(now)
	reg.Reinit(-5)
	st := reg.State()
	assert.Equal(t, uint64(now), st.Floor)
	assert.Equal(t, []schema.TableState{
		{Table: "rec.high", Version: math.MaxUint64 - 1},
		{Table: "rec.low", Version: uint64(now)},
	}, st.Tables)
	rel := schema.Relation{Namespace: "rec", Name: "new", Identity: schema.IdentityDefault}
	assert.Equal(t, uint64(now), reg.Observe(&rel).Version)
}

func TestRegistry_Observe_VersionSaturates(t *testing.T) {
	t.Parallel()
	reg := schema.NewRegistry()
	a := evolveRelation("id", "a")
	require.NoError(t, reg.Seed(schema.RegistryState{Tables: []schema.TableState{
		{Table: "rec.evolve", Version: math.MaxUint64, Identity: a.Identity, Columns: a.Columns},
	}}))
	b := evolveRelation("id", "b")
	obs := reg.Observe(&b)
	assert.True(t, obs.Changed)
	assert.Equal(t, uint64(math.MaxUint64), obs.Version)
}

func TestRegistry_State_RoundTrip(t *testing.T) {
	t.Parallel()
	reg := schema.NewRegistry()
	a := evolveRelation("id", "a")
	reg.Observe(&a)
	st := reg.State()
	st.Tables[0].Columns[0].Name = "mutated"
	clone := schema.NewRegistry()
	require.NoError(t, clone.Seed(reg.State()))
	assert.Equal(t, reg.State(), clone.State())
	assert.False(t, clone.Observe(&a).Changed, "exported state is a deep copy")
}

// TestRegistry_VersionsNeverDecrease checks that a schema version never
// decreases under arbitrary sequences of Seed, Observe and Reinit.
func TestRegistry_VersionsNeverDecrease(t *testing.T) {
	t.Parallel()
	tables := []string{"a", "b", "c"}
	shapes := [][]string{{"id"}, {"id", "x"}, {"id", "y"}, {}}
	rapid.Check(t, func(t *rapid.T) {
		reg := schema.NewRegistry()
		last := map[string]uint64{}
		steps := rapid.IntRange(1, 40).Draw(t, "steps")
		for range steps {
			table := rapid.SampledFrom(tables).Draw(t, "table")
			registryStep(t, reg, table, shapes, last)
			for _, name := range tables {
				v, ok := reg.Version("s." + name)
				if !ok {
					continue
				}
				require.GreaterOrEqual(t, v, last["s."+name], "version of %s decreased", name)
				last["s."+name] = v
			}
		}
	})
}

func registryStep(t *rapid.T, reg *schema.Registry, table string, shapes [][]string, last map[string]uint64) {
	switch rapid.IntRange(0, 2).Draw(t, "op") {
	case 0:
		rel := evolveRelation(rapid.SampledFrom(shapes).Draw(t, "shape")...)
		rel.Namespace, rel.Name = "s", table
		obs := reg.Observe(&rel)
		assert.GreaterOrEqual(t, obs.Version, uint64(1))
		if obs.Changed && last["s."+table] < math.MaxUint64 {
			assert.Greater(t, obs.Version, last["s."+table], "a change increments")
		}
	case 1:
		ts := schema.TableState{Table: "s." + table, Version: rapid.Uint64().Draw(t, "version")}
		if rapid.Bool().Draw(t, "with columns") {
			rel := evolveRelation(rapid.SampledFrom(shapes).Draw(t, "seed shape")...)
			ts.Identity, ts.Columns = rel.Identity, rel.Columns
		}
		floor := rapid.Uint64().Draw(t, "floor")
		require.NoError(t, reg.Seed(schema.RegistryState{Floor: floor, Tables: []schema.TableState{ts}}))
	default:
		reg.Reinit(rapid.Int64().Draw(t, "now"))
	}
}
