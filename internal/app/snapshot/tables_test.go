// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakecatalog"
)

func stPolicy(t *testing.T, cfg pipeline.TablePolicyConfig) *pipeline.TablePolicy {
	t.Helper()
	p, err := pipeline.NewTablePolicy(cfg)
	require.NoError(t, err)
	return p
}

func TestNewCatalogTables_Validation(t *testing.T) {
	t.Parallel()
	cat := fakecatalog.New()
	policy := stPolicy(t, pipeline.TablePolicyConfig{DB: "shop"})
	tests := []struct {
		name         string
		lister       PublicationLister
		publications []string
		resolver     pipeline.TableResolver
		wantErr      bool
	}{
		{name: "valid", lister: cat, publications: []string{"wake_pub"}, resolver: policy},
		{name: "nil lister", publications: []string{"wake_pub"}, resolver: policy, wantErr: true},
		{name: "nil resolver", lister: cat, publications: []string{"wake_pub"}, wantErr: true},
		{name: "nil publications", lister: cat, resolver: policy, wantErr: true},
		{name: "empty publications", lister: cat, publications: []string{}, resolver: policy, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ct, err := NewCatalogTables(tc.lister, tc.publications, tc.resolver)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrInvalidOption)
				assert.Nil(t, ct)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, ct)
		})
	}
}

func TestCatalogTables_CapturedTables(t *testing.T) {
	t.Parallel()
	infos := []pipeline.TableInfo{
		{Schema: "public", Table: "orders", OID: 1, HasKey: true},
		{Schema: "public", Table: "log", OID: 2, HasKey: false},
		{Schema: "public", Table: "items", OID: 3, HasKey: true},
		{Schema: "audit", Table: "events", OID: 4, HasKey: true},
	}
	tests := []struct {
		name string
		cfg  pipeline.TablePolicyConfig
		want []Table
	}{
		{
			name: "name map applied and keyless skipped",
			cfg: pipeline.TablePolicyConfig{
				DB: "shop", NameMap: map[string]string{"public.orders": "sales.orders_v2"},
				Exclude: []string{"audit.*"},
			},
			want: []Table{
				{
					Name: "public.orders", Schema: "public", Table: "orders",
					Source: change.Source{DB: "shop", Schema: "sales", Table: "orders_v2"},
				},
				{
					Name: "public.items", Schema: "public", Table: "items",
					Source: change.Source{DB: "shop", Schema: "public", Table: "items"},
				},
			},
		},
		{
			name: "keyless emitted when allowed",
			cfg:  pipeline.TablePolicyConfig{DB: "shop", EmitUnkeyed: true, Include: []string{"public.*"}},
			want: []Table{
				{
					Name: "public.orders", Schema: "public", Table: "orders",
					Source: change.Source{DB: "shop", Schema: "public", Table: "orders"},
				},
				{
					Name: "public.log", Schema: "public", Table: "log",
					Source: change.Source{DB: "shop", Schema: "public", Table: "log"},
				},
				{
					Name: "public.items", Schema: "public", Table: "items",
					Source: change.Source{DB: "shop", Schema: "public", Table: "items"},
				},
			},
		},
		{
			name: "nothing captured",
			cfg:  pipeline.TablePolicyConfig{DB: "shop", Include: []string{"nomatch.*"}},
			want: []Table{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cat := fakecatalog.New()
			cat.SetPublicationTables(infos, nil)
			ct, err := NewCatalogTables(cat, []string{"pub_a", "pub_b"}, stPolicy(t, tc.cfg))
			require.NoError(t, err)
			got, err := ct.CapturedTables(t.Context())
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, [][]string{{"pub_a", "pub_b"}}, cat.PublicationCalls())
		})
	}
}

func TestCatalogTables_CapturedTables_ListerError(t *testing.T) {
	t.Parallel()
	cat := fakecatalog.New()
	cat.SetPublicationTables(nil, errStBoom)
	ct, err := NewCatalogTables(cat, []string{"pub"}, stPolicy(t, pipeline.TablePolicyConfig{DB: "shop"}))
	require.NoError(t, err)
	got, err := ct.CapturedTables(t.Context())
	require.ErrorIs(t, err, errStBoom)
	assert.Nil(t, got)
}

func TestFindTable(t *testing.T) {
	t.Parallel()
	tables := []Table{stTable("orders"), stTable("items")}
	tests := []struct {
		name   string
		tables []Table
		find   string
		want   Table
		wantOK bool
	}{
		{name: "hit first", tables: tables, find: "public.orders", want: tables[0], wantOK: true},
		{name: "hit last", tables: tables, find: "public.items", want: tables[1], wantOK: true},
		{name: "miss", tables: tables, find: "public.users"},
		{name: "unqualified name misses", tables: tables, find: "orders"},
		{name: "empty list", find: "public.orders"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := findTable(tc.tables, tc.find)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
