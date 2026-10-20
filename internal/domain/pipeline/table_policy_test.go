// SPDX-License-Identifier: Apache-2.0

package pipeline_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestNewTablePolicy_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  pipeline.TablePolicyConfig
		want error
	}{
		{
			name: "key without dot", cfg: pipeline.TablePolicyConfig{NameMap: map[string]string{"users": "public.users"}},
			want: pipeline.ErrInvalidTablePolicy,
		},
		{
			name: "target without dot", cfg: pipeline.TablePolicyConfig{NameMap: map[string]string{"a.b": "users"}},
			want: pipeline.ErrInvalidTablePolicy,
		},
		{
			name: "target invalid", cfg: pipeline.TablePolicyConfig{NameMap: map[string]string{"a.b": "pub lic.users"}},
			want: pipeline.ErrInvalidTablePolicy,
		},
		{name: "bad include", cfg: pipeline.TablePolicyConfig{Include: []string{"["}}, want: pipeline.ErrBadTablePattern},
		{name: "bad exclude", cfg: pipeline.TablePolicyConfig{Exclude: []string{""}}, want: pipeline.ErrBadTablePattern},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pipeline.NewTablePolicy(tc.cfg)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestTablePolicy_Resolve(t *testing.T) {
	t.Parallel()
	cfg := pipeline.TablePolicyConfig{
		DB:      "app",
		NameMap: map[string]string{"weird.Ta ble": "public.table_x", "public.renamed": "app.users"},
		Include: []string{"public.*", "weird.*"},
		Exclude: []string{"public.audit_*"},
	}
	cases := []struct {
		name        string
		emitUnkeyed bool
		info        pipeline.TableInfo
		want        pipeline.TableDecision
	}{
		{
			name: "captured", info: pipeline.TableInfo{Schema: "public", Table: "users", HasKey: true},
			want: pipeline.TableDecision{Capture: true, Source: change.Source{DB: "app", Schema: "public", Table: "users"}},
		},
		{
			name: "mapped", info: pipeline.TableInfo{Schema: "public", Table: "renamed", HasKey: true},
			want: pipeline.TableDecision{Capture: true, Source: change.Source{DB: "app", Schema: "app", Table: "users"}},
		},
		{
			name: "mapped invalid name", info: pipeline.TableInfo{Schema: "weird", Table: "Ta ble", HasKey: true},
			want: pipeline.TableDecision{Capture: true, Source: change.Source{DB: "app", Schema: "public", Table: "table_x"}},
		},
		{
			name: "invalid unmapped", info: pipeline.TableInfo{Schema: "weird", Table: "a.b", HasKey: true},
			want: pipeline.TableDecision{Skip: pipeline.SkipIdentifierInvalid},
		},
		{
			name: "excluded", info: pipeline.TableInfo{Schema: "public", Table: "audit_log", HasKey: true},
			want: pipeline.TableDecision{Skip: pipeline.SkipNotCaptured},
		},
		{
			name: "not included", info: pipeline.TableInfo{Schema: "other", Table: "t", HasKey: true},
			want: pipeline.TableDecision{Skip: pipeline.SkipNotCaptured},
		},
		{
			name: "keyless", info: pipeline.TableInfo{Schema: "public", Table: "events"},
			want: pipeline.TableDecision{Skip: pipeline.SkipKeyless},
		},
		{
			name: "keyless emitted", emitUnkeyed: true, info: pipeline.TableInfo{Schema: "public", Table: "events"},
			want: pipeline.TableDecision{Capture: true, Source: change.Source{DB: "app", Schema: "public", Table: "events"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := cfg
			c.EmitUnkeyed = tc.emitUnkeyed
			p, err := pipeline.NewTablePolicy(c)
			require.NoError(t, err)
			assert.Equal(t, tc.want, p.Resolve(tc.info))
		})
	}
}

func TestTablePolicy_Resolve_NoIncludeCapturesAll(t *testing.T) {
	t.Parallel()
	p, err := pipeline.NewTablePolicy(pipeline.TablePolicyConfig{DB: "app"})
	require.NoError(t, err)
	got := p.Resolve(pipeline.TableInfo{Schema: "s", Table: "t", HasKey: true})
	assert.True(t, got.Capture)
	assert.Equal(t, change.Source{DB: "app", Schema: "s", Table: "t"}, got.Source)
}
