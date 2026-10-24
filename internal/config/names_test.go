// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
)

func TestValidIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want bool
	}{
		{"users", true},
		{"Order_Items-2", true},
		{"", false},
		{"has space", false},
		{"dot.ted", false},
		{"naïve", false},
		{"a/b", false},
		{"tab\t", false},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, config.ValidIdentifier(tc.in))
		})
	}
}

func TestSplitQualified(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		in          string
		schema, tbl string
		wantOK      bool
	}{
		{"plain", "public.users", "public", "users", true},
		{"split at first dot", "public.my.table", "public", "my.table", true},
		{"no dot", "users", "", "", false},
		{"empty schema", ".users", "", "", false},
		{"empty table", "public.", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			schema, tbl, ok := config.SplitQualified(tc.in)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.schema, schema)
			assert.Equal(t, tc.tbl, tbl)
		})
	}
}

func TestTopicName(t *testing.T) {
	t.Parallel()

	prefix := "cdc"
	longest := strings.Repeat("t", 249-len("cdc.public."))
	tests := []struct {
		name                  string
		prefix, schema, table string
		want                  string
		wantErr               error
	}{
		{"simple", prefix, "public", "users", "cdc.public.users", nil},
		{"exactly 249", prefix, "public", longest, "cdc.public." + longest, nil},
		{"250", prefix, "public", longest + "t", "", config.ErrTopicTooLong},
		{"bad prefix", "my prefix", "public", "users", "", config.ErrIdentifierInvalid},
		{"empty prefix", "", "public", "users", "", config.ErrIdentifierInvalid},
		{"bad schema", prefix, "pub lic", "users", "", config.ErrIdentifierInvalid},
		{"bad table", prefix, "public", "us.ers", "", config.ErrIdentifierInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.TopicName(tc.prefix, tc.schema, tc.table)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTables_MappedName(t *testing.T) {
	t.Parallel()

	tbl := config.Tables{NameMap: map[string]string{
		"public.User Data": "public.user_data",
		"public.broken":    "nodot",
	}}
	tests := []struct {
		name          string
		schema, table string
		wantSchema    string
		wantTable     string
	}{
		{"mapped", "public", "User Data", "public", "user_data"},
		{"unmapped passes through", "public", "orders", "public", "orders"},
		{"malformed target passes through", "public", "broken", "public", "broken"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, tb := tbl.MappedName(tc.schema, tc.table)
			assert.Equal(t, tc.wantSchema, s)
			assert.Equal(t, tc.wantTable, tb)
		})
	}
}
