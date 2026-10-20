// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/watermark"
)

func TestParseTrigger_Valid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		in     string
		schema string
		table  string
	}{
		{name: "plain", in: `{"table":"public.users"}`, schema: "public", table: "users"},
		{name: "dot in table name", in: `{"table":"app.v1.events"}`, schema: "app", table: "v1.events"},
		{name: "quoted characters", in: `{"table":"My Schema.\"Odd\" <name>"}`, schema: "My Schema", table: `"Odd" <name>`},
		{
			name: "63-byte names", in: `{"table":"` + strings.Repeat("s", 63) + "." + strings.Repeat("t", 63) + `"}`,
			schema: strings.Repeat("s", 63), table: strings.Repeat("t", 63),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := watermark.ParseTrigger([]byte(tc.in))
			require.NoError(t, err)
			schema, table := got.Split()
			assert.Equal(t, tc.schema, schema)
			assert.Equal(t, tc.table, table)
			assert.Equal(t, tc.schema+"."+tc.table, got.Table)
		})
	}
}

func TestParseTrigger_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ``},
		{name: "empty object", in: `{}`},
		{name: "no schema", in: `{"table":"users"}`},
		{name: "empty schema", in: `{"table":".users"}`},
		{name: "empty table", in: `{"table":"public."}`},
		{name: "null", in: `{"table":null}`},
		{name: "number", in: `{"table":1}`},
		{name: "extra member", in: `{"table":"public.users","run":1}`},
		{name: "duplicate", in: `{"table":"public.users","table":"public.orders"}`},
		{name: "control character", in: `{"table":"public.us\nrs"}`},
		{name: "delete character", in: `{"table":"public.us\u007frs"}`},
		{name: "name too long", in: `{"table":"public.` + strings.Repeat("t", 64) + `"}`},
		{name: "trailing data", in: `{"table":"public.users"} {}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := watermark.ParseTrigger([]byte(tc.in))
			require.ErrorIs(t, err, watermark.ErrInvalidTrigger)
		})
	}
}

func TestTrigger_MarshalJSON_RoundTrip(t *testing.T) {
	t.Parallel()
	in := watermark.Trigger{Table: `app.<"x">&y`}
	b, err := in.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(b), `<\"x\">&y`, "no HTML escaping")
	assert.JSONEq(t, `{"table":"app.<\"x\">&y"}`, string(b))
	viaStd, err := json.Marshal(in)
	require.NoError(t, err)
	assert.JSONEq(t, string(b), string(viaStd))
	got, err := watermark.ParseTrigger(b)
	require.NoError(t, err)
	assert.Equal(t, in, got)

	_, err = watermark.Trigger{Table: "nodot"}.MarshalJSON()
	require.ErrorIs(t, err, watermark.ErrInvalidTrigger)
}

func TestSplitTable_InvalidUTF8(t *testing.T) {
	t.Parallel()
	_, _, ok := watermark.SplitTable("public.\xff")
	assert.False(t, ok)
}
