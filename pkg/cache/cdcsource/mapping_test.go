// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/event"
)

func mustMapping(t *testing.T, templates map[string][]string) *Mapping {
	t.Helper()
	m, err := NewMapping(templates)
	require.NoError(t, err)
	return m
}

func decode(t *testing.T, payload []byte) *event.Event {
	t.Helper()
	e, err := event.Decode(payload)
	require.NoError(t, err)
	return e
}

func TestNewMapping_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		templates map[string][]string
		target    error
		contains  []string
	}{
		{"nil", nil, ErrMapping, []string{"no tables"}},
		{"no dot", map[string][]string{"users": {"u:{id}"}}, ErrMapping, []string{`"users"`}},
		{"bad schema", map[string][]string{"pub lic.users": {"u:{id}"}}, ErrMapping, []string{"<schema>.<table>"}},
		{"bad table", map[string][]string{"public.us.ers": {"u:{id}"}}, ErrMapping, []string{"<schema>.<table>"}},
		{"empty list", map[string][]string{"public.users": {}}, ErrMapping, []string{"no key template"}},
		{"bad template", map[string][]string{"public.users": {"u:{id"}}, ErrTemplate, []string{"table public.users"}},
		{
			"all problems reported",
			map[string][]string{"a.b": {"{"}, "c.d": {"}"}, "x": nil},
			ErrTemplate,
			[]string{`"{"`, `"}"`, `table "x"`},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, err := NewMapping(tc.templates)

			require.ErrorIs(t, err, tc.target)
			assert.Nil(t, m)
			for _, s := range tc.contains {
				assert.Contains(t, err.Error(), s)
			}
		})
	}
}

func TestMapping_Accessors(t *testing.T) {
	t.Parallel()

	m := mustMapping(t, map[string][]string{
		"public.users": {"user:{id}", "user:email:{email}"},
		"app.orders":   {"order:{id}"},
	})

	assert.Equal(t, []string{"app.orders", "public.users"}, m.Tables())
	tpls := m.Templates("public.users")
	require.Len(t, tpls, 2)
	assert.Equal(t, "user:email:{email}", tpls[1].String())
	assert.Nil(t, m.Templates("public.nope"))
}

func TestMapping_Keys(t *testing.T) {
	t.Parallel()

	m := mustMapping(t, map[string][]string{
		"public.users":  {"user:{id}", "user:email:{email}"},
		"public.ledger": {"ledger:{id}:{amount}"},
		"public.multi":  {"m:{tenant}:{id}", "m:flag:{active}", "m:doc:{doc}"},
		"public.logs":   {"log:{msg}"},
	})
	tests := []struct {
		name    string
		payload []byte
		want    []string
	}{
		{
			"create renders the after image",
			evt("c", "public.users", `{"id":42}`, "null", `{"id":42,"email":"a@example.com"}`),
			[]string{"user:42", "user:email:a@example.com"},
		},
		{
			"update with full identity invalidates old and new unique values",
			evt("u", "public.users", `{"id":42}`, `{"id":42,"email":"a@example.com"}`, `{"id":42,"email":"b@example.com"}`),
			[]string{"user:42", "user:email:a@example.com", "user:email:b@example.com"},
		},
		{
			"update with default identity renders what the images carry",
			evt("u", "public.users", `{"id":42}`, `{"id":42}`, `{"id":42,"email":"b@example.com"}`),
			[]string{"user:42", "user:email:b@example.com"},
		},
		{
			"primary key change invalidates the old and the new key",
			evt("u", "public.users", `{"id":43}`, `{"id":42}`, `{"id":43,"email":"b@example.com"}`),
			[]string{"user:42", "user:43", "user:email:b@example.com"},
		},
		{
			"delete renders the before image",
			evt("d", "public.users", `{"id":43}`, `{"id":43,"email":"b@example.com"}`, "null"),
			[]string{"user:43", "user:email:b@example.com"},
		},
		{
			"delete with default identity resolves the key",
			evt("d", "public.users", `{"id":43}`, `{}`, "null"),
			[]string{"user:43"},
		},
		{
			"integers render exactly, also above 2^53",
			evt("c", "public.ledger", `{"id":9223372036854775807}`, "null",
				`{"id":9223372036854775807,"amount":9007199254740993}`),
			[]string{"ledger:9223372036854775807:9007199254740993"},
		},
		{
			"strings verbatim, booleans as text, null and objects skip",
			evt("c", "public.multi", `{"tenant":"a:b \"q\" ü","id":-1}`, "null",
				`{"tenant":"a:b \"q\" ü","id":-1,"active":true,"doc":{"x":1}}`),
			[]string{`m:a:b "q" ü:-1`, "m:flag:true"},
		},
		{
			"null value skips the rendering",
			evt("c", "public.users", `{"id":1}`, "null", `{"id":1,"email":null}`),
			[]string{"user:1"},
		},
		{
			"keyless table renders from the image",
			evt("c", "public.logs", "null", "null", `{"msg":"hello"}`),
			[]string{"log:hello"},
		},
		{
			"unmapped table",
			evt("c", "public.other", `{"id":1}`, "null", `{"id":1}`),
			nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := m.Keys(decode(t, tc.payload))

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMapping_MissingFromBefore(t *testing.T) {
	t.Parallel()

	m := mustMapping(t, map[string][]string{"public.users": {"user:{id}", "user:email:{email}", "user:{org}"}})
	tests := []struct {
		name    string
		payload []byte
		want    []string
	}{
		{"full before", evt("u", "public.users", `{"id":1}`, `{"id":1,"email":"a","org":null}`, `{"id":1}`), nil},
		{"key-only before", evt("d", "public.users", `{"id":1}`, `{"id":1}`, "null"), []string{"email", "org"}},
		{"keyless", evt("c", "public.users", "null", "null", `{"id":1}`), []string{"id", "email", "org"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, m.missingFromBefore(decode(t, tc.payload)))
		})
	}
}

func TestMapping_CheckReplicaIdentity(t *testing.T) {
	t.Parallel()

	m := mustMapping(t, map[string][]string{
		"public.users":  {"user:{id}", "user:email:{email}"},
		"public.orders": {"order:{id}", "order:ref:{ref}:{tenant}"},
		"public.items":  {"item:{sku}"},
	})
	users := TableInfo{Schema: "public", Table: "users", Columns: []string{"id", "email"}, IdentityColumns: []string{"id"}}
	orders := TableInfo{
		Schema: "public", Table: "orders", Columns: []string{"id", "ref", "tenant"},
		IdentityColumns: []string{"ref", "tenant"},
	}
	items := TableInfo{Schema: "public", Table: "items", Columns: []string{"sku"}, IdentityColumns: []string{"sku"}}

	t.Run("ok with full identity and key-only templates", func(t *testing.T) {
		t.Parallel()
		full := users
		full.IdentityFull = true
		orders := orders
		orders.IdentityFull = true

		require.NoError(t, m.CheckReplicaIdentity([]TableInfo{full, orders, items}))
	})

	t.Run("non-key columns without full identity", func(t *testing.T) {
		t.Parallel()

		err := m.CheckReplicaIdentity([]TableInfo{users, orders, items})

		require.ErrorIs(t, err, ErrRequiresFullIdentity)
		var ie *IdentityError
		require.ErrorAs(t, err, &ie)
		assert.Contains(t, err.Error(), "table public.orders: templates reference non-key columns [id]")
		assert.Contains(t, err.Error(), "table public.users: templates reference non-key columns [email]")
		assert.Contains(t, err.Error(), "cdcsource: template_requires_full_identity: ")
	})

	t.Run("unknown table and column", func(t *testing.T) {
		t.Parallel()
		short := items
		short.Columns = nil
		full := users
		full.IdentityFull = true

		err := m.CheckReplicaIdentity([]TableInfo{full, short})

		require.ErrorIs(t, err, ErrUnknownTable)
		require.ErrorIs(t, err, ErrUnknownColumn)
		require.NotErrorIs(t, err, ErrRequiresFullIdentity)
		assert.Contains(t, err.Error(), "table public.orders not found")
		assert.Contains(t, err.Error(), "table public.items has no published column [sku]")
	})
}

func TestErrorCode(t *testing.T) {
	t.Parallel()

	_, tplErr := ParseTemplate("{")
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			"identity wins in a joined error", errors.Join(ErrUnknownColumn, &IdentityError{Table: "a.b"}),
			CodeTemplateRequiresFullIdentity,
		},
		{"unknown table", fmt.Errorf("x: %w", ErrUnknownTable), CodeTemplateUnknownTable},
		{"unknown column", ErrUnknownColumn, CodeTemplateUnknownColumn},
		{"template", tplErr, CodeTemplateInvalid},
		{"mapping", ErrMapping, CodeMappingInvalid},
		{"config", invalidConfig("x"), CodeInvalidConfig},
		{"not running", ErrSourceNotRunning, CodeSourceNotRunning},
		{"foreign", errors.New("other"), ""},
		{"nil", nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, ErrorCode(tc.err))
		})
	}
}
