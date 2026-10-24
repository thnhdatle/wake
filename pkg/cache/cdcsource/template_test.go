// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTemplate_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		columns []string
		render  string
	}{
		{"single placeholder", "user:{id}", []string{"id"}, "user:<id>"},
		{"two placeholders", "user:{tenant}:{id}", []string{"tenant", "id"}, "user:<tenant>:<id>"},
		{"repeated column", "{id}-{id}", []string{"id"}, "<id>-<id>"},
		{"no placeholder", "users:all", nil, "users:all"},
		{"escaped braces", "a{{b}}c", nil, "a{b}c"},
		{"escape around placeholder", "{{{id}}}", []string{"id"}, "{<id>}"},
		{"placeholder only", "{email}", []string{"email"}, "<email>"},
		{"column with inner space and unicode", "k:{first name}:{ñ}", []string{"first name", "ñ"}, "k:<first name>:<ñ>"},
		{"adjacent placeholders", "{a}{b}", []string{"a", "b"}, "<a><b>"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tpl, err := ParseTemplate(tc.in)

			require.NoError(t, err)
			assert.Equal(t, tc.in, tpl.String())
			assert.Equal(t, tc.columns, tpl.Columns())
			got, ok := tpl.render(func(c string) (string, bool) { return "<" + c + ">", true })
			assert.True(t, ok)
			assert.Equal(t, tc.render, got)
		})
	}
}

func TestParseTemplate_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     string
		offset int
		reason string
	}{
		{"empty", "", 0, "empty template"},
		{"unclosed", "user:{id", 5, "unclosed placeholder"},
		{"nested open", "user:{a{b}", 5, "unclosed placeholder"},
		{"empty placeholder", "user:{}", 5, "empty placeholder"},
		{"stray close", "user:}", 5, `unexpected "}"`},
		{"stray close after placeholder", "{id}}", 4, `unexpected "}"`},
		{"leading space", "{ id}", 0, "white space"},
		{"trailing space", "x{id }", 1, "white space"},
		{"lone open at end", "user{", 4, "unclosed placeholder"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseTemplate(tc.in)

			require.ErrorIs(t, err, ErrTemplate)
			var te *TemplateError
			require.ErrorAs(t, err, &te)
			assert.Equal(t, tc.in, te.Template)
			assert.Equal(t, tc.offset, te.Offset)
			assert.Contains(t, te.Reason, tc.reason)
			assert.Contains(t, err.Error(), "cdcsource: template_invalid: ")
			assert.NotErrorIs(t, err, ErrMapping)
		})
	}
}

func TestTemplate_Render_Unresolved(t *testing.T) {
	t.Parallel()

	tpl, err := ParseTemplate("user:{id}:{email}")
	require.NoError(t, err)
	empty, err := ParseTemplate("{id}")
	require.NoError(t, err)

	_, ok := tpl.render(func(c string) (string, bool) { return "1", c == "id" })
	assert.False(t, ok, "a missing column skips the rendering")
	_, ok = empty.render(func(string) (string, bool) { return "", true })
	assert.False(t, ok, "an empty key is never rendered")
}

func escape(s string) string {
	return strings.NewReplacer("{", "{{", "}", "}}").Replace(s)
}

func FuzzParseTemplate(f *testing.F) {
	for _, s := range []string{
		"user:{id}", "user:email:{email}", "a{{b}}c", "{{{id}}}", "{", "}", "{}", "{ a}", "x{a{b}}", "",
		"{a}{b}", "k:{ñ}", "{{", "}}",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		tpl, err := ParseTemplate(s)
		if err != nil {
			var te *TemplateError
			require.ErrorAs(t, err, &te)
			require.GreaterOrEqual(t, te.Offset, 0)
			require.LessOrEqual(t, te.Offset, len(s))
			return
		}
		require.Equal(t, s, tpl.String())
		var rebuilt strings.Builder
		for _, p := range tpl.parts {
			if p.column {
				require.NotEmpty(t, p.text)
				require.NotContains(t, p.text, "{")
				require.NotContains(t, p.text, "}")
				rebuilt.WriteString("{" + p.text + "}")
				continue
			}
			rebuilt.WriteString(escape(p.text))
		}
		require.Equal(t, s, rebuilt.String(), "parts must reproduce the template")
	})
}
