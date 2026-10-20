// SPDX-License-Identifier: Apache-2.0

package cdcsource

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Template is a parsed cache-key template such as "user:{id}" or
// "user:email:{email}".
//
// Syntax: literal text with placeholders. A placeholder is a column name in
// braces, "{column}"; the name is everything up to the next "}", must not be
// empty, must not contain "{" and must not start or end with white space.
// "{{" and "}}" outside a placeholder stand for a literal "{" and "}".
// Any other "{" or "}" is an error, so a template either parses unambiguously
// or is rejected with a *TemplateError. Column names are matched exactly
// (case-sensitive), as Postgres sends them.
//
// A template without placeholders is valid: it renders to the same key for
// every event of its table (a list or count key, for example).
type Template struct {
	raw   string
	parts []part
}

// part is either a literal or a column reference.
type part struct {
	text   string // literal text, or the column name when column is true
	column bool
}

// ParseTemplate parses a key template. Errors are *TemplateError values
// matching ErrTemplate.
func ParseTemplate(s string) (Template, error) {
	if s == "" {
		return Template{}, &TemplateError{Template: s, Reason: "empty template"}
	}
	var (
		parts []part
		lit   strings.Builder
	)
	flush := func() {
		if lit.Len() > 0 {
			parts = append(parts, part{text: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(s); {
		switch c := s[i]; {
		case c == '{' && i+1 < len(s) && s[i+1] == '{':
			lit.WriteByte('{')
			i += 2
		case c == '}' && i+1 < len(s) && s[i+1] == '}':
			lit.WriteByte('}')
			i += 2
		case c == '}':
			return Template{}, &TemplateError{Template: s, Offset: i, Reason: `unexpected "}" (write "}}" for a literal brace)`}
		case c == '{':
			name, next, err := placeholder(s, i)
			if err != nil {
				return Template{}, err
			}
			flush()
			parts = append(parts, part{text: name, column: true})
			i = next
		default:
			lit.WriteByte(c)
			i++
		}
	}
	flush()
	return Template{raw: s, parts: parts}, nil
}

// placeholder parses the placeholder opening at s[open] and returns the
// column name and the offset after the closing brace.
func placeholder(s string, open int) (name string, next int, err error) {
	end := strings.IndexAny(s[open+1:], "{}")
	if end < 0 || s[open+1+end] == '{' {
		return "", 0, &TemplateError{Template: s, Offset: open, Reason: "unclosed placeholder"}
	}
	name = s[open+1 : open+1+end]
	if name == "" {
		return "", 0, &TemplateError{Template: s, Offset: open, Reason: "empty placeholder"}
	}
	first, _ := utf8.DecodeRuneInString(name)
	last, _ := utf8.DecodeLastRuneInString(name)
	if unicode.IsSpace(first) || unicode.IsSpace(last) {
		return "", 0, &TemplateError{
			Template: s, Offset: open, Reason: "placeholder name has leading or trailing white space",
		}
	}
	return name, open + end + 2, nil
}

// String returns the template as it was parsed.
func (t Template) String() string { return t.raw }

// Columns returns the referenced column names in order of first appearance,
// without duplicates.
func (t Template) Columns() []string {
	var cols []string
	for _, p := range t.parts {
		if p.column && !slices.Contains(cols, p.text) {
			cols = append(cols, p.text)
		}
	}
	return cols
}

// render builds the key with lookup resolving each column to its text. ok is
// false when a column cannot be resolved (absent, null or not a scalar) or the
// key would be empty.
func (t Template) render(lookup func(col string) (string, bool)) (string, bool) {
	var b strings.Builder
	for _, p := range t.parts {
		if !p.column {
			b.WriteString(p.text)
			continue
		}
		v, ok := lookup(p.text)
		if !ok {
			return "", false
		}
		b.WriteString(v)
	}
	if b.Len() == 0 {
		return "", false
	}
	return b.String(), true
}
