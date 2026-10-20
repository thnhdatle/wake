// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"strings"
	"unicode/utf8"
)

// maxIdentifierBytes is Postgres' NAMEDATALEN-1: longer names cannot exist.
const maxIdentifierBytes = 63

// ulidLen is the length of a ULID in Crockford base32.
const ulidLen = 26

// ulidAlphabet is Crockford's base32 alphabet (no I, L, O, U).
const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// validNonce reports whether s is a canonical (upper-case) ULID. The first
// character is at most '7' because a ULID holds 128 bits in 130.
func validNonce(s string) bool {
	if len(s) != ulidLen || s[0] > '7' {
		return false
	}
	for i := range len(s) {
		if strings.IndexByte(ulidAlphabet, s[i]) < 0 {
			return false
		}
	}
	return true
}

// SplitTable splits "<schema>.<table>" at the first dot. It reports false
// when either part is empty, longer than 63 bytes, not valid UTF-8 or
// contains a control character. Schema names containing a dot cannot be
// expressed in this form; table names may contain dots.
func SplitTable(qualified string) (schema, table string, ok bool) {
	schema, table, found := strings.Cut(qualified, ".")
	if !found || !validIdentifier(schema) || !validIdentifier(table) {
		return "", "", false
	}
	return schema, table, true
}

func validIdentifier(s string) bool {
	if s == "" || len(s) > maxIdentifierBytes || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7F {
			return false
		}
	}
	return true
}
