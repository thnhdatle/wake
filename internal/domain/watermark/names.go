// SPDX-License-Identifier: Apache-2.0

package watermark

import (
	"strings"
	"unicode/utf8"
)

const maxIdentifierBytes = 63

const ulidLen = 26

const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

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
