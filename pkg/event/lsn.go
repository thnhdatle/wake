// SPDX-License-Identifier: Apache-2.0

package event

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseLSN parses a CDC event LSN such as "0/16B3748".
func ParseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("%w: lsn %q has no slash", ErrInvalid, s)
	}
	h, err := parseHex32(hi)
	if err != nil {
		return 0, fmt.Errorf("%w: lsn %q: %w", ErrInvalid, s, err)
	}
	l, err := parseHex32(lo)
	if err != nil {
		return 0, fmt.Errorf("%w: lsn %q: %w", ErrInvalid, s, err)
	}
	return h<<32 | l, nil
}

// FormatLSN renders an LSN as upper-case hexadecimal "X/X".
func FormatLSN(lsn uint64) string {
	return strings.ToUpper(strconv.FormatUint(lsn>>32, 16)) + "/" +
		strings.ToUpper(strconv.FormatUint(lsn&0xFFFFFFFF, 16))
}

func parseHex32(s string) (uint64, error) {
	if s == "" || len(s) > 8 {
		return 0, fmt.Errorf("hex part %q must have 1 to 8 digits", s)
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'A' || c > 'F') {
			return 0, fmt.Errorf("hex part %q has a non upper-case-hex digit", s)
		}
	}
	n, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("hex part %q: %w", s, err)
	}
	return n, nil
}
