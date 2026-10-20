// SPDX-License-Identifier: Apache-2.0

package lsn

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN is a position in the Postgres write-ahead log. LSNs compare
// numerically; the zero value is InvalidLSN.
type LSN uint64

// InvalidLSN is the zero position, which Postgres never assigns to a record.
const InvalidLSN LSN = 0

const hexDigits = "0123456789ABCDEF"

// Parse reads an LSN in the "X/X" form (upper- or lower-case hexadecimal,
// each half at most 8 digits).
func Parse(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok || hi == "" || lo == "" || len(hi) > 8 || len(lo) > 8 {
		return InvalidLSN, fmt.Errorf("%w: %q", ErrInvalid, s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return InvalidLSN, fmt.Errorf("%w: %q", ErrInvalid, s)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return InvalidLSN, fmt.Errorf("%w: %q", ErrInvalid, s)
	}
	return LSN(h<<32 | l), nil
}

// String formats the LSN as "X/X" with upper-case digits and no padding.
func (l LSN) String() string {
	var buf [17]byte
	return string(l.AppendText(buf[:0]))
}

// AppendText appends the "X/X" form of the LSN to dst without allocating
// beyond dst's growth.
func (l LSN) AppendText(dst []byte) []byte {
	dst = appendHex(dst, uint32(l>>32))
	dst = append(dst, '/')
	return appendHex(dst, uint32(l)) //nolint:gosec // G115: intentional truncation to the low half
}

// MarshalText implements encoding.TextMarshaler.
func (l LSN) MarshalText() ([]byte, error) {
	return l.AppendText(nil), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (l *LSN) UnmarshalText(b []byte) error {
	v, err := Parse(string(b))
	if err != nil {
		return err
	}
	*l = v
	return nil
}

// Compare returns -1, 0 or +1 when a is before, equal to or after b.
func Compare(a, b LSN) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func appendHex(dst []byte, v uint32) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	var buf [8]byte
	i := len(buf)
	for v != 0 {
		i--
		buf[i] = hexDigits[v&0xF]
		v >>= 4
	}
	return append(dst, buf[i:]...)
}
