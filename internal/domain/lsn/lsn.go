// SPDX-License-Identifier: Apache-2.0

package lsn

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN is a Postgres log sequence number.
type LSN uint64

const InvalidLSN LSN = 0

const hexDigits = "0123456789ABCDEF"

// Parse parses an LSN written as two hexadecimal halves separated by a slash.
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

func (l LSN) String() string {
	var buf [17]byte
	return string(l.AppendText(buf[:0]))
}

func (l LSN) AppendText(dst []byte) []byte {
	dst = appendHex(dst, uint32(l>>32))
	dst = append(dst, '/')
	return appendHex(dst, uint32(l)) //nolint:gosec // G115: intentional truncation to the low half
}

func (l LSN) MarshalText() ([]byte, error) {
	return l.AppendText(nil), nil
}

func (l *LSN) UnmarshalText(b []byte) error {
	v, err := Parse(string(b))
	if err != nil {
		return err
	}
	*l = v
	return nil
}

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
