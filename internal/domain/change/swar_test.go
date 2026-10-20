// SPDX-License-Identifier: Apache-2.0

package change

import (
	"encoding/binary"
	"testing"
)

// checkPlainWord fails t when plainWord disagrees with the byte-by-byte
// test on w.
func checkPlainWord(t *testing.T, w [8]byte) {
	t.Helper()
	want := true
	for _, b := range w {
		want = want && plainByte(b)
	}
	if got := plainWord(binary.LittleEndian.Uint64(w[:])); got != want {
		t.Fatalf("% x: plainWord %v, want %v", w[:], got, want)
	}
}

// fill returns 8 copies of bg.
func fill(bg byte) [8]byte {
	var w [8]byte
	for i := range w {
		w[i] = bg
	}
	return w
}

// TestPlainWord_AdjacentPairs checks plainWord against the byte-by-byte
// test for every pair of byte values at every pair of adjacent positions
// in a plain background: borrows between bytes are where SWAR goes wrong.
func TestPlainWord_AdjacentPairs(t *testing.T) {
	t.Parallel()
	for p := range 7 {
		for b1 := range 256 {
			for b2 := range 256 {
				w := fill('a')
				w[p], w[p+1] = byte(b1), byte(b2)
				checkPlainWord(t, w)
			}
		}
	}
}

// TestPlainWord_SingleBytes checks every single byte at every position in
// backgrounds at the edges of the plain range.
func TestPlainWord_SingleBytes(t *testing.T) {
	t.Parallel()
	for _, bg := range []byte{' ', '!', '#', '[', ']', '~', 0x7f} {
		for p := range 8 {
			for b := range 256 {
				w := fill(bg)
				w[p] = byte(b)
				checkPlainWord(t, w)
			}
		}
	}
}

func TestPlainPrefix_StringAndBytesAgree(t *testing.T) {
	t.Parallel()
	cases := []string{"", "1234567", "12345678", "12345678\n", "1234567\n12345678", "0123456789abcdef0", "abcdefghé"}
	for _, s := range cases {
		if a, b := plainPrefix(s), plainPrefixBytes([]byte(s)); a != b || a%8 != 0 {
			t.Errorf("%q: plainPrefix %d, plainPrefixBytes %d", s, a, b)
		}
	}
}
