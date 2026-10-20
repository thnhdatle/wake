// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// reader decodes big-endian protocol fields from one message. The first
// failure is kept in err; later reads return zero values, so parsers check
// err once at the end.
type reader struct {
	buf []byte
	off int
	typ byte
	err error
}

func (r *reader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %q message at offset %d: %s", ErrMalformed, r.typ, r.off, fmt.Sprintf(format, args...))
	}
}

func (r *reader) remaining() int { return len(r.buf) - r.off }

func (r *reader) take(n int, what string) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > r.remaining() {
		r.fail("%s: need %d bytes, have %d", what, n, r.remaining())
		return nil
	}
	b := r.buf[r.off : r.off+n : r.off+n]
	r.off += n
	return b
}

func (r *reader) u8(what string) byte {
	if b := r.take(1, what); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u16(what string) uint16 {
	if b := r.take(2, what); b != nil {
		return binary.BigEndian.Uint16(b)
	}
	return 0
}

func (r *reader) u32(what string) uint32 {
	if b := r.take(4, what); b != nil {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

func (r *reader) u64(what string) uint64 {
	if b := r.take(8, what); b != nil {
		return binary.BigEndian.Uint64(b)
	}
	return 0
}

// cstring reads a NUL-terminated string.
func (r *reader) cstring(what string) string {
	if r.err != nil {
		return ""
	}
	i := bytes.IndexByte(r.buf[r.off:], 0)
	if i < 0 {
		r.fail("%s: missing NUL terminator", what)
		return ""
	}
	s := string(r.buf[r.off : r.off+i])
	r.off += i + 1
	return s
}

// finish reports trailing bytes as malformed and returns the first error.
func (r *reader) finish() error {
	if r.err == nil && r.remaining() > 0 {
		r.fail("%d trailing bytes", r.remaining())
	}
	return r.err
}
