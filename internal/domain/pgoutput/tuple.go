// SPDX-License-Identifier: Apache-2.0

package pgoutput

// TupleKind is the kind byte of one TupleData column.
type TupleKind byte

// Tuple column kinds.
const (
	// TupleNull is SQL NULL.
	TupleNull TupleKind = 'n'
	// TupleUnchanged is an unchanged TOAST value that Postgres did not send.
	TupleUnchanged TupleKind = 'u'
	// TupleText is a value in its type's text output format.
	TupleText TupleKind = 't'
	// TupleBinary is a value in binary format (only with the binary option,
	// which Wake does not request; RowImages rejects it).
	TupleBinary TupleKind = 'b'
)

// TupleColumn is one column of a TupleData. Data is set for TupleText and
// TupleBinary and aliases the buffer passed to Parse or Decode.
type TupleColumn struct {
	Kind TupleKind
	Data []byte
}

// Tuple is a TupleData: one entry per relation column, in column order.
type Tuple []TupleColumn

// parseTuple reads TupleData. The column count is checked against the bytes
// left (each column takes at least one byte) before allocating.
func parseTuple(r *reader) Tuple {
	n := int(r.u16("tuple column count"))
	if r.err != nil {
		return nil
	}
	if n > r.remaining() {
		r.fail("tuple column count %d exceeds the %d bytes left", n, r.remaining())
		return nil
	}
	t := make(Tuple, n)
	for i := range t {
		kind := TupleKind(r.u8("tuple column kind"))
		switch kind {
		case TupleNull, TupleUnchanged:
		case TupleText, TupleBinary:
			size := int32(r.u32("tuple column length")) //nolint:gosec // G115: wire Int32 sent as uint32
			if size < 0 {
				r.fail("negative tuple column length %d", size)
				return nil
			}
			t[i].Data = r.take(int(size), "tuple column data")
		default:
			r.fail("unknown tuple column kind %q", byte(kind))
		}
		if r.err != nil {
			return nil
		}
		t[i].Kind = kind
	}
	return t
}
