// SPDX-License-Identifier: Apache-2.0

package pgoutput

type TupleKind byte

const (
	TupleNull      TupleKind = 'n'
	TupleUnchanged TupleKind = 'u'
	TupleText      TupleKind = 't'
	TupleBinary    TupleKind = 'b'
)

type TupleColumn struct {
	Kind TupleKind
	Data []byte
}

type Tuple []TupleColumn

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
