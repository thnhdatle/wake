// SPDX-License-Identifier: Apache-2.0

package pgoutput

// Delete is a removed row; Old is the 'K' (replica identity) or 'O' (whole
// row, REPLICA IDENTITY FULL) tuple.
type Delete struct {
	RelationOID uint32
	OldKind     OldKind
	Old         Tuple
}

// MessageType implements Message.
func (*Delete) MessageType() MessageType { return MessageDelete }

func parseDelete(r *reader) *Delete {
	m := &Delete{RelationOID: r.u32("relation oid")}
	tag := OldKind(r.u8("old tuple tag"))
	if r.err != nil {
		return nil
	}
	if tag != OldKey && tag != OldFull {
		r.fail("expected 'K' or 'O' tuple, got %q", byte(tag))
		return nil
	}
	m.OldKind = tag
	m.Old = parseTuple(r)
	return m
}
