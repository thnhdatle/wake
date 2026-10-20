// SPDX-License-Identifier: Apache-2.0

package pgoutput

// OldKind tells which old tuple an Update or Delete carries.
type OldKind byte

// Old tuple kinds.
const (
	// OldNone means no old tuple: an Update under a DEFAULT or INDEX replica
	// identity whose key did not change.
	OldNone OldKind = 0
	// OldKey is a 'K' tuple: every relation column, with values only for the
	// replica identity columns (others are 'n').
	OldKey OldKind = 'K'
	// OldFull is an 'O' tuple: the whole old row (REPLICA IDENTITY FULL).
	OldFull OldKind = 'O'
)

// Update is a changed row. Old is set when OldKind is OldKey or OldFull.
type Update struct {
	RelationOID uint32
	OldKind     OldKind
	Old         Tuple
	New         Tuple
}

// MessageType implements Message.
func (*Update) MessageType() MessageType { return MessageUpdate }

func parseUpdate(r *reader) *Update {
	m := &Update{RelationOID: r.u32("relation oid")}
	tag := r.u8("tuple tag")
	if r.err != nil {
		return nil
	}
	if k := OldKind(tag); k == OldKey || k == OldFull {
		m.OldKind = k
		m.Old = parseTuple(r)
		tag = r.u8("new tuple tag")
	}
	if r.err == nil && tag != 'N' {
		r.fail("expected 'N' tuple, got %q", tag)
		return nil
	}
	m.New = parseTuple(r)
	return m
}
