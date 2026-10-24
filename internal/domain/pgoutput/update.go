// SPDX-License-Identifier: Apache-2.0

package pgoutput

type OldKind byte

const (
	OldNone OldKind = 0
	OldKey  OldKind = 'K'
	OldFull OldKind = 'O'
)

type Update struct {
	RelationOID uint32
	OldKind     OldKind
	Old         Tuple
	New         Tuple
}

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
