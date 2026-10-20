// SPDX-License-Identifier: Apache-2.0

package pgoutput

// Insert is a new row.
type Insert struct {
	RelationOID uint32
	New         Tuple
}

// MessageType implements Message.
func (*Insert) MessageType() MessageType { return MessageInsert }

func parseInsert(r *reader) *Insert {
	m := &Insert{RelationOID: r.u32("relation oid")}
	if tag := r.u8("new tuple tag"); r.err == nil && tag != 'N' {
		r.fail("expected 'N' tuple, got %q", tag)
		return nil
	}
	m.New = parseTuple(r)
	return m
}
