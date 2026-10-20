// SPDX-License-Identifier: Apache-2.0

package pgoutput

import "github.com/thnhdatle/wake/internal/domain/schema"

// minColumnBytes is the smallest encoding of a Relation column: flags, an
// empty name's NUL, type OID and type modifier.
const minColumnBytes = 1 + 1 + 4 + 4

// Relation describes a published table; Postgres sends it before the first
// row message of the relation in a session and again after the relation's
// definition changes. Column TypeName is resolved by Decoder.Decode; Parse
// leaves it empty.
type Relation struct {
	schema.Relation
}

// MessageType implements Message.
func (*Relation) MessageType() MessageType { return MessageRelation }

func parseRelation(r *reader) *Relation {
	m := &Relation{}
	m.OID = r.u32("relation oid")
	m.Namespace = r.cstring("namespace")
	m.Name = r.cstring("relation name")
	m.Identity = schema.ReplicaIdentity(r.u8("replica identity"))
	n := int(r.u16("column count"))
	if r.err != nil {
		return nil
	}
	if !m.Identity.Valid() {
		r.fail("invalid replica identity %q", byte(m.Identity))
		return nil
	}
	if n*minColumnBytes > r.remaining() {
		r.fail("column count %d exceeds the %d bytes left", n, r.remaining())
		return nil
	}
	m.Columns = make([]schema.Column, n)
	for i := range m.Columns {
		c := &m.Columns[i]
		c.Key = r.u8("column flags")&1 != 0
		c.Name = r.cstring("column name")
		c.TypeOID = r.u32("column type")
		c.TypeMod = int32(r.u32("column type modifier")) //nolint:gosec // G115: wire Int32 sent as uint32
	}
	return m
}
