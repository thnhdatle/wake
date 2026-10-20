// SPDX-License-Identifier: Apache-2.0

package pgoutput

// TypeMessage describes a non-built-in type used by a following Relation
// message (pgoutput sends it for types with OIDs at or above
// FirstGenbkiObjectId).
type TypeMessage struct {
	OID uint32
	// Namespace is the type's schema; "" for pg_catalog.
	Namespace string
	Name      string
}

// MessageType implements Message.
func (*TypeMessage) MessageType() MessageType { return MessageTypeDef }

func parseType(r *reader) *TypeMessage {
	m := &TypeMessage{OID: r.u32("type oid")}
	m.Namespace = r.cstring("type namespace")
	m.Name = r.cstring("type name")
	return m
}
