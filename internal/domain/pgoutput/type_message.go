// SPDX-License-Identifier: Apache-2.0

package pgoutput

type TypeMessage struct {
	OID       uint32
	Namespace string
	Name      string
}

func (*TypeMessage) MessageType() MessageType { return MessageTypeDef }

func parseType(r *reader) *TypeMessage {
	m := &TypeMessage{OID: r.u32("type oid")}
	m.Namespace = r.cstring("type namespace")
	m.Name = r.cstring("type name")
	return m
}
