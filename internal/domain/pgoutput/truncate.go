// SPDX-License-Identifier: Apache-2.0

package pgoutput

// Truncate option bits.
const (
	TruncateCascade         uint8 = 1
	TruncateRestartIdentity uint8 = 2
)

// Truncate empties one or more relations in one statement.
type Truncate struct {
	Options      uint8
	RelationOIDs []uint32
}

// MessageType implements Message.
func (*Truncate) MessageType() MessageType { return MessageTruncate }

// Cascade reports TRUNCATE ... CASCADE.
func (m *Truncate) Cascade() bool { return m.Options&TruncateCascade != 0 }

// RestartIdentity reports TRUNCATE ... RESTART IDENTITY.
func (m *Truncate) RestartIdentity() bool { return m.Options&TruncateRestartIdentity != 0 }

func parseTruncate(r *reader) *Truncate {
	n := int(r.u32("relation count"))
	m := &Truncate{Options: r.u8("options")}
	if r.err != nil {
		return nil
	}
	if n > r.remaining()/4 {
		r.fail("relation count %d exceeds the %d bytes left", n, r.remaining())
		return nil
	}
	m.RelationOIDs = make([]uint32, n)
	for i := range m.RelationOIDs {
		m.RelationOIDs[i] = r.u32("relation oid")
	}
	return m
}
