// SPDX-License-Identifier: Apache-2.0

package pgoutput

const (
	TruncateCascade         uint8 = 1
	TruncateRestartIdentity uint8 = 2
)

type Truncate struct {
	Options      uint8
	RelationOIDs []uint32
}

func (*Truncate) MessageType() MessageType { return MessageTruncate }

func (m *Truncate) Cascade() bool { return m.Options&TruncateCascade != 0 }

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
