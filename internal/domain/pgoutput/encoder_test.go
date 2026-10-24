// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"encoding/binary"
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

const pgEpochUnixMs = 946_684_800_000

type wire []byte

func (w wire) u8(v byte) wire     { return append(w, v) }
func (w wire) u16(v uint16) wire  { return binary.BigEndian.AppendUint16(w, v) }
func (w wire) u32(v uint32) wire  { return binary.BigEndian.AppendUint32(w, v) }
func (w wire) u64(v uint64) wire  { return binary.BigEndian.AppendUint64(w, v) }
func (w wire) lsn(v lsn.LSN) wire { return w.u64(uint64(v)) }
func (w wire) cstr(s string) wire { return append(append(w, s...), 0) }

func (w wire) pgTime(tsMs int64) wire {
	return w.u64(uint64((tsMs - pgEpochUnixMs) * 1000)) //nolint:gosec // G115: wire Int64 as uint64
}

func (w wire) tuple(t pgoutput.Tuple) wire {
	w = w.u16(uint16(len(t))) //nolint:gosec // G115: test tuples are small
	for _, c := range t {
		w = w.u8(byte(c.Kind))
		if c.Kind == pgoutput.TupleText || c.Kind == pgoutput.TupleBinary {
			w = append(w.u32(uint32(len(c.Data))), c.Data...) //nolint:gosec // G115: test values are small
		}
	}
	return w
}

func (w wire) old(kind pgoutput.OldKind, t pgoutput.Tuple) wire {
	if kind == pgoutput.OldNone {
		return w
	}
	return w.u8(byte(kind)).tuple(t)
}

func encodeRelation(rel *schema.Relation) wire {
	w := wire{'R'}.u32(rel.OID).cstr(rel.Namespace).cstr(rel.Name).u8(byte(rel.Identity))
	w = w.u16(uint16(len(rel.Columns))) //nolint:gosec // G115: test relations are small
	for _, c := range rel.Columns {
		flags := byte(0)
		if c.Key {
			flags = 1
		}
		w = w.u8(flags).cstr(c.Name).u32(c.TypeOID).u32(uint32(c.TypeMod)) //nolint:gosec // G115: typmod bits
	}
	return w
}

func encode(m pgoutput.Message) []byte {
	switch m := m.(type) {
	case *pgoutput.Begin:
		return wire{'B'}.lsn(m.FinalLSN).pgTime(m.TsMs).u32(m.XID)
	case *pgoutput.Commit:
		return wire{'C'}.u8(m.Flags).lsn(m.CommitLSN).lsn(m.EndLSN).pgTime(m.TsMs)
	case *pgoutput.Origin:
		return wire{'O'}.lsn(m.CommitLSN).cstr(m.Name)
	case *pgoutput.Relation:
		return encodeRelation(&m.Relation)
	case *pgoutput.TypeMessage:
		return wire{'Y'}.u32(m.OID).cstr(m.Namespace).cstr(m.Name)
	case *pgoutput.Insert:
		return wire{'I'}.u32(m.RelationOID).u8('N').tuple(m.New)
	case *pgoutput.Update:
		return wire{'U'}.u32(m.RelationOID).old(m.OldKind, m.Old).u8('N').tuple(m.New)
	case *pgoutput.Delete:
		return wire{'D'}.u32(m.RelationOID).old(m.OldKind, m.Old)
	case *pgoutput.Truncate:
		w := wire{'T'}.u32(uint32(len(m.RelationOIDs))).u8(m.Options) //nolint:gosec // G115: small test lists
		for _, oid := range m.RelationOIDs {
			w = w.u32(oid)
		}
		return w
	case *pgoutput.LogicalMessage:
		w := wire{'M'}.u8(m.Flags).lsn(m.LSN).cstr(m.Prefix)
		return append(w.u32(uint32(len(m.Content))), m.Content...) //nolint:gosec // G115: small test content
	default:
		panic(fmt.Sprintf("encode: unexpected message %T", m))
	}
}

func txt(s string) pgoutput.TupleColumn {
	return pgoutput.TupleColumn{Kind: pgoutput.TupleText, Data: []byte(s)}
}

func null() pgoutput.TupleColumn { return pgoutput.TupleColumn{Kind: pgoutput.TupleNull} }

func toast() pgoutput.TupleColumn { return pgoutput.TupleColumn{Kind: pgoutput.TupleUnchanged} }

func col(name string, oid uint32, key bool) schema.Column {
	tn, _ := schema.TypeName(oid)
	return schema.Column{Name: name, TypeOID: oid, TypeMod: -1, TypeName: tn, Key: key}
}
