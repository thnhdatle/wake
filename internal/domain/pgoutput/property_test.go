// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func genTuple(t *rapid.T, n int, label string) pgoutput.Tuple {
	tuple := make(pgoutput.Tuple, n)
	for i := range tuple {
		switch rapid.IntRange(0, 3).Draw(t, label+" kind") {
		case 0:
			tuple[i] = null()
		case 1:
			tuple[i] = toast()
		case 2:
			tuple[i] = pgoutput.TupleColumn{Kind: pgoutput.TupleBinary, Data: rapid.SliceOf(rapid.Byte()).Draw(t, label+" bin")}
		default:
			tuple[i] = pgoutput.TupleColumn{Kind: pgoutput.TupleText, Data: []byte(rapid.String().Draw(t, label+" text"))}
		}
	}
	return tuple
}

func genRelation(t *rapid.T) *pgoutput.Relation {
	n := rapid.IntRange(0, 6).Draw(t, "columns")
	rel := &pgoutput.Relation{Relation: schema.Relation{
		OID:       rapid.Uint32().Draw(t, "oid"),
		Namespace: rapid.StringMatching(`[a-z_]{0,8}`).Draw(t, "namespace"),
		Name:      rapid.StringMatching(`[a-z_]{1,8}`).Draw(t, "name"),
		Identity:  rapid.SampledFrom([]schema.ReplicaIdentity{'d', 'n', 'f', 'i'}).Draw(t, "identity"),
		Columns:   make([]schema.Column, n),
	}}
	for i := range rel.Columns {
		rel.Columns[i] = schema.Column{
			Name:    rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "column"),
			TypeOID: rapid.Uint32().Draw(t, "type"),
			TypeMod: rapid.Int32().Draw(t, "typmod"),
			Key:     rapid.Bool().Draw(t, "key"),
		}
	}
	return rel
}

func genMessage(t *rapid.T) pgoutput.Message {
	oid := rapid.Uint32().Draw(t, "relation oid")
	n := rapid.IntRange(0, 5).Draw(t, "tuple columns")
	switch rapid.IntRange(0, 9).Draw(t, "type") {
	case 0:
		return &pgoutput.Begin{
			FinalLSN: lsn.LSN(rapid.Uint64().Draw(t, "lsn")), XID: rapid.Uint32().Draw(t, "xid"),
			TsMs: rapid.Int64Range(-1e12, 1e13).Draw(t, "ts"),
		}
	case 1:
		return &pgoutput.Commit{
			Flags: rapid.Byte().Draw(t, "flags"), CommitLSN: lsn.LSN(rapid.Uint64().Draw(t, "commit")),
			EndLSN: lsn.LSN(rapid.Uint64().Draw(t, "end")), TsMs: rapid.Int64Range(-1e12, 1e13).Draw(t, "ts"),
		}
	case 2:
		return &pgoutput.Origin{
			CommitLSN: lsn.LSN(rapid.Uint64().Draw(t, "lsn")), Name: rapid.StringMatching(`[a-z]*`).Draw(t, "origin"),
		}
	case 3:
		return genRelation(t)
	case 4:
		return &pgoutput.TypeMessage{
			OID:       oid,
			Namespace: rapid.StringMatching(`[a-z]*`).Draw(t, "ns"),
			Name:      rapid.StringMatching(`[a-z]+`).Draw(t, "tn"),
		}
	case 5:
		return &pgoutput.Insert{RelationOID: oid, New: genTuple(t, n, "new")}
	case 6:
		u := &pgoutput.Update{RelationOID: oid, New: genTuple(t, n, "new")}
		kinds := []pgoutput.OldKind{pgoutput.OldNone, pgoutput.OldKey, pgoutput.OldFull}
		if k := rapid.SampledFrom(kinds).Draw(t, "old"); k != pgoutput.OldNone {
			u.OldKind, u.Old = k, genTuple(t, n, "old")
		}
		return u
	case 7:
		return &pgoutput.Delete{
			RelationOID: oid, OldKind: rapid.SampledFrom([]pgoutput.OldKind{pgoutput.OldKey, pgoutput.OldFull}).Draw(t, "old"),
			Old: genTuple(t, n, "old"),
		}
	case 8:
		return &pgoutput.Truncate{
			Options: rapid.Byte().Draw(t, "options"), RelationOIDs: rapid.SliceOf(rapid.Uint32()).Draw(t, "oids"),
		}
	default:
		return &pgoutput.LogicalMessage{
			Flags: rapid.Byte().Draw(t, "flags"), LSN: lsn.LSN(rapid.Uint64().Draw(t, "lsn")),
			Prefix: rapid.StringMatching(`[a-z.]*`).Draw(t, "prefix"), Content: rapid.SliceOf(rapid.Byte()).Draw(t, "content"),
		}
	}
}

// TestParse_EncodeDecodeRoundTrip: Parse inverts the protocol encoding for
// every message type, relation and tuple shape.
func TestParse_EncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		msg := genMessage(t)
		got, err := pgoutput.Parse(encode(msg))
		require.NoError(t, err)
		assert.Equal(t, normalize(msg), normalize(got))
	})
}

// normalize maps nil and empty byte slices and tuples to one form, which
// the wire encoding cannot distinguish.
func normalize(m pgoutput.Message) pgoutput.Message {
	fix := func(t pgoutput.Tuple) {
		for i := range t {
			if t[i].Data == nil && (t[i].Kind == pgoutput.TupleText || t[i].Kind == pgoutput.TupleBinary) {
				t[i].Data = []byte{}
			}
		}
	}
	switch m := m.(type) {
	case *pgoutput.Insert:
		fix(m.New)
	case *pgoutput.Update:
		fix(m.New)
		fix(m.Old)
	case *pgoutput.Delete:
		fix(m.Old)
	case *pgoutput.Truncate:
		if m.RelationOIDs == nil {
			m.RelationOIDs = []uint32{}
		}
	case *pgoutput.LogicalMessage:
		if m.Content == nil {
			m.Content = []byte{}
		}
	}
	return m
}

// TestParse_ArbitraryBytesNeverPanic: any input yields a message or a
// classified error.
func TestParse_ArbitraryBytesNeverPanic(t *testing.T) {
	t.Parallel()
	types := []byte("BCORYIUDTMSEcAbPKrpx")
	rapid.Check(t, func(t *rapid.T) {
		body := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(t, "body")
		data := append([]byte{rapid.SampledFrom(types).Draw(t, "type")}, body...)
		_, err := pgoutput.Parse(data)
		if err != nil {
			var ue *pgoutput.UnsupportedMessageError
			assert.True(t, errors.Is(err, pgoutput.ErrMalformed) || errors.As(err, &ue), "unclassified error %v", err)
		}
	})
}

// toastCase draws a relation of int4 columns with a consistent identity and
// an update whose tuples follow what Postgres can send for it.
func toastCase(t *rapid.T) (*pgoutput.Relation, *pgoutput.Update) {
	n := rapid.IntRange(1, 6).Draw(t, "columns")
	identity := rapid.SampledFrom([]schema.ReplicaIdentity{'d', 'f', 'i'}).Draw(t, "identity")
	rel := &pgoutput.Relation{Relation: schema.Relation{OID: 1, Namespace: "p", Name: "t", Identity: identity}}
	for i := range n {
		key := identity == schema.IdentityFull || (i == 0) || rapid.Bool().Draw(t, "key")
		rel.Columns = append(rel.Columns, col("c"+strconv.Itoa(i), schema.OIDInt4, key))
	}
	value := func(label string) pgoutput.TupleColumn {
		if rapid.Bool().Draw(t, label+" null") {
			return null()
		}
		return txt(strconv.Itoa(rapid.IntRange(-1000, 1000).Draw(t, label)))
	}
	up := &pgoutput.Update{RelationOID: 1, New: make(pgoutput.Tuple, n)}
	for i := range n {
		up.New[i] = value("new")
		if !rel.Columns[i].Key && rapid.Bool().Draw(t, "toast") {
			up.New[i] = toast()
		}
	}
	switch {
	case identity == schema.IdentityFull:
		up.OldKind, up.Old = pgoutput.OldFull, make(pgoutput.Tuple, n)
		for i := range n {
			up.Old[i] = value("old")
		}
	case rapid.Bool().Draw(t, "key changed"):
		up.OldKind, up.Old = pgoutput.OldKey, make(pgoutput.Tuple, n)
		for i := range n {
			up.Old[i] = null()
			if rel.Columns[i].Key {
				up.Old[i] = value("old key")
			}
		}
	}
	return rel, up
}

// TestDecoder_RowImages_ToastRules checks the TOAST rules: FULL
// never emits unchanged_columns; otherwise a column is never both in after
// and in unchanged_columns, and together they cover every column.
func TestDecoder_RowImages_ToastRules(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		rel, up := toastCase(t)
		dec := pgoutput.NewDecoder()
		_, err := dec.Decode(encode(rel))
		require.NoError(t, err)
		img, err := dec.RowImages(up)
		require.NoError(t, err)
		if rel.Identity == schema.IdentityFull {
			assert.Empty(t, img.Unchanged)
			assert.Len(t, img.After, len(rel.Columns))
			assert.Len(t, img.Before, len(rel.Columns))
		}
		covered := slices.Clone(img.Unchanged)
		for _, c := range img.After {
			assert.NotContains(t, img.Unchanged, c.Name, "column in after and unchanged_columns")
			covered = append(covered, c.Name)
		}
		assert.ElementsMatch(t, columnNames(rel), covered)
		keys, _ := dec.KeyColumns(1)
		assert.Equal(t, keys, rowNames(img.Key), "key holds exactly the key columns")
		if rel.Identity != schema.IdentityFull {
			assert.Equal(t, rel.KeyColumnNames(), rowNames(img.Before), "before holds the identity columns")
		}
	})
}

func columnNames(rel *pgoutput.Relation) []string {
	out := make([]string, len(rel.Columns))
	for i, c := range rel.Columns {
		out[i] = c.Name
	}
	return out
}

func rowNames(r change.Row) []string {
	if r == nil {
		return nil
	}
	out := make([]string, len(r))
	for i, c := range r {
		out[i] = c.Name
	}
	return out
}
