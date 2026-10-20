// SPDX-License-Identifier: Apache-2.0

package pgoutput_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

// Relation OIDs of the row image fixtures.
const (
	oidDocs uint32 = 100 + iota
	oidFullPK
	oidFullNoPK
	oidIndex
	oidKeyless
	oidNothing
)

// imageDecoder returns a decoder that knows one relation per identity kind;
// the FULL table with primary key has its key set like the app would.
func imageDecoder(t *testing.T) *pgoutput.Decoder {
	t.Helper()
	dec := pgoutput.NewDecoder()
	three := func(keys ...bool) []schema.Column {
		return []schema.Column{
			col("id", schema.OIDInt4, keys[0]), col("title", schema.OIDText, keys[1]), col("body", schema.OIDText, keys[2]),
		}
	}
	mustDecode(t, dec,
		relationMsg(oidDocs, "docs", schema.IdentityDefault, three(true, false, false)...),
		relationMsg(oidFullPK, "docs_full", schema.IdentityFull, three(true, true, true)...),
		relationMsg(oidFullNoPK, "nopk_full", schema.IdentityFull, three(true, true, true)...),
		relationMsg(oidIndex, "uniq_idx", schema.IdentityIndex, three(true, true, false)...),
		relationMsg(oidKeyless, "keyless", schema.IdentityDefault, three(false, false, false)...),
		relationMsg(oidNothing, "nothing", schema.IdentityNothing, three(false, false, false)...),
	)
	require.NoError(t, dec.SetKeyColumns(oidFullPK, []string{"id"}))
	return dec
}

// rowJSON renders a row image like change.Event does ("null" for nil).
func rowJSON(r change.Row) string {
	if r == nil {
		return "null"
	}
	parts := make([]string, len(r))
	for i, c := range r {
		parts[i] = string(change.AppendString(nil, c.Name)) + ":" + string(c.Value)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func tup(cols ...pgoutput.TupleColumn) pgoutput.Tuple { return cols }

func TestDecoder_RowImages_Rules(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		msg       pgoutput.Message
		key       string
		before    string
		after     string
		unchanged []string
	}{
		{
			name: "insert", msg: &pgoutput.Insert{RelationOID: oidDocs, New: tup(txt("1"), txt("a"), null())},
			key: `{"id":1}`, before: "null", after: `{"id":1,"title":"a","body":null}`,
		},
		{
			name: "insert keyless", msg: &pgoutput.Insert{RelationOID: oidKeyless, New: tup(txt("1"), txt("k"), txt("x"))},
			key: "null", before: "null", after: `{"id":1,"title":"k","body":"x"}`,
		},
		{
			name: "update default toast omitted",
			msg:  &pgoutput.Update{RelationOID: oidDocs, New: tup(txt("1"), txt("b"), toast())},
			key:  `{"id":1}`, before: `{"id":1}`, after: `{"id":1,"title":"b"}`, unchanged: []string{"body"},
		},
		{
			name: "update default key change uses K",
			msg: &pgoutput.Update{
				RelationOID: oidDocs, OldKind: pgoutput.OldKey,
				Old: tup(txt("2"), null(), null()), New: tup(txt("3"), txt("t"), txt("small")),
			},
			key: `{"id":3}`, before: `{"id":2}`, after: `{"id":3,"title":"t","body":"small"}`,
		},
		{
			name: "update default toasted key taken from K",
			msg: &pgoutput.Update{
				RelationOID: oidDocs, OldKind: pgoutput.OldKey,
				Old: tup(txt("7"), null(), null()), New: tup(toast(), txt("t"), toast()),
			},
			key: `{"id":7}`, before: `{"id":7}`, after: `{"title":"t"}`, unchanged: []string{"id", "body"},
		},
		{
			name: "update full copies toast from before",
			msg: &pgoutput.Update{
				RelationOID: oidFullPK, OldKind: pgoutput.OldFull,
				Old: tup(txt("1"), txt("a"), txt("yyy")), New: tup(txt("1"), txt("b"), toast()),
			},
			key: `{"id":1}`, before: `{"id":1,"title":"a","body":"yyy"}`, after: `{"id":1,"title":"b","body":"yyy"}`,
		},
		{
			name: "update full without pk keys every column",
			msg: &pgoutput.Update{
				RelationOID: oidFullNoPK, OldKind: pgoutput.OldFull,
				Old: tup(txt("1"), txt("one"), null()), New: tup(txt("1"), txt("uno"), null()),
			},
			key:    `{"id":1,"title":"uno","body":null}`,
			before: `{"id":1,"title":"one","body":null}`, after: `{"id":1,"title":"uno","body":null}`,
		},
		{
			name: "update index identity unchanged key",
			msg:  &pgoutput.Update{RelationOID: oidIndex, New: tup(txt("1"), txt("2"), txt("d"))},
			key:  `{"id":1,"title":"2"}`, before: `{"id":1,"title":"2"}`, after: `{"id":1,"title":"2","body":"d"}`,
		},
		{
			name: "update index identity key change",
			msg: &pgoutput.Update{
				RelationOID: oidIndex, OldKind: pgoutput.OldKey,
				Old: tup(txt("1"), txt("2"), null()), New: tup(txt("1"), txt("3"), txt("d")),
			},
			key: `{"id":1,"title":"3"}`, before: `{"id":1,"title":"2"}`, after: `{"id":1,"title":"3","body":"d"}`,
		},
		{
			name: "update identity nothing",
			msg:  &pgoutput.Update{RelationOID: oidNothing, New: tup(txt("1"), txt("2"), txt("3"))},
			key:  "null", before: `{}`, after: `{"id":1,"title":"2","body":"3"}`,
		},
		{
			name: "delete default",
			msg:  &pgoutput.Delete{RelationOID: oidDocs, OldKind: pgoutput.OldKey, Old: tup(txt("3"), null(), null())},
			key:  `{"id":3}`, before: `{"id":3}`, after: "null",
		},
		{
			name: "delete full with pk",
			msg:  &pgoutput.Delete{RelationOID: oidFullPK, OldKind: pgoutput.OldFull, Old: tup(txt("1"), txt("b"), txt("y"))},
			key:  `{"id":1}`, before: `{"id":1,"title":"b","body":"y"}`, after: "null",
		},
		{
			name: "delete full without pk",
			msg:  &pgoutput.Delete{RelationOID: oidFullNoPK, OldKind: pgoutput.OldFull, Old: tup(txt("1"), txt("uno"), null())},
			key:  `{"id":1,"title":"uno","body":null}`, before: `{"id":1,"title":"uno","body":null}`, after: "null",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dec := imageDecoder(t)
			img, err := dec.RowImages(tc.msg)
			require.NoError(t, err)
			assert.Equal(t, tc.key, rowJSON(img.Key), "key")
			assert.Equal(t, tc.before, rowJSON(img.Before), "before")
			assert.Equal(t, tc.after, rowJSON(img.After), "after")
			assert.Equal(t, tc.unchanged, img.Unchanged, "unchanged")
			wantOp := map[pgoutput.MessageType]change.Op{
				pgoutput.MessageInsert: change.OpCreate,
				pgoutput.MessageUpdate: change.OpUpdate,
				pgoutput.MessageDelete: change.OpDelete,
			}[tc.msg.MessageType()]
			assert.Equal(t, wantOp, img.Op)
		})
	}
}

func TestDecoder_RowImages_Errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  pgoutput.Message
		err  error
	}{
		{
			name: "insert toast", err: pgoutput.ErrUnexpectedUnchanged,
			msg: &pgoutput.Insert{RelationOID: oidDocs, New: tup(txt("1"), toast(), null())},
		},
		{
			name: "update toasted key without K", err: pgoutput.ErrUnchangedKey,
			msg: &pgoutput.Update{RelationOID: oidDocs, New: tup(toast(), txt("b"), null())},
		},
		{
			name: "update full without old", err: pgoutput.ErrMissingOldTuple,
			msg: &pgoutput.Update{RelationOID: oidFullPK, New: tup(txt("1"), txt("b"), null())},
		},
		{
			name: "update full toast without old value",
			msg: &pgoutput.Update{
				RelationOID: oidFullPK, OldKind: pgoutput.OldFull,
				Old: tup(txt("1"), txt("a"), toast()), New: tup(txt("1"), txt("b"), toast()),
			},
			err: pgoutput.ErrUnexpectedUnchanged,
		},
		{name: "delete without old", msg: &pgoutput.Delete{RelationOID: oidDocs}, err: pgoutput.ErrMissingOldTuple},
		{
			name: "delete toasted key", err: pgoutput.ErrUnchangedKey,
			msg: &pgoutput.Delete{RelationOID: oidDocs, OldKind: pgoutput.OldKey, Old: tup(toast(), null(), null())},
		},
		{
			name: "column count", err: pgoutput.ErrColumnCount,
			msg: &pgoutput.Insert{RelationOID: oidDocs, New: tup(txt("1"))},
		},
		{
			name: "old column count", err: pgoutput.ErrColumnCount,
			msg: &pgoutput.Update{
				RelationOID: oidDocs, OldKind: pgoutput.OldKey, Old: tup(txt("1")), New: tup(txt("1"), null(), null()),
			},
		},
		{
			name: "delete column count", err: pgoutput.ErrColumnCount,
			msg: &pgoutput.Delete{RelationOID: oidDocs, OldKind: pgoutput.OldKey, Old: tup(txt("1"))},
		},
		{
			name: "update new column count", err: pgoutput.ErrColumnCount,
			msg: &pgoutput.Update{RelationOID: oidDocs, New: tup(txt("1"))},
		},
		{
			name: "binary", err: pgoutput.ErrBinaryValue,
			msg: &pgoutput.Insert{
				RelationOID: oidDocs, New: tup(pgoutput.TupleColumn{Kind: pgoutput.TupleBinary}, null(), null()),
			},
		},
		{
			name: "bad kind", err: pgoutput.ErrMalformed,
			msg: &pgoutput.Insert{RelationOID: oidDocs, New: tup(pgoutput.TupleColumn{Kind: 'z'}, null(), null())},
		},
		{
			name: "invalid value", err: schema.ErrInvalidValue,
			msg: &pgoutput.Insert{RelationOID: oidDocs, New: tup(txt("x"), null(), null())},
		},
		{
			name: "unknown relation", err: pgoutput.ErrUnknownRelation,
			msg: &pgoutput.Insert{RelationOID: 1, New: tup()},
		},
		{name: "not a row message", msg: &pgoutput.Begin{}, err: pgoutput.ErrNotRowMessage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dec := imageDecoder(t)
			_, err := dec.RowImages(tc.msg)
			require.ErrorIs(t, err, tc.err)
		})
	}
}

func TestDecoder_RowImages_ValuesDoNotAliasMessage(t *testing.T) {
	t.Parallel()
	dec := imageDecoder(t)
	data := encode(&pgoutput.Insert{RelationOID: oidDocs, New: tup(txt("1"), txt("abc"), null())})
	m, err := dec.Decode(data)
	require.NoError(t, err)
	img, err := dec.RowImages(m)
	require.NoError(t, err)
	for i := range data {
		data[i] = 0
	}
	assert.JSONEq(t, `{"id":1,"title":"abc","body":null}`, rowJSON(img.After))
}
