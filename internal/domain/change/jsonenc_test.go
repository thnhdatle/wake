// SPDX-License-Identifier: Apache-2.0

package change_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
)

func stdEncode(t require.TestingT, v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	require.NoError(t, enc.Encode(v))
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

func TestAppendString_MatchesEncodingJSON(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.OneOf(
			rapid.String(),
			rapid.StringOf(rapid.SampledFrom([]rune{
				'<', '>', '&', '"', '\\', '\n', '\r', '\t', '\b', '\f', 0, 0x1f, 0x7f, '\u2028', '\u2029', 'é', '😀',
			})),
			rapid.Custom(func(t *rapid.T) string { return string(rapid.SliceOf(rapid.Byte()).Draw(t, "raw")) }),
		).Draw(t, "s")
		assert.Equal(t, string(stdEncode(t, s)), string(change.AppendString(nil, s)))
	})
}

func TestAppendString_LongRunsMatchEncodingJSON(t *testing.T) {
	t.Parallel()
	plain := rapid.SampledFrom([]byte("aZ09 ~!#$%&'()*+,-./:;<=>?@[]^_`{|}\x7f"))
	special := rapid.SampledFrom([]byte{0, 0x01, 0x1f, '"', '\\', '\n', 0x80, 0xc3, 0xe2, 0xff})
	rapid.Check(t, func(t *rapid.T) {
		b := rapid.SliceOfN(plain, 0, 40).Draw(t, "plain")
		for i, n := 0, rapid.IntRange(0, 3).Draw(t, "specials"); i < n; i++ {
			at := rapid.IntRange(0, len(b)).Draw(t, "at")
			b = append(b[:at], append([]byte{special.Draw(t, "special")}, b[at:]...)...)
		}
		s := string(b)
		want := string(stdEncode(t, s))
		assert.Equal(t, want, string(change.AppendString(nil, s)))
		assert.Equal(t, want, string(change.AppendStringBytes([]byte("x"), b))[1:])
	})
}

func TestAppendStringBytes_Cases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: `""`},
		{name: "short plain", in: "abc", want: `"abc"`},
		{name: "two plain blocks", in: "0123456789abcdef", want: `"0123456789abcdef"`},
		{name: "quote in second block", in: `01234567"9`, want: `"01234567\"9"`},
		{name: "control in tail", in: "0123456789\x01", want: `"0123456789\u0001"`},
		{name: "invalid utf-8", in: "01234567\xff", want: "\"01234567\ufffd\""},
		{name: "line separator", in: "01234567\u2028", want: `"01234567\u2028"`},
		{name: "del stays literal", in: "0123456\x7f", want: "\"0123456\x7f\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, string(change.AppendStringBytes(nil, []byte(tc.in))))
			assert.Equal(t, tc.want, string(change.AppendString(nil, tc.in)))
		})
	}
}

func TestAppendString_NoHTMLEscaping(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `"<a>&b"`, string(change.AppendString(nil, "<a>&b")))
}

func TestAppendCanonicalKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  change.Row
		want string
	}{
		{name: "nil", key: nil, want: `null`},
		{name: "single", key: change.Row{{Name: "id", Value: []byte(`42`)}}, want: `{"id":42}`},
		{
			name: "sorted by code point",
			key: change.Row{
				{Name: "b", Value: []byte(`"x"`)},
				{Name: "a", Value: []byte(`1`)},
				{Name: "B", Value: []byte(`2`)},
				{Name: "é", Value: []byte(`3`)},
			},
			want: `{"B":2,"a":1,"b":"x","é":3}`,
		},
		{name: "html literal", key: change.Row{{Name: "k", Value: []byte(`"<&>"`)}}, want: `{"k":"<&>"}`},
		{
			name: "int8 exact",
			key:  change.Row{{Name: "id", Value: []byte(`9223372036854775807`)}},
			want: `{"id":9223372036854775807}`,
		},
		{
			name: "nested object sorted and compacted",
			key:  change.Row{{Name: "j", Value: []byte(`{"z": 1, "a": [ {"y":true,"b":null} ], "m":"<"}`)}},
			want: `{"j":{"a":[{"b":null,"y":true}],"m":"<","z":1}}`,
		},
		{name: "escaped string normalised", key: change.Row{{Name: "s", Value: []byte(`"A\/"`)}}, want: `{"s":"A/"}`},
		{name: "number with spaces", key: change.Row{{Name: "n", Value: []byte(` 1 `)}}, want: `{"n":1}`},
		{name: "invalid value verbatim", key: change.Row{{Name: "n", Value: []byte(`{`)}}, want: `{"n":{}`},
		{name: "empty value verbatim", key: change.Row{{Name: "n", Value: nil}}, want: `{"n":}`},
		{
			name: "booleans",
			key:  change.Row{{Name: "t", Value: []byte(`true`)}, {Name: "f", Value: []byte(`false`)}},
			want: `{"f":false,"t":true}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, string(change.AppendCanonicalKey(nil, tc.key)))
		})
	}
}

func TestAppendCanonicalKey_ManyColumns(t *testing.T) {
	t.Parallel()
	key := make(change.Row, 0, 12)
	for _, n := range strings.Split("l k j i h g f e d c b a", " ") {
		key = append(key, change.Column{Name: n, Value: []byte(`0`)})
	}
	assert.Equal(t, `{"a":0,"b":0,"c":0,"d":0,"e":0,"f":0,"g":0,"h":0,"i":0,"j":0,"k":0,"l":0}`,
		string(change.AppendCanonicalKey(nil, key)))
}

func TestAppendCanonicalKey_IndependentOfColumnOrder(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		names := rapid.SliceOfNDistinct(rapid.String(), 1, 6, func(s string) string { return s }).Draw(t, "names")
		key := make(change.Row, len(names))
		for i, n := range names {
			key[i] = change.Column{Name: n, Value: stdEncode(t, rapid.Int64().Draw(t, "v"))}
		}
		perm := rapid.Permutation(key).Draw(t, "perm")
		got := change.AppendCanonicalKey(nil, key)
		assert.Equal(t, string(got), string(change.AppendCanonicalKey(nil, perm)))
		var m map[string]json.Number
		dec := json.NewDecoder(bytes.NewReader(got))
		dec.UseNumber()
		require.NoError(t, dec.Decode(&m))
		assert.Len(t, m, len(names))
	})
}

func TestCanonicalValue(t *testing.T) {
	t.Parallel()
	got, err := change.CanonicalValue([]byte(`{"b":1,"a":2}`))
	require.NoError(t, err)
	assert.Equal(t, `{"a":2,"b":1}`, string(got))
	_, err = change.CanonicalValue([]byte(`{`))
	require.ErrorIs(t, err, change.ErrInvalidJSON)
}

func TestRow_Lookup(t *testing.T) {
	t.Parallel()
	r := change.Row{{Name: "id", Value: []byte(`1`)}}
	v, ok := r.Lookup("id")
	assert.True(t, ok)
	assert.Equal(t, `1`, string(v))
	_, ok = r.Lookup("x")
	assert.False(t, ok)
}

func TestValidIdentifier(t *testing.T) {
	t.Parallel()
	assert.True(t, change.ValidIdentifier("public"))
	assert.True(t, change.ValidIdentifier("a-B_9"))
	assert.False(t, change.ValidIdentifier(""))
	assert.False(t, change.ValidIdentifier("my table"))
	assert.False(t, change.ValidIdentifier("a.b"))
	assert.Equal(t, "public.users", change.Source{Schema: "public", Table: "users"}.QualifiedName())
}

type fixture struct {
	ID            string          `json:"id"`
	Source        sourceJSON      `json:"source"`
	Op            string          `json:"op"`
	TsMs          int64           `json:"ts_ms"`
	TxID          uint32          `json:"tx_id"`
	Key           json.RawMessage `json:"key"`
	Before        json.RawMessage `json:"before"`
	After         json.RawMessage `json:"after"`
	Unchanged     []string        `json:"unchanged_columns"`
	SchemaVersion uint64          `json:"schema_version"`
	Type          string          `json:"type"`
	KafkaRecord   json.RawMessage `json:"kafka_record"`
	ColsBefore    []colJSON       `json:"columns_before"`
	ColsAfter     []colJSON       `json:"columns_after"`
}

type sourceJSON struct {
	DB     string `json:"db"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

type colJSON struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func orderedRow(t *testing.T, raw json.RawMessage) change.Row {
	t.Helper()
	if string(raw) == "null" || raw == nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	require.NoError(t, err)
	require.Equal(t, json.Delim('{'), tok)
	row := change.Row{}
	for dec.More() {
		name, err := dec.Token()
		require.NoError(t, err)
		var v json.RawMessage
		require.NoError(t, dec.Decode(&v))
		row = append(row, change.Column{Name: name.(string), Value: v}) //nolint:forcetypeassert // object keys are strings
	}
	return row
}

func TestEvent_AppendJSON_ReproducesContractFixtures(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("../../../testdata/contracts/cdc/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile(f) //nolint:gosec // G304: fixture path from a fixed glob
			require.NoError(t, err)
			raw = bytes.TrimSpace(raw)
			var fx fixture
			require.NoError(t, json.Unmarshal(raw, &fx))
			if fx.KafkaRecord != nil {
				return
			}
			id, err := change.ParseID(fx.ID)
			require.NoError(t, err)
			src := change.Source{DB: fx.Source.DB, Schema: fx.Source.Schema, Table: fx.Source.Table}
			if fx.Type == "schema_change" {
				sc := change.SchemaChange{ID: id, Source: src, TsMs: fx.TsMs, SchemaVersion: fx.SchemaVersion}
				for _, c := range fx.ColsBefore {
					sc.ColumnsBefore = append(sc.ColumnsBefore, change.ColumnType{Name: c.Name, Type: c.Type})
				}
				for _, c := range fx.ColsAfter {
					sc.ColumnsAfter = append(sc.ColumnsAfter, change.ColumnType{Name: c.Name, Type: c.Type})
				}
				assert.Equal(t, string(raw), string(sc.AppendJSON(nil)))
				return
			}
			ev := change.Event{
				ID: id, Source: src, Op: change.Op(fx.Op), TsMs: fx.TsMs, TxID: fx.TxID,
				Key: orderedRow(t, fx.Key), Before: orderedRow(t, fx.Before), After: orderedRow(t, fx.After),
				Unchanged: fx.Unchanged, SchemaVersion: fx.SchemaVersion,
			}
			assert.Equal(t, string(raw), string(ev.AppendJSON(nil)))
			assert.Equal(t, id.LSN, ev.LSN())
			assert.Equal(t, id.Seq(), ev.Seq())
		})
	}
}

func TestSchemaChange_AppendJSON_NullBeforeEmptyAfter(t *testing.T) {
	t.Parallel()
	sc := change.SchemaChange{ID: change.SchemaID(1, 0), SchemaVersion: 1}
	assert.JSONEq(t,
		`{"type":"schema_change","id":"0/1-s0","source":{"db":"","schema":"","table":""},"lsn":"0/1",`+
			`"ts_ms":0,"schema_version":1,"columns_before":null,"columns_after":[]}`,
		string(sc.AppendJSON(nil)))
}

func TestMessage_AppendJSON(t *testing.T) {
	t.Parallel()
	m := change.Message{
		ID: change.MessageID(0x10, 2), TsMs: 5, Transactional: true, Prefix: "app.<x>", Content: []byte("hi"),
	}
	got := string(m.AppendJSON(nil))
	assert.JSONEq(t, `{"type":"message","id":"0/10-m2","lsn":"0/10","ts_ms":5,"transactional":true,`+
		`"prefix":"app.<x>","content_base64":"aGk="}`, got)
	assert.Contains(t, got, `"prefix":"app.<x>"`, "no HTML escaping")
}

func TestEvent_AppendJSON_EmptyRowIsObject(t *testing.T) {
	t.Parallel()
	ev := change.Event{
		ID: change.StreamID(1, 0), Op: change.OpUpdate, Before: change.Row{}, After: change.Row{}, SchemaVersion: 1,
	}
	assert.Contains(t, string(ev.AppendJSON(nil)), `"before":{},"after":{}`)
}
