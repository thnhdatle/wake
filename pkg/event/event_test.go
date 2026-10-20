// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/event"
)

func TestDecode_Fixtures_AllContractFixtures(t *testing.T) {
	t.Parallel()
	want := map[string]struct {
		kind event.Kind
		op   event.Op
	}{
		"01-create.json":                  {event.KindEvent, event.OpCreate},
		"02-update-default-identity.json": {event.KindEvent, event.OpUpdate},
		"03-update-unchanged-toast.json":  {event.KindEvent, event.OpUpdate},
		"04-update-key-change.json":       {event.KindEvent, event.OpUpdate},
		"05-delete.json":                  {event.KindEvent, event.OpDelete},
		"07-truncate.json":                {event.KindEvent, event.OpTruncate},
		"08-schema-change.json":           {event.KindSchemaChange, ""},
		"09-snapshot-initial.json":        {event.KindEvent, event.OpRead},
		"10-snapshot-watermark.json":      {event.KindEvent, event.OpRead},
		"11-int8-above-2pow53.json":       {event.KindEvent, event.OpCreate},
		"12-keyless-insert.json":          {event.KindEvent, event.OpCreate},
		"06-kafka-tombstone.json":         {"", ""},
	}

	names := fixtureNames(t)
	require.Len(t, names, len(want), "every fixture needs an expectation")
	for _, name := range names {
		exp, ok := want[name]
		require.True(t, ok, name)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			data := fixture(t, name)
			switch name {
			case "06-kafka-tombstone.json":
				checkTombstoneFixture(t, data)
			case "08-schema-change.json":
				kind, err := event.KindOf(data)
				require.NoError(t, err)
				assert.Equal(t, exp.kind, kind)
				sc, err := event.DecodeSchemaChange(data)
				require.NoError(t, err)
				out, err := json.Marshal(sc)
				require.NoError(t, err)
				assert.JSONEq(t, string(data), string(out))
			default:
				kind, err := event.KindOf(data)
				require.NoError(t, err)
				assert.Equal(t, exp.kind, kind)
				e, err := event.Decode(data)
				require.NoError(t, err)
				assert.Equal(t, exp.op, e.Op)
				assert.True(t, e.Op.Known())
				out, err := json.Marshal(e)
				require.NoError(t, err)
				assert.JSONEq(t, string(data), string(out))
				_, err = e.Seq()
				require.NoError(t, err)
			}
		})
	}
}

// checkTombstoneFixture checks the Kafka record wrapper fixture.
func checkTombstoneFixture(t *testing.T, data []byte) {
	t.Helper()
	var wrapper struct {
		Record struct {
			Topic string  `json:"topic"`
			Key   string  `json:"key"`
			Value *string `json:"value"`
		} `json:"kafka_record"`
	}
	require.NoError(t, json.Unmarshal(data, &wrapper))
	require.Nil(t, wrapper.Record.Value)

	var value []byte
	assert.True(t, event.IsKafkaTombstone(value))
	_, err := event.Decode(value)
	require.ErrorIs(t, err, event.ErrTombstone)
	_, err = event.KindOf(value)
	require.ErrorIs(t, err, event.ErrTombstone)
}

func TestDecode_Int8_ExactAbove2Pow53(t *testing.T) {
	t.Parallel()
	e, err := event.Decode(fixture(t, "11-int8-above-2pow53.json"))
	require.NoError(t, err)

	id, ok := e.After.Value("id")
	require.True(t, ok)
	n, err := id.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(9223372036854775807), n)

	amount, ok := e.After.Value("amount")
	require.True(t, ok)
	n, err = amount.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(9007199254740993), n)
	b, err := amount.BigInt()
	require.NoError(t, err)
	assert.Zero(t, big.NewInt(9007199254740993).Cmp(b))
	text, err := amount.Text()
	require.NoError(t, err)
	assert.Equal(t, "9007199254740993", text)
	assert.NotEqual(t, "9007199254740992", text)

	out, err := json.Marshal(e)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"amount":9007199254740993`)
	assert.Contains(t, string(out), `"key":{"id":9223372036854775807}`)
}

func TestDecode_Fields_FixtureContent(t *testing.T) {
	t.Parallel()
	e, err := event.Decode(fixture(t, "01-create.json"))
	require.NoError(t, err)
	require.NotNil(t, e.TxID)
	assert.Equal(t, uint64(7812), *e.TxID)
	assert.Equal(t, "0/16B3748-3", e.ID)
	assert.Equal(t, event.Source{DB: "app", Schema: "public", Table: "users"}, e.Source)
	assert.Equal(t, int64(1760600000000), e.TsMs)
	assert.Equal(t, "0/16B3748", e.LSN)
	assert.Equal(t, int64(3), e.SchemaVersion)
	assert.Nil(t, e.Before)
	assert.True(t, e.Key.Has("id"))
	email, err := mustValue(t, e.After, "email").String()
	require.NoError(t, err)
	assert.Equal(t, "a@example.com", email)
}

func mustValue(t *testing.T, r event.Row, col string) event.Value {
	t.Helper()
	v, ok := r.Value(col)
	require.True(t, ok, col)
	return v
}

func TestDecode_NullKeyAndSnapshot_Shape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		file    string
		nilKey  bool
		nilTxID bool
	}{
		{"truncate has no key", "07-truncate.json", true, false},
		{"keyless insert", "12-keyless-insert.json", true, false},
		{"snapshot has no tx id", "09-snapshot-initial.json", false, true},
		{"watermark has no tx id", "10-snapshot-watermark.json", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, err := event.Decode(fixture(t, tc.file))
			require.NoError(t, err)
			assert.Equal(t, tc.nilKey, e.Key == nil)
			assert.Equal(t, tc.nilTxID, e.TxID == nil)
		})
	}
}

func TestDecode_UnknownOp_NotAnError(t *testing.T) {
	t.Parallel()
	e, err := event.Decode([]byte(validEvent("x", "null", "null")))
	require.NoError(t, err)
	assert.Equal(t, event.Op("x"), e.Op)
	assert.False(t, e.Op.Known())
}

// validEvent builds an event JSON with the given op, before and after.
func validEvent(op, before, after string) string {
	return `{"id":"0/A-1","source":{"db":"d","schema":"s","table":"t"},"op":"` + op +
		`","ts_ms":1,"lsn":"0/A","key":{"id":1},"before":` + before + `,"after":` + after +
		`,"schema_version":1}`
}

func TestDecode_Invalid_Table(t *testing.T) {
	t.Parallel()
	good := validEvent("c", "null", `{"id":1}`)
	replace := func(old, repl string) string {
		require.Contains(t, good, old)
		return strings.Replace(good, old, repl, 1)
	}
	tests := []struct {
		name string
		in   string
	}{
		{"not json", "{"},
		{"trailing garbage", good + " x"},
		{"array", "[]"},
		{"json null", "null"},
		{"number", "1"},
		{"bad id", replace(`"0/A-1"`, `"0/a-1"`)},
		{"id without suffix", replace(`"0/A-1"`, `"0/A"`)},
		{"id with schema suffix", replace(`"0/A-1"`, `"0/A-s1"`)},
		{"id null", replace(`"0/A-1"`, `null`)},
		{"id wrong type", replace(`"0/A-1"`, `5`)},
		{"bad lsn", replace(`"lsn":"0/A"`, `"lsn":"zz"`)},
		{"lsn lower case", replace(`"lsn":"0/A"`, `"lsn":"0/a"`)},
		{"lsn null", replace(`"lsn":"0/A"`, `"lsn":null`)},
		{"missing id", replace(`"id":"0/A-1",`, ``)},
		{"missing source", replace(`"source":{"db":"d","schema":"s","table":"t"},`, ``)},
		{"missing op", replace(`"op":"c",`, ``)},
		{"missing ts_ms", replace(`"ts_ms":1,`, ``)},
		{"missing lsn", replace(`"lsn":"0/A",`, ``)},
		{"missing key", replace(`"key":{"id":1},`, ``)},
		{"missing before", replace(`"before":null,`, ``)},
		{"missing after", replace(`,"after":{"id":1}`, ``)},
		{"missing schema_version", replace(`,"schema_version":1`, ``)},
		{"ts_ms null", replace(`"ts_ms":1`, `"ts_ms":null`)},
		{"ts_ms negative", replace(`"ts_ms":1`, `"ts_ms":-1`)},
		{"ts_ms fraction", replace(`"ts_ms":1`, `"ts_ms":1.5`)},
		{"schema_version zero", replace(`"schema_version":1`, `"schema_version":0`)},
		{"source without table", replace(`,"table":"t"`, ``)},
		{"source without schema", replace(`"schema":"s",`, ``)},
		{"key wrong type", replace(`"key":{"id":1}`, `"key":[1]`)},
		{"tx_id negative", replace(`"id":"0/A-1",`, `"id":"0/A-1","tx_id":-1,`)},
		{"create with before", validEvent("c", `{"id":1}`, `{"id":1}`)},
		{"create without after", validEvent("c", "null", "null")},
		{"read with before", validEvent("r", `{"id":1}`, `{"id":1}`)},
		{"read without after", validEvent("r", "null", "null")},
		{"update without after", validEvent("u", `{"id":1}`, "null")},
		{"delete with after", validEvent("d", `{"id":1}`, `{"id":1}`)},
		{"after wrong type", validEvent("u", "null", `[]`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, err := event.Decode([]byte(tc.in))
			require.ErrorIs(t, err, event.ErrInvalid)
			assert.Nil(t, e)
		})
	}
}

func TestDecode_Valid_OpCombinations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"create", validEvent("c", "null", `{"id":1}`)},
		{"read empty after", validEvent("r", "null", `{}`)},
		{"update default identity", validEvent("u", `{"id":1}`, `{"id":1}`)},
		{"update without before image", validEvent("u", "null", `{"id":1}`)},
		{"delete", validEvent("d", `{"id":1}`, "null")},
		{"truncate", validEvent("t", "null", "null")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := event.Decode([]byte(tc.input))
			require.NoError(t, err)
		})
	}
}

func TestDecode_Tombstone_EmptyInputs(t *testing.T) {
	t.Parallel()
	for name, in := range map[string][]byte{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := event.Decode(in)
			require.ErrorIs(t, err, event.ErrTombstone)
			require.NotErrorIs(t, err, event.ErrInvalid)
			assert.True(t, event.IsKafkaTombstone(in))
		})
	}
	assert.False(t, event.IsKafkaTombstone([]byte("null")))
}

func TestEvent_Seq_FromID(t *testing.T) {
	t.Parallel()
	e, err := event.Decode(fixture(t, "10-snapshot-watermark.json"))
	require.NoError(t, err)
	seq, err := e.Seq()
	require.NoError(t, err)
	assert.Equal(t, event.Seq{LSN: 0x16D0010, K: 10000}, seq)

	_, err = (&event.Event{ID: "bad"}).Seq()
	require.ErrorIs(t, err, event.ErrInvalid)
}

func TestEvent_KeyChanged_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		file string
		want bool
	}{
		{"key change update", "04-update-key-change.json", true},
		{"same key update", "02-update-default-identity.json", false},
		{"toast update", "03-update-unchanged-toast.json", false},
		{"create", "01-create.json", false},
		{"delete", "05-delete.json", false},
		{"truncate", "07-truncate.json", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, err := event.Decode(fixture(t, tc.file))
			require.NoError(t, err)
			assert.Equal(t, tc.want, e.KeyChanged())
		})
	}

	t.Run("update without before image", func(t *testing.T) {
		t.Parallel()
		e := &event.Event{Op: event.OpUpdate, Key: event.Row{"id": json.RawMessage("1")}}
		assert.False(t, e.KeyChanged())
	})
	t.Run("composite key one column moved", func(t *testing.T) {
		t.Parallel()
		e := &event.Event{
			Op:     event.OpUpdate,
			Key:    event.Row{"a": json.RawMessage("1"), "b": json.RawMessage(`"y"`)},
			Before: event.Row{"a": json.RawMessage("1"), "b": json.RawMessage(`"x"`)},
		}
		assert.True(t, e.KeyChanged())
	})
}

func TestMergeUnchanged_Table(t *testing.T) {
	t.Parallel()
	raw := func(s string) json.RawMessage { return json.RawMessage(s) }
	tests := []struct {
		name          string
		prev          event.Row
		after         event.Row
		unchanged     []string
		wantAfter     event.Row
		wantUnchanged []string
	}{
		{
			name:          "copies and clears",
			prev:          event.Row{"id": raw("7"), "body": raw(`"long"`)},
			after:         event.Row{"id": raw("7"), "title": raw(`"new"`)},
			unchanged:     []string{"body"},
			wantAfter:     event.Row{"id": raw("7"), "title": raw(`"new"`), "body": raw(`"long"`)},
			wantUnchanged: nil,
		},
		{
			name:          "keeps columns missing in prev",
			prev:          event.Row{"body": raw(`"long"`)},
			after:         event.Row{"id": raw("7")},
			unchanged:     []string{"body", "blob"},
			wantAfter:     event.Row{"id": raw("7"), "body": raw(`"long"`)},
			wantUnchanged: []string{"blob"},
		},
		{
			name:          "nil prev leaves everything",
			prev:          nil,
			after:         event.Row{"id": raw("7")},
			unchanged:     []string{"body"},
			wantAfter:     event.Row{"id": raw("7")},
			wantUnchanged: []string{"body"},
		},
		{
			name:          "after value wins",
			prev:          event.Row{"body": raw(`"old"`)},
			after:         event.Row{"body": raw(`"cur"`)},
			unchanged:     []string{"body"},
			wantAfter:     event.Row{"body": raw(`"cur"`)},
			wantUnchanged: nil,
		},
		{
			name:          "nothing unchanged",
			prev:          event.Row{"body": raw(`"old"`)},
			after:         event.Row{"id": raw("7")},
			unchanged:     nil,
			wantAfter:     event.Row{"id": raw("7")},
			wantUnchanged: nil,
		},
		{
			name:          "nil after untouched",
			prev:          event.Row{"body": raw(`"old"`)},
			after:         nil,
			unchanged:     []string{"body"},
			wantAfter:     nil,
			wantUnchanged: []string{"body"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := &event.Event{Op: event.OpUpdate, After: tc.after, UnchangedColumns: tc.unchanged}
			event.MergeUnchanged(tc.prev, e)
			assert.Equal(t, tc.wantAfter, e.After)
			assert.Equal(t, tc.wantUnchanged, e.UnchangedColumns)
		})
	}
	t.Run("nil event", func(t *testing.T) {
		t.Parallel()
		assert.NotPanics(t, func() { event.MergeUnchanged(event.Row{}, nil) })
	})
}

func TestMergeUnchanged_Fixture_ToastUpdate(t *testing.T) {
	t.Parallel()
	e, err := event.Decode(fixture(t, "03-update-unchanged-toast.json"))
	require.NoError(t, err)
	prev := event.Row{"id": json.RawMessage("7"), "body": json.RawMessage(`"big text"`)}
	event.MergeUnchanged(prev, e)
	assert.Empty(t, e.UnchangedColumns)
	body, err := mustValue(t, e.After, "body").String()
	require.NoError(t, err)
	assert.Equal(t, "big text", body)

	// The merged image does not alias prev.
	prev["body"][1] = 'X'
	body, err = mustValue(t, e.After, "body").String()
	require.NoError(t, err)
	assert.Equal(t, "big text", body)
}

func FuzzDecode(f *testing.F) {
	for _, name := range fixtureNames(f) {
		f.Add(fixture(f, name))
	}
	f.Add([]byte(nil))
	f.Add([]byte(`{"type":"message"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		e, err := event.Decode(data)
		if err == nil {
			out, mErr := json.Marshal(e)
			require.NoError(t, mErr)
			_, err = event.Decode(out)
			require.NoError(t, err, "re-decode of %s", out)
		}
		_, _ = event.DecodeSchemaChange(data)
		_, _ = event.DecodeMessage(data)
		_, _ = event.KindOf(data)
	})
}
