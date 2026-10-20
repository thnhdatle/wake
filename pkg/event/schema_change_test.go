// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/event"
)

const validSchemaChange = `{"type":"schema_change","id":"0/16C0000-s0","source":{"db":"app","schema":"public",` +
	`"table":"users"},"lsn":"0/16C0000","ts_ms":1760600100000,"schema_version":4,"columns_before":null,` +
	`"columns_after":[{"name":"id","type":"int8"}]}`

func TestDecodeSchemaChange_Fixture_Content(t *testing.T) {
	t.Parallel()
	sc, err := event.DecodeSchemaChange(fixture(t, "08-schema-change.json"))
	require.NoError(t, err)
	assert.Equal(t, "0/16C0000-s0", sc.ID)
	assert.Equal(t, int64(4), sc.SchemaVersion)
	assert.Equal(t, []event.Column{{Name: "id", Type: "int8"}, {Name: "email", Type: "text"}}, sc.ColumnsBefore)
	require.Len(t, sc.ColumnsAfter, 3)
	assert.Equal(t, event.Column{Name: "country", Type: "text"}, sc.ColumnsAfter[2])

	seq, err := sc.Seq()
	require.NoError(t, err)
	assert.Equal(t, event.Seq{LSN: 0x16C0000, K: 0, Schema: true}, seq)
}

func TestDecodeSchemaChange_NullColumnsBefore_RoundTrip(t *testing.T) {
	t.Parallel()
	sc, err := event.DecodeSchemaChange([]byte(validSchemaChange))
	require.NoError(t, err)
	assert.Nil(t, sc.ColumnsBefore)
	out, err := json.Marshal(sc)
	require.NoError(t, err)
	assert.JSONEq(t, validSchemaChange, string(out))

	// A value (not only a pointer) marshals with its type member too.
	out, err = json.Marshal(*sc)
	require.NoError(t, err)
	assert.JSONEq(t, validSchemaChange, string(out))
}

func TestDecodeSchemaChange_Invalid_Table(t *testing.T) {
	t.Parallel()
	replace := func(old, repl string) string {
		require.Contains(t, validSchemaChange, old)
		return strings.Replace(validSchemaChange, old, repl, 1)
	}
	tests := []struct {
		name string
		in   string
	}{
		{"not json", "{"},
		{"wrong type", replace(`"schema_change"`, `"message"`)},
		{"missing type", replace(`"type":"schema_change",`, ``)},
		{"type null", replace(`"schema_change"`, `null`)},
		{"data event id", replace(`0/16C0000-s0`, `0/16C0000-0`)},
		{"bad lsn", replace(`"lsn":"0/16C0000"`, `"lsn":"x"`)},
		{"missing columns_before", replace(`"columns_before":null,`, ``)},
		{"missing columns_after", replace(`,"columns_after":[{"name":"id","type":"int8"}]`, ``)},
		{"columns_after null", replace(`"columns_after":[{"name":"id","type":"int8"}]`, `"columns_after":null`)},
		{"column without type", replace(`,"type":"int8"`, ``)},
		{"column without name", replace(`"name":"id",`, ``)},
		{"schema_version zero", replace(`"schema_version":4`, `"schema_version":0`)},
		{"negative ts_ms", replace(`"ts_ms":1760600100000`, `"ts_ms":-1`)},
		{"source without table", replace(`,"table":"users"`, ``)},
		{"columns_before wrong column", replace(`"columns_before":null`, `"columns_before":[{"name":"","type":"t"}]`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sc, err := event.DecodeSchemaChange([]byte(tc.in))
			require.ErrorIs(t, err, event.ErrInvalid)
			assert.Nil(t, sc)
		})
	}
}

func TestDecodeSchemaChange_Tombstone_Empty(t *testing.T) {
	t.Parallel()
	_, err := event.DecodeSchemaChange(nil)
	require.ErrorIs(t, err, event.ErrTombstone)
}

func TestSchemaChange_Seq_BadID(t *testing.T) {
	t.Parallel()
	_, err := (&event.SchemaChange{ID: "nope"}).Seq()
	require.ErrorIs(t, err, event.ErrInvalid)
}
