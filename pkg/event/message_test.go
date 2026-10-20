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

const validMessage = `{"type":"message","id":"0/16B3748-m0","lsn":"0/16B3748","ts_ms":1760600000000,` +
	`"transactional":true,"prefix":"app.audit","content_base64":"aGVsbG8="}`

func TestDecodeMessage_Valid_RoundTrip(t *testing.T) {
	t.Parallel()
	m, err := event.DecodeMessage([]byte(validMessage))
	require.NoError(t, err)
	assert.Equal(t, "0/16B3748-m0", m.ID)
	assert.Equal(t, "app.audit", m.Prefix)
	assert.True(t, m.Transactional)
	assert.Equal(t, []byte("hello"), m.Content)

	out, err := json.Marshal(m)
	require.NoError(t, err)
	assert.JSONEq(t, validMessage, string(out))
}

func TestMessage_MarshalJSON_EmptyContent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content []byte
	}{
		{"nil content", nil},
		{"empty content", []byte{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, err := json.Marshal(event.Message{ID: "0/A-m0", LSN: "0/A", Content: tc.content})
			require.NoError(t, err)
			assert.Contains(t, string(out), `"content_base64":""`)
			_, err = event.DecodeMessage(out)
			require.NoError(t, err)
		})
	}
}

func TestDecodeMessage_Invalid_Table(t *testing.T) {
	t.Parallel()
	replace := func(old, repl string) string {
		require.Contains(t, validMessage, old)
		return strings.Replace(validMessage, old, repl, 1)
	}
	tests := []struct {
		name string
		in   string
	}{
		{"not json", "{"},
		{"wrong type", replace(`"message"`, `"schema_change"`)},
		{"missing type", replace(`"type":"message",`, ``)},
		{"id without m", replace(`0/16B3748-m0`, `0/16B3748-0`)},
		{"id bad number", replace(`0/16B3748-m0`, `0/16B3748-mx`)},
		{"id bad lsn", replace(`0/16B3748-m0`, `zz-m0`)},
		{"bad lsn", replace(`"lsn":"0/16B3748"`, `"lsn":"1"`)},
		{"negative ts_ms", replace(`"ts_ms":1760600000000`, `"ts_ms":-5`)},
		{"bad base64", replace(`aGVsbG8=`, `***`)},
		{"content null", replace(`"content_base64":"aGVsbG8="`, `"content_base64":null`)},
		{"missing prefix", replace(`"prefix":"app.audit",`, ``)},
		{"missing transactional", replace(`"transactional":true,`, ``)},
		{"transactional wrong type", replace(`"transactional":true`, `"transactional":"yes"`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := event.DecodeMessage([]byte(tc.in))
			require.ErrorIs(t, err, event.ErrInvalid)
			assert.Nil(t, m)
		})
	}
}

func TestDecodeMessage_Tombstone_Empty(t *testing.T) {
	t.Parallel()
	_, err := event.DecodeMessage([]byte{})
	require.ErrorIs(t, err, event.ErrTombstone)
}
