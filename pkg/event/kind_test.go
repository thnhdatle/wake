// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/event"
)

func TestKindOf_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    event.Kind
		wantErr error
	}{
		{"data event", validEvent("c", "null", `{"id":1}`), event.KindEvent, nil},
		{"schema change", validSchemaChange, event.KindSchemaChange, nil},
		{"message", validMessage, event.KindMessage, nil},
		{"empty", "", "", event.ErrTombstone},
		{"unknown type", `{"type":"other"}`, "", event.ErrInvalid},
		{"event as type", `{"type":"event"}`, "", event.ErrInvalid},
		{"type null", `{"type":null}`, "", event.ErrInvalid},
		{"type not string", `{"type":5}`, "", event.ErrInvalid},
		{"not json", "[", "", event.ErrInvalid},
		{"array", "[]", "", event.ErrInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.KindOf([]byte(tc.in))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestKindOf_MixedWebhookArray_Dispatch(t *testing.T) {
	t.Parallel()
	body := "[" + validEvent("c", "null", `{"id":1}`) + "," + validSchemaChange + "," + validMessage + "]"
	var items []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(body), &items))
	require.Len(t, items, 3)

	got := make([]event.Kind, 0, len(items))
	for _, item := range items {
		kind, err := event.KindOf(item)
		require.NoError(t, err)
		got = append(got, kind)
		switch kind {
		case event.KindEvent:
			_, err = event.Decode(item)
		case event.KindSchemaChange:
			_, err = event.DecodeSchemaChange(item)
		case event.KindMessage:
			_, err = event.DecodeMessage(item)
		}
		require.NoError(t, err)
	}
	assert.Equal(t, []event.Kind{event.KindEvent, event.KindSchemaChange, event.KindMessage}, got)
}
