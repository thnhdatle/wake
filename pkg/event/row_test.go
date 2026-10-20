// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/event"
)

func TestRow_HasAndValue_Table(t *testing.T) {
	t.Parallel()
	row := event.Row{"id": json.RawMessage("42"), "gone": json.RawMessage("null")}
	tests := []struct {
		name   string
		row    event.Row
		col    string
		want   bool
		isNull bool
	}{
		{"present", row, "id", true, false},
		{"present null", row, "gone", true, true},
		{"absent", row, "email", false, true},
		{"nil row", nil, "id", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.row.Has(tc.col))
			v, ok := tc.row.Value(tc.col)
			assert.Equal(t, tc.want, ok)
			assert.Equal(t, tc.isNull, v.IsNull())
		})
	}
}

func TestRow_Value_ExactInteger(t *testing.T) {
	t.Parallel()
	e, err := event.Decode(fixture(t, "11-int8-above-2pow53.json"))
	require.NoError(t, err)
	v, ok := e.After.Value("amount")
	require.True(t, ok)
	n, err := v.Int64()
	require.NoError(t, err)
	assert.Equal(t, int64(9007199254740993), n)
}
