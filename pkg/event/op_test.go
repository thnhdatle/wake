// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/wake/pkg/event"
)

func TestOp_Known_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   event.Op
		want bool
	}{
		{"create", event.OpCreate, true},
		{"update", event.OpUpdate, true},
		{"delete", event.OpDelete, true},
		{"read", event.OpRead, true},
		{"truncate", event.OpTruncate, true},
		{"unknown letter", event.Op("x"), false},
		{"empty", event.Op(""), false},
		{"upper case", event.Op("C"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.op.Known())
		})
	}
}
