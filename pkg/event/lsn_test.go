// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/pkg/event"
)

func TestParseLSN_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    uint64
		wantErr bool
	}{
		{"zero", "0/0", 0, false},
		{"typical", "0/16B3748", 0x16B3748, false},
		{"high word", "1/0", 1 << 32, false},
		{"max", "FFFFFFFF/FFFFFFFF", ^uint64(0), false},
		{"no slash", "16B3748", 0, true},
		{"empty", "", 0, true},
		{"lower case", "0/16b3748", 0, true},
		{"empty half", "/1", 0, true},
		{"empty low half", "1/", 0, true},
		{"too long half", "0/123456789", 0, true},
		{"not hex", "0/G", 0, true},
		{"two slashes", "0/1/2", 0, true},
		{"sign", "+1/2", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.ParseLSN(tc.in)
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrInvalid)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestFormatLSN_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   uint64
		want string
	}{
		{"zero", 0, "0/0"},
		{"typical", 0x16B3748, "0/16B3748"},
		{"high word", 1 << 32, "1/0"},
		{"max", ^uint64(0), "FFFFFFFF/FFFFFFFF"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, event.FormatLSN(tc.in))
		})
	}
}

func TestFormatLSN_RoundTripProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		lsn := rapid.Uint64().Draw(rt, "lsn")
		got, err := event.ParseLSN(event.FormatLSN(lsn))
		require.NoError(rt, err)
		require.Equal(rt, lsn, got)
	})
}
