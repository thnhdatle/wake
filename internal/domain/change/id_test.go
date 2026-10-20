// SPDX-License-Identifier: Apache-2.0

package change_test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
)

var c4IDRE = regexp.MustCompile(`^[0-9A-F]+/[0-9A-F]+-(?:[0-9]+|r[0-9]+-[0-9]+|s[0-9]+)$`)

func TestID_String(t *testing.T) {
	t.Parallel()
	at := lsn.LSN(0x16B3748)
	cases := []struct {
		name string
		id   change.ID
		want string
	}{
		{name: "stream", id: change.StreamID(at, 3), want: "0/16B3748-3"},
		{name: "snapshot", id: change.SnapshotID(at, 2, 10000), want: "0/16B3748-r2-10000"},
		{name: "schema", id: change.SchemaID(at, 0), want: "0/16B3748-s0"},
		{name: "message", id: change.MessageID(at, 1), want: "0/16B3748-m1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.id.String())
			got, err := change.ParseID(tc.want)
			require.NoError(t, err)
			assert.Equal(t, tc.id, got)
		})
	}
}

func TestParseID_Invalid(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"", "0/1", "0/1-", "0/1-x", "0/1-r1", "0/1-r-1", "0/1-r1-", "0/1-s", "0/1-m",
		"0/a-1", "zz-1", "0/1-+1", "0/1-1a", "0/1-99999999999999999999", "0/1-r99999999999999999999-1",
	} {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			_, err := change.ParseID(in)
			require.ErrorIs(t, err, change.ErrInvalidID)
		})
	}
}

func genID(t *rapid.T) change.ID {
	kind := change.IDKind(rapid.IntRange(0, 3).Draw(t, "kind"))
	id := change.ID{
		LSN:  lsn.LSN(rapid.Uint64().Draw(t, "lsn")),
		Kind: kind,
		N:    rapid.Uint64().Draw(t, "n"),
	}
	if kind == change.IDKindSnapshot {
		id.Run = rapid.Uint64().Draw(t, "run")
	}
	return id
}

func TestID_RoundTripAndGrammar(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		id := genID(t)
		s := id.String()
		got, err := change.ParseID(s)
		require.NoError(t, err)
		assert.Equal(t, id, got)
		if id.Kind != change.IDKindMessage {
			assert.Regexp(t, c4IDRE, s)
		}
	})
}

func TestID_StreamIDsIncreaseWithSeq(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		a := change.StreamID(lsn.LSN(rapid.Uint64().Draw(t, "la")), rapid.Uint64().Draw(t, "ka"))
		b := change.StreamID(lsn.LSN(rapid.Uint64().Draw(t, "lb")), rapid.Uint64().Draw(t, "kb"))
		if a == b {
			return
		}
		assert.NotEqual(t, a.String(), b.String(), "distinct seq must give distinct ids")
	})
}
