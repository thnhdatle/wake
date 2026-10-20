// SPDX-License-Identifier: Apache-2.0

package event_test

import (
	"cmp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/pkg/event"
)

func TestParseSeq_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		id      string
		want    event.Seq
		wantErr bool
	}{
		{"stream", "0/16B3748-3", event.Seq{LSN: 0x16B3748, K: 3}, false},
		{"snapshot", "0/16D0010-r2-10000", event.Seq{LSN: 0x16D0010, K: 10000}, false},
		{"schema", "0/16C0000-s0", event.Seq{LSN: 0x16C0000, K: 0, Schema: true}, false},
		{"high lsn", "1/0-7", event.Seq{LSN: 1 << 32, K: 7}, false},
		{"empty", "", event.Seq{}, true},
		{"no suffix", "0/16B3748", event.Seq{}, true},
		{"empty suffix", "0/1-", event.Seq{}, true},
		{"lower case lsn", "0/16b3748-1", event.Seq{}, true},
		{"bad lsn", "x-1", event.Seq{}, true},
		{"snapshot without counter", "0/1-r2", event.Seq{}, true},
		{"snapshot empty run", "0/1-r-3", event.Seq{}, true},
		{"snapshot bad counter", "0/1-r2-x", event.Seq{}, true},
		{"schema bad k", "0/1-sx", event.Seq{}, true},
		{"message id", "0/1-m0", event.Seq{}, true},
		{"negative k", "0/1--1", event.Seq{}, true},
		{"k overflow", "0/1-18446744073709551616", event.Seq{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := event.ParseSeq(tc.id)
			if tc.wantErr {
				require.ErrorIs(t, err, event.ErrInvalid)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCompare_Table(t *testing.T) {
	t.Parallel()
	mustSeq := func(id string) event.Seq {
		s, err := event.ParseSeq(id)
		require.NoError(t, err)
		return s
	}
	tests := []struct {
		name string
		a, b event.Seq
		want int
	}{
		{"hex A above 9 numerically", mustSeq("0/A-0"), mustSeq("0/9-0"), 1},
		{"hex 9 below A", mustSeq("0/9-0"), mustSeq("0/A-0"), -1},
		{"high word wins", mustSeq("1/0-0"), mustSeq("0/FFFFFFFF-0"), 1},
		{"lsn before k", mustSeq("0/1-9"), mustSeq("0/2-0"), -1},
		{"k decides", mustSeq("0/1-2"), mustSeq("0/1-10"), -1},
		{"equal", mustSeq("0/1-2"), mustSeq("0/1-2"), 0},
		{"schema before data", mustSeq("0/1-s2"), mustSeq("0/1-2"), -1},
		{"data after schema", mustSeq("0/1-2"), mustSeq("0/1-s2"), 1},
		{"schema equal", mustSeq("0/1-s2"), mustSeq("0/1-s2"), 0},
		{"later schema after earlier data", mustSeq("0/1-s3"), mustSeq("0/1-2"), 1},
		{"snapshot counter", mustSeq("0/1-r1-5"), mustSeq("0/1-r1-6"), -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, event.Compare(tc.a, tc.b))
		})
	}
}

// genSeq draws sequences from a small space so that ties are frequent.
func genSeq() *rapid.Generator[event.Seq] {
	return rapid.Custom(func(rt *rapid.T) event.Seq {
		return event.Seq{
			LSN:    rapid.OneOf(rapid.Uint64Range(0, 3), rapid.Uint64Range(1<<32-2, 1<<32+2), rapid.Uint64()).Draw(rt, "lsn"),
			K:      rapid.Uint64Range(0, 3).Draw(rt, "k"),
			Schema: rapid.Bool().Draw(rt, "schema"),
		}
	})
}

func TestCompare_AntisymmetryProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a, b := genSeq().Draw(rt, "a"), genSeq().Draw(rt, "b")
		require.Equal(rt, -event.Compare(b, a), event.Compare(a, b))
		require.Equal(rt, a == b, event.Compare(a, b) == 0)
	})
}

func TestCompare_TransitivityProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a, b, c := genSeq().Draw(rt, "a"), genSeq().Draw(rt, "b"), genSeq().Draw(rt, "c")
		if event.Compare(a, b) <= 0 && event.Compare(b, c) <= 0 {
			require.LessOrEqual(rt, event.Compare(a, c), 0)
		}
		if event.Compare(a, b) < 0 && event.Compare(b, c) < 0 {
			require.Negative(rt, event.Compare(a, c))
		}
	})
}

func TestCompare_NumericLSNProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		a, b := genSeq().Draw(rt, "a"), genSeq().Draw(rt, "b")
		if a.LSN == b.LSN {
			return
		}
		// Round-trip through the textual form: ordering must follow the number,
		// not the string.
		pa, err := event.ParseLSN(event.FormatLSN(a.LSN))
		require.NoError(rt, err)
		pb, err := event.ParseLSN(event.FormatLSN(b.LSN))
		require.NoError(rt, err)
		require.Equal(rt, cmp.Compare(pa, pb), event.Compare(a, b))
	})
}

func TestCompare_SchemaBeforeDataProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		s := genSeq().Draw(rt, "seq")
		schema, data := s, s
		schema.Schema, data.Schema = true, false
		require.Equal(rt, -1, event.Compare(schema, data))
		require.Equal(rt, 1, event.Compare(data, schema))
	})
}

func TestParseSeq_FormatProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		lsn := rapid.Uint64().Draw(rt, "lsn")
		k := rapid.Uint64().Draw(rt, "k")
		run := rapid.Uint64().Draw(rt, "run")
		base := event.FormatLSN(lsn)

		got, err := event.ParseSeq(base + "-" + utoa(k))
		require.NoError(rt, err)
		require.Equal(rt, event.Seq{LSN: lsn, K: k}, got)

		got, err = event.ParseSeq(base + "-r" + utoa(run) + "-" + utoa(k))
		require.NoError(rt, err)
		require.Equal(rt, event.Seq{LSN: lsn, K: k}, got)

		got, err = event.ParseSeq(base + "-s" + utoa(k))
		require.NoError(rt, err)
		require.Equal(rt, event.Seq{LSN: lsn, K: k, Schema: true}, got)
	})
}
