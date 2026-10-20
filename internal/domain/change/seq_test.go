// SPDX-License-Identifier: Apache-2.0

package change_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
)

func genSeq(t *rapid.T, label string) change.Seq {
	return change.Seq{
		LSN:    lsn.LSN(rapid.Uint64Range(0, 4).Draw(t, label+"_lsn")),
		K:      rapid.Uint64Range(0, 4).Draw(t, label+"_k"),
		Schema: rapid.Bool().Draw(t, label+"_schema"),
	}
}

func TestCompareSeq_TotalOrder(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		a, b, c := genSeq(t, "a"), genSeq(t, "b"), genSeq(t, "c")
		assert.Equal(t, -change.CompareSeq(a, b), change.CompareSeq(b, a), "antisymmetric")
		assert.Equal(t, a == b, change.CompareSeq(a, b) == 0, "equal iff identical")
		if a.Less(b) && b.Less(c) {
			assert.True(t, a.Less(c), "transitive")
		}
	})
}

func TestCompareSeq_LSNFirstThenKThenSchema(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a, b change.Seq
	}{
		{name: "lower lsn wins over k", a: change.Seq{LSN: 1, K: 9}, b: change.Seq{LSN: 2}},
		{name: "lower k", a: change.Seq{LSN: 1, K: 0, Schema: false}, b: change.Seq{LSN: 1, K: 1, Schema: true}},
		{name: "schema before data", a: change.Seq{LSN: 1, K: 1, Schema: true}, b: change.Seq{LSN: 1, K: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, -1, change.CompareSeq(tc.a, tc.b))
			assert.Equal(t, 1, change.CompareSeq(tc.b, tc.a))
		})
	}
}

func TestID_Seq(t *testing.T) {
	t.Parallel()
	assert.Equal(t, change.Seq{LSN: 5, K: 2, Schema: true}, change.SchemaID(5, 2).Seq())
	assert.Equal(t, change.Seq{LSN: 5, K: 7}, change.SnapshotID(5, 1, 7).Seq())
	assert.True(t, change.SchemaID(5, 2).Seq().Less(change.StreamID(5, 2).Seq()))
}
