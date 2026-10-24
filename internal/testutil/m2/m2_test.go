// SPDX-License-Identifier: Apache-2.0

package m2_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/testutil/m2"
	"github.com/thnhdatle/wake/pkg/event"
)

const src = `"source":{"db":"app","schema":"public","table":"users"}`

func create(lsn string, k, id int, email string) string {
	return fmt.Sprintf(`{"id":"%s-%d",%s,"op":"c","ts_ms":1,"lsn":"%s","tx_id":7,"key":{"id":%d},`+
		`"before":null,"after":{"id":%d,"email":%q,"bio":"long"},"schema_version":1}`, lsn, k, src, lsn, id, id, email)
}

func read(lsn string, run, n, id int, email string) string {
	return fmt.Sprintf(`{"id":"%s-r%d-%d",%s,"op":"r","ts_ms":1,"lsn":"%s","key":{"id":%d},`+
		`"before":null,"after":{"id":%d,"email":%q,"bio":"long"},"schema_version":1}`, lsn, run, n, src, lsn, id, id,
		email)
}

func update(lsn string, k, before, id int, email string, unchanged bool) string {
	after := fmt.Sprintf(`{"id":%d,"email":%q,"bio":"new"}`, id, email)
	extra := ""
	if unchanged {
		after = fmt.Sprintf(`{"id":%d,"email":%q}`, id, email)
		extra = `,"unchanged_columns":["bio"]`
	}
	return fmt.Sprintf(`{"id":"%s-%d",%s,"op":"u","ts_ms":1,"lsn":"%s","tx_id":7,"key":{"id":%d},`+
		`"before":{"id":%d},"after":%s%s,"schema_version":1}`, lsn, k, src, lsn, id, before, after, extra)
}

func del(lsn string, k, id int) string {
	return fmt.Sprintf(`{"id":"%s-%d",%s,"op":"d","ts_ms":1,"lsn":"%s","tx_id":7,"key":{"id":%d},`+
		`"before":{"id":%d},"after":null,"schema_version":1}`, lsn, k, src, lsn, id, id)
}

func truncate(lsn string) string {
	return fmt.Sprintf(`{"id":"%s-0",%s,"op":"t","ts_ms":1,"lsn":"%s","tx_id":7,"key":null,`+
		`"before":null,"after":null,"schema_version":1}`, lsn, src, lsn)
}

func key(id int) string { return fmt.Sprintf(`{"id":%d}`, id) }

func row(id int, email, bio string) event.Row {
	return event.Row{
		"id": []byte(strconv.Itoa(id)), "email": []byte(strconv.Quote(email)), "bio": []byte(strconv.Quote(bio)),
	}
}

func TestReplayer_State(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		payloads []string
		want     m2.Table
	}{
		{
			name: "applies in seq order, not arrival order",
			payloads: []string{
				update("0/200", 0, 1, 1, "b", false),
				read("0/100", 1, 0, 1, "a"),
			},
			want: m2.Table{key(1): row(1, "b", "new")},
		},
		{
			name: "delete removes; tombstone skipped",
			payloads: []string{
				create("0/100", 0, 1, "a"), create("0/100", 1, 2, "b"), del("0/200", 0, 1), "",
			},
			want: m2.Table{key(2): row(2, "b", "long")},
		},
		{
			name: "key change deletes the before key first",
			payloads: []string{
				create("0/100", 0, 1, "a"), update("0/200", 0, 1, 5, "a", false),
			},
			want: m2.Table{key(5): row(5, "a", "new")},
		},
		{
			name:     "unchanged columns keep the previous value",
			payloads: []string{create("0/100", 0, 1, "a"), update("0/200", 0, 1, 1, "z", true)},
			want:     m2.Table{key(1): row(1, "z", "long")},
		},
		{
			name: "a snapshot read with a later seq than a delete restores the row",
			payloads: []string{
				read("0/300", 2, 7, 1, "stale?"), del("0/250", 0, 1), create("0/100", 0, 1, "a"),
			},
			want: m2.Table{key(1): row(1, "stale?", "long")},
		},
		{
			name: "truncate empties the table, duplicates are ignored",
			payloads: []string{
				create("0/100", 0, 1, "a"), truncate("0/200"), create("0/300", 0, 2, "b"), create("0/300", 0, 2, "b"),
			},
			want: m2.Table{key(2): row(2, "b", "long")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var r m2.Replayer
			for _, p := range tc.payloads {
				require.NoError(t, r.Add([]byte(p)))
			}

			st, err := r.State()

			require.NoError(t, err)
			assert.Empty(t, m2.Diff(st["public.users"], tc.want))
		})
	}
}

func TestReplayer_SkipsOtherKindsAndCounts(t *testing.T) {
	t.Parallel()
	var r m2.Replayer
	schema := `{"type":"schema_change","id":"0/100-s0",` + src + `,"lsn":"0/100","ts_ms":1,"schema_version":2,` +
		`"columns_before":null,"columns_after":[{"name":"id","type":"int8"}]}`

	require.NoError(t, r.Add([]byte(schema)))
	require.NoError(t, r.Add(nil))
	require.NoError(t, r.Add([]byte(create("0/100", 0, 1, "a"))))
	require.NoError(t, r.Add([]byte(create("0/100", 0, 1, "a"))))
	require.Error(t, r.Add([]byte(`{"id":"nope"}`)))

	assert.Equal(t, m2.Stats{Events: 1, Duplicates: 1, Tombstones: 1, Other: 1}, r.Stats())
}

func TestReplayer_KeylessRowsAreKeyedByID(t *testing.T) {
	t.Parallel()
	var r m2.Replayer
	for _, k := range []int{0, 1} {
		p := fmt.Sprintf(`{"id":"0/100-%d",%s,"op":"c","ts_ms":1,"lsn":"0/100","tx_id":7,"key":null,`+
			`"before":null,"after":{"v":1},"schema_version":1}`, k, src)
		require.NoError(t, r.Add([]byte(p)))
	}

	st, err := r.State()

	require.NoError(t, err)
	assert.Len(t, st["public.users"], 2)
}

func TestDiffAndFromRows(t *testing.T) {
	t.Parallel()
	want, err := m2.FromRows([]string{"id"}, []event.Row{row(1, "a", "x"), row(2, "b", "y")})
	require.NoError(t, err)
	got := m2.Table{key(1): row(1, "a", "x"), key(3): row(3, "c", "z")}
	spaced := m2.Table{
		key(1): {"id": []byte(" 1 "), "email": []byte(`"a"`), "bio": []byte(`"x"`)},
		key(2): row(2, "b", "y"),
	}

	assert.Equal(t, []string{
		`extra at sink: {"id":3}`,
		`missing at sink: {"id":2}`,
	}, m2.Diff(got, want))
	assert.Empty(t, m2.Diff(spaced, want), "values compare as compact JSON")
	_, err = m2.FromRows([]string{"nope"}, []event.Row{row(1, "a", "x")})
	require.Error(t, err)
}

func TestCanonicalKey(t *testing.T) {
	t.Parallel()
	k, err := m2.CanonicalKey(event.Row{"b": []byte(` "x" `), "a": []byte("12345678901234567890")})

	require.NoError(t, err)
	canonical := []byte(`{"a":12345678901234567890,"b":"x"}`)
	assert.Equal(t, canonical, []byte(k), "byte-exact canonical form")
}
