// SPDX-License-Identifier: Apache-2.0

package watermark_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/watermark"
)

const testNonce = "01JB0000000000000000000000"

func lowPayload(chunk uint64) watermark.Payload {
	return watermark.Payload{Nonce: testNonce, Run: 3, Table: "public.users", Chunk: chunk, Mark: watermark.MarkLow}
}

func highPayload(chunk uint64, snap watermark.Snapshot, committed ...uint64) watermark.Payload {
	p := lowPayload(chunk)
	p.Mark = watermark.MarkHigh
	p.Snapshot = snap
	p.CommittedXIP = committed
	return p
}

func TestParsePayload_Valid(t *testing.T) {
	t.Parallel()
	snap := watermark.Snapshot{Xmin: 100, Xmax: 110, XIP: []uint64{100, 104}}
	cases := []struct {
		name string
		in   string
		want watermark.Payload
	}{
		{
			name: "low",
			in:   `{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":0,"mark":"low"}`,
			want: lowPayload(0),
		},
		{
			name: "low with whitespace and other member order",
			in: " {\"mark\" : \"low\", \"chunk\":9,\"table\":\"public.users\",\"run\":3,\n" +
				"\t\"nonce\":\"01JB0000000000000000000000\"}\n",
			want: lowPayload(9),
		},
		{
			name: "high",
			in: `{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":2,"mark":"high",` +
				`"snapshot":"100:110:100,104","committed_xip":[104]}`,
			want: highPayload(2, snap, 104),
		},
		{
			name: "high with empty committed_xip",
			in: `{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":2,"mark":"high",` +
				`"snapshot":"100:110:100,104","committed_xip":[]}`,
			want: highPayload(2, snap),
		},
		{
			name: "table name with dot and unicode",
			in:   `{"nonce":"01JB0000000000000000000000","run":3,"table":"app.ünï.code","chunk":0,"mark":"low"}`,
			want: watermark.Payload{Nonce: testNonce, Run: 3, Table: "app.ünï.code", Mark: watermark.MarkLow},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := watermark.ParsePayload([]byte(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParsePayload_Invalid(t *testing.T) {
	t.Parallel()
	const (
		nonce = `"nonce":"01JB0000000000000000000000"`
		users = `"table":"public.users"`
		base  = nonce + `,"run":3,` + users + `,"chunk":2`
		hi    = `,"mark":"high","snapshot":"100:110:`
	)
	cases := []struct {
		name string
		in   string
	}{
		{name: "empty", in: ``},
		{name: "array", in: `[]`},
		{name: "null", in: `null`},
		{name: "truncated", in: `{` + base},
		{name: "trailing data", in: `{` + base + `,"mark":"low"}{}`},
		{name: "trailing garbage", in: `{` + base + `,"mark":"low"} x`},
		{name: "invalid utf-8", in: "{" + base + `,"mark":"low","x":"` + "\xff" + `"}`},
		{name: "unknown member", in: `{` + base + `,"mark":"low","extra":1}`},
		{name: "duplicate member", in: `{` + base + `,"mark":"low","chunk":3}`},
		{name: "missing mark", in: `{` + base + `}`},
		{name: "missing nonce", in: `{"run":3,"table":"public.users","chunk":2,"mark":"low"}`},
		{name: "null run", in: `{` + nonce + `,"run":null,` + users + `,"chunk":2,"mark":"low"}`},
		{name: "null nonce", in: `{"nonce":null,"run":3,` + users + `,"chunk":2,"mark":"low"}`},
		{name: "string run", in: `{` + nonce + `,"run":"3",` + users + `,"chunk":2,"mark":"low"}`},
		{name: "negative chunk", in: `{` + nonce + `,"run":3,` + users + `,"chunk":-1,"mark":"low"}`},
		{name: "float chunk", in: `{` + nonce + `,"run":3,` + users + `,"chunk":1.5,"mark":"low"}`},
		{name: "exponent chunk", in: `{` + nonce + `,"run":3,` + users + `,"chunk":1e2,"mark":"low"}`},
		{name: "unknown mark", in: `{` + base + `,"mark":"mid"}`},
		{name: "upper-case mark", in: `{` + base + `,"mark":"LOW"}`},
		{name: "bad nonce", in: `{"nonce":"not-a-ulid","run":3,` + users + `,"chunk":2,"mark":"low"}`},
		{name: "lower-case nonce", in: `{"nonce":"01jb0000000000000000000000","run":3,` + users + `,"chunk":2,"mark":"low"}`},
		{name: "nonce overflow", in: `{"nonce":"81JB0000000000000000000000","run":3,` + users + `,"chunk":2,"mark":"low"}`},
		{name: "table without schema", in: `{` + nonce + `,"run":3,"table":"users","chunk":2,"mark":"low"}`},
		{name: "table with control", in: `{` + nonce + `,"run":3,"table":"a.b\u0000","chunk":2,"mark":"low"}`},
		{name: "low with snapshot", in: `{` + base + `,"mark":"low","snapshot":"100:110:"}`},
		{name: "low with committed_xip", in: `{` + base + `,"mark":"low","committed_xip":[]}`},
		{name: "high without snapshot", in: `{` + base + `,"mark":"high","committed_xip":[]}`},
		{name: "high without committed_xip", in: `{` + base + `` + hi + `"}`},
		{name: "high bad snapshot", in: `{` + base + `,"mark":"high","snapshot":"110:100:","committed_xip":[]}`},
		{name: "high null snapshot", in: `{` + base + `,"mark":"high","snapshot":null,"committed_xip":[]}`},
		{name: "high null committed_xip", in: `{` + base + `` + hi + `","committed_xip":null}`},
		{name: "committed null element", in: `{` + base + `` + hi + `104","committed_xip":[null]}`},
		{name: "committed not in xip", in: `{` + base + `` + hi + `104","committed_xip":[105]}`},
		{name: "committed unsorted", in: `{` + base + `` + hi + `101,104","committed_xip":[104,101]}`},
		{name: "committed duplicate", in: `{` + base + `` + hi + `104","committed_xip":[104,104]}`},
		{name: "committed not array", in: `{` + base + `` + hi + `104","committed_xip":104}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := watermark.ParsePayload([]byte(tc.in))
			require.ErrorIs(t, err, watermark.ErrInvalidPayload)
		})
	}
}

func TestPayload_MarshalJSON_Exact(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   watermark.Payload
		want string
	}{
		{
			name: "low",
			in:   lowPayload(4),
			want: `{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":4,"mark":"low"}`,
		},
		{
			name: "high",
			in:   highPayload(4, watermark.Snapshot{Xmin: 100, Xmax: 110, XIP: []uint64{100, 104}}, 100, 104),
			want: `{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":4,"mark":"high",` +
				`"snapshot":"100:110:100,104","committed_xip":[100,104]}`,
		},
		{
			name: "high without committed",
			in:   highPayload(4, watermark.Snapshot{Xmin: 100, Xmax: 100}),
			want: `{"nonce":"01JB0000000000000000000000","run":3,"table":"public.users","chunk":4,"mark":"high",` +
				`"snapshot":"100:100:","committed_xip":[]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.in.MarshalJSON()
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, string(got))
			assert.Equal(t, tc.want, string(got))
			viaStd, err := json.Marshal(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(viaStd))
		})
	}
}

func TestPayload_MarshalJSON_Invalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   watermark.Payload
	}{
		{name: "zero", in: watermark.Payload{}},
		{name: "high with zero snapshot", in: highPayload(1, watermark.Snapshot{})},
		{name: "low with committed", in: watermark.Payload{
			Nonce: testNonce, Table: "a.b", Mark: watermark.MarkLow, CommittedXIP: []uint64{},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := tc.in.MarshalJSON()
			require.ErrorIs(t, err, watermark.ErrInvalidPayload)
		})
	}
}

func TestPayload_UnmarshalJSON_KeepsValueOnError(t *testing.T) {
	t.Parallel()
	p := lowPayload(1)
	require.Error(t, json.Unmarshal([]byte(`{"mark":"low"}`), &p))
	assert.Equal(t, lowPayload(1), p)
}

func TestPayload_IsFor_Filter(t *testing.T) {
	t.Parallel()
	p := lowPayload(1)
	cases := []struct {
		name  string
		nonce string
		run   uint64
		table string
		own   bool
		match bool
	}{
		{name: "same process run and table", nonce: testNonce, run: 3, table: "public.users", own: true, match: true},
		{name: "crashed process", nonce: "01JB0000000000000000000001", run: 3, table: "public.users"},
		{name: "other run", nonce: testNonce, run: 4, table: "public.users", own: true},
		{name: "other table", nonce: testNonce, run: 3, table: "public.orders", own: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.own, p.IsOwn(tc.nonce))
			assert.Equal(t, tc.match, p.IsFor(tc.nonce, tc.run, tc.table))
		})
	}
}

func TestPayload_MarshalJSON_RoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		p := watermark.Payload{
			Nonce: rapid.StringMatching(`[0-7][0-9A-HJKMNP-TV-Z]{25}`).Draw(t, "nonce"),
			Run:   rapid.Uint64().Draw(t, "run"),
			Table: rapid.StringMatching(`[a-z_]{1,10}\.[a-z_."<>&]{1,10}`).Draw(t, "table"),
			Chunk: rapid.Uint64().Draw(t, "chunk"),
			Mark:  watermark.MarkLow,
		}
		if rapid.Bool().Draw(t, "high") {
			p.Mark = watermark.MarkHigh
			p.Snapshot = genSnapshot(t)
			for _, x := range p.Snapshot.XIP {
				if rapid.Bool().Draw(t, "committed") {
					p.CommittedXIP = append(p.CommittedXIP, x)
				}
			}
		}
		b, err := p.MarshalJSON()
		require.NoError(t, err)
		got, err := watermark.ParsePayload(b)
		require.NoError(t, err)
		assert.Equal(t, p, got)
	})
}
