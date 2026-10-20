// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

func validSettings() map[string]setting {
	return map[string]setting{
		"server_version_num":     {value: "170002"},
		"wal_level":              {value: "logical"},
		"wal_sender_timeout":     {value: "60000", unit: "ms"},
		"max_replication_slots":  {value: "10"},
		"max_wal_senders":        {value: "12"},
		"max_slot_wal_keep_size": {value: "1024", unit: "MB"},
	}
}

func TestParseSettings_ConvertsUnits(t *testing.T) {
	t.Parallel()
	s, err := parseSettings(validSettings())
	require.NoError(t, err)
	assert.Equal(t, pipeline.ServerSettings{
		ServerVersionNum: 170002, WALLevel: "logical", WALSenderTimeout: time.Minute, MaxReplicationSlots: 10,
		MaxWALSenders: 12, MaxSlotWALKeepSize: 1 << 30,
	}, s)

	rows := validSettings()
	rows["max_slot_wal_keep_size"] = setting{value: "-1", unit: "MB"}
	rows["wal_sender_timeout"] = setting{value: "0", unit: "ms"}
	s, err = parseSettings(rows)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), s.MaxSlotWALKeepSize)
	assert.Zero(t, s.WALSenderTimeout)
}

func TestParseSettings_RejectsBadRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(map[string]setting)
	}{
		{name: "missing", edit: func(m map[string]setting) { delete(m, "wal_level") }},
		{name: "bad version", edit: func(m map[string]setting) { m["server_version_num"] = setting{value: "x"} }},
		{name: "bad slots", edit: func(m map[string]setting) { m["max_replication_slots"] = setting{value: ""} }},
		{name: "bad senders", edit: func(m map[string]setting) { m["max_wal_senders"] = setting{value: "1.5"} }},
		{name: "bad timeout unit", edit: func(m map[string]setting) {
			m["wal_sender_timeout"] = setting{value: "1", unit: "fortnight"}
		}},
		{name: "bad timeout value", edit: func(m map[string]setting) {
			m["wal_sender_timeout"] = setting{value: "x", unit: "ms"}
		}},
		{name: "bad size unit", edit: func(m map[string]setting) {
			m["max_slot_wal_keep_size"] = setting{value: "1", unit: "PB"}
		}},
		{name: "bad size value", edit: func(m map[string]setting) {
			m["max_slot_wal_keep_size"] = setting{value: "x", unit: "MB"}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rows := validSettings()
			tc.edit(rows)
			_, err := parseSettings(rows)
			require.Error(t, err)
		})
	}
}

func TestSettingUnits_HandleMultipliers(t *testing.T) {
	t.Parallel()
	d, err := durationSetting(setting{value: "2", unit: "min"})
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, d)
	d, err = durationSetting(setting{value: "3", unit: "s"})
	require.NoError(t, err)
	assert.Equal(t, 3*time.Second, d)
	n, err := sizeSetting(setting{value: "4", unit: "8kB"})
	require.NoError(t, err)
	assert.Equal(t, int64(32<<10), n)
	n, err = sizeSetting(setting{value: "5", unit: ""})
	require.NoError(t, err)
	assert.Equal(t, int64(5), n)
	_, err = sizeSetting(setting{value: "5", unit: "99999999999999999999kB"})
	require.Error(t, err)
}

func TestPositions_RoundTrip(t *testing.T) {
	t.Parallel()
	raw, err := encodeKeyPosition([]string{"42", `a"b`, "é"})
	require.NoError(t, err)
	vals, err := decodeKeyPosition(raw, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{"42", `a"b`, "é"}, vals)
	_, err = decodeKeyPosition(raw, 2)
	require.ErrorIs(t, err, ErrBadPosition)
	_, err = decodeKeyPosition(json.RawMessage(`[1]`), 1)
	require.ErrorIs(t, err, ErrBadPosition)

	raw, err = encodeCTIDPosition(16384, tid{page: 12, item: 7})
	require.NoError(t, err)
	assert.JSONEq(t, `{"rel":16384,"ctid":"(12,7)"}`, string(raw))
	rel, at, err := decodeCTIDPosition(raw)
	require.NoError(t, err)
	assert.Equal(t, uint32(16384), rel)
	assert.Equal(t, tid{page: 12, item: 7}, at)
	for _, bad := range []string{`["1"]`, `{"rel":1,"ctid":"(1,2)","x":1}`, `{"rel":1,"ctid":"1,2"}`} {
		_, _, err = decodeCTIDPosition(json.RawMessage(bad))
		require.ErrorIs(t, err, ErrBadPosition, bad)
	}
}

func TestParseTID_RejectsMalformed(t *testing.T) {
	t.Parallel()
	got, err := parseTID("(4294967295,65535)")
	require.NoError(t, err)
	assert.Equal(t, tid{page: 4294967295, item: 65535}, got)
	for _, bad := range []string{"", "(1,2", "1,2)", "(1;2)", "(x,1)", "(1,65536)", "(-1,1)", "(4294967296,1)"} {
		_, err := parseTID(bad)
		require.ErrorIs(t, err, ErrBadPosition, bad)
	}
}

func meta(identity schema.ReplicaIdentity, pk, ri []string) *tableMeta {
	return &tableMeta{
		oid: 1, schema: "s", name: `t"x`, identity: identity, pk: pk, ri: ri,
		cols: []column{{name: "a", typeOID: 23}, {name: "b", typeOID: 25}, {name: "c", typeOID: 1184}},
	}
}

func TestTableMeta_KeysFollowIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		m       *tableMeta
		pageKey []string
		event   []string
	}{
		{
			name: "default with pk", m: meta(schema.IdentityDefault, []string{"b", "a"}, nil),
			pageKey: []string{"b", "a"}, event: []string{"a", "b"},
		},
		{name: "default without pk", m: meta(schema.IdentityDefault, nil, nil)},
		{
			name: "index", m: meta(schema.IdentityIndex, nil, []string{"c", "a"}),
			pageKey: []string{"c", "a"}, event: []string{"a", "c"},
		},
		{
			name: "index with pk pages by pk", m: meta(schema.IdentityIndex, []string{"b"}, []string{"c"}),
			pageKey: []string{"b"}, event: []string{"c"},
		},
		{
			name: "full with pk", m: meta(schema.IdentityFull, []string{"b", "a"}, nil),
			pageKey: []string{"b", "a"}, event: []string{"b", "a"},
		},
		{name: "full without pk", m: meta(schema.IdentityFull, nil, nil), event: []string{"a", "b", "c"}},
		{name: "nothing with pk", m: meta(schema.IdentityNothing, []string{"a"}, nil), pageKey: []string{"a"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.pageKey, tc.m.pageKey())
			assert.Equal(t, tc.event, tc.m.eventKey())
		})
	}
}

func TestKeysetSQL_QuotesAndParameterises(t *testing.T) {
	t.Parallel()
	m := meta(schema.IdentityDefault, []string{"b", "a"}, nil)
	assert.Equal(t, `SELECT "a", "b", "c" FROM ONLY "s"."t""x" ORDER BY "b", "a" LIMIT $1`, keysetSQL(m, m.pk, false))
	assert.Equal(t, `SELECT "a", "b", "c" FROM ONLY "s"."t""x" WHERE ("b", "a") > ($1, $2) ORDER BY "b", "a" LIMIT $3`,
		keysetSQL(m, m.pk, true))
	m.partitioned = true
	assert.Equal(t, `SELECT "a", "b", "c" FROM "s"."t""x" ORDER BY "b", "a" LIMIT $1`, keysetSQL(m, m.pk, false))
	assert.Equal(t, `SELECT ctid, "a", "b", "c" FROM ONLY "p"."leaf" WHERE ctid > $1 AND ctid < $2 ORDER BY ctid LIMIT $3`,
		ctidSQL(m, &leaf{schema: "p", name: "leaf"}))
}

func TestRangePages_UsesStatistics(t *testing.T) {
	t.Parallel()
	assert.Equal(t, uint32(3), rangePages(&leaf{}, 100), "default density 50 rows per page")
	assert.Equal(t, uint32(11), rangePages(&leaf{relpages: 10, reltuples: 100}, 100))
	assert.Equal(t, uint32(101), rangePages(&leaf{relpages: 1000, reltuples: 1}, 100), "at least one row per page")
	assert.Equal(t, uint32(maxRangePages), rangePages(&leaf{relpages: 1 << 30, reltuples: 1}, MaxChunkRows*2))
}

func TestRenderer_RendersRowsAndKeys(t *testing.T) {
	t.Parallel()
	m := meta(schema.IdentityDefault, []string{"b", "a"}, nil)
	r, err := newRenderer(m, 1)
	require.NoError(t, err)
	row, err := r.row([][]byte{[]byte("(0,1)"), []byte("7"), nil, []byte("2026-10-18 12:00:00+02")})
	require.NoError(t, err)
	assert.Equal(t, change.Row{
		{Name: "a", Value: []byte("7")},
		{Name: "b", Value: []byte("null")},
		{Name: "c", Value: []byte(`"2026-10-18T12:00:00+02:00"`)},
	}, row.After)
	assert.Equal(t, change.Row{{Name: "a", Value: []byte("7")}, {Name: "b", Value: []byte("null")}}, row.Key)

	_, err = r.row([][]byte{[]byte("7")})
	require.Error(t, err)
	_, err = r.row([][]byte{nil, []byte("x"), nil, nil})
	require.ErrorIs(t, err, ErrValue)

	keyless, err := newRenderer(meta(schema.IdentityDefault, nil, nil), 0)
	require.NoError(t, err)
	row, err = keyless.row([][]byte{[]byte("1"), []byte("b"), nil})
	require.NoError(t, err)
	assert.Nil(t, row.Key)

	bad := meta(schema.IdentityFull, []string{"generated"}, nil)
	_, err = newRenderer(bad, 0)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestValidateRequest_Bounds(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateRequest(&pipeline.ChunkRequest{Schema: "s", Table: "t", Limit: 1}))
	require.ErrorIs(t, validateRequest(&pipeline.ChunkRequest{Table: "t", Limit: 1}), ErrInvalidArgument)
	require.ErrorIs(t, validateRequest(&pipeline.ChunkRequest{Schema: "s", Table: "t"}), ErrInvalidArgument)
	require.ErrorIs(t, validateRequest(&pipeline.ChunkRequest{Schema: "s", Table: "t", Limit: MaxChunkRows + 1}),
		ErrInvalidArgument)
}

func TestSnapshotNameRE_AcceptsPostgresFormat(t *testing.T) {
	t.Parallel()
	assert.True(t, snapshotNameRE.MatchString("00000003-00000002-1"))
	assert.True(t, snapshotNameRE.MatchString("00000004-0000001B"))
	for _, bad := range []string{"", "x", "00000003-00000002-1'", "0000-0000-1; DROP", "00000003-0000000g-1"} {
		assert.False(t, snapshotNameRE.MatchString(bad), bad)
	}
}

func TestConstructors_ValidateDependencies(t *testing.T) {
	t.Parallel()
	_, err := NewCatalog(nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = NewChunkReader(nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = NewEmitter(nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = NewPublications(nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	p, err := NewPool("postgres://u@localhost/db?DateStyle=German", WithMaxConns(2))
	require.NoError(t, err)
	assert.Equal(t, "ISO", p.cfg.RuntimeParams["DateStyle"])
	assert.Equal(t, "UTC", p.cfg.RuntimeParams["TimeZone"])
	for _, opts := range [][]Option{{WithQueryTimeout(0)}, {WithChunkTimeout(-1)}} {
		_, err = NewCatalog(p, opts...)
		require.ErrorIs(t, err, ErrInvalidArgument)
	}
	_, err = NewCatalog(p, WithLogger(nil), WithQueryTimeout(time.Second), WithChunkTimeout(time.Second))
	require.NoError(t, err)
	_, err = NewPool("postgres://u@localhost/db", WithMaxIdle(0))
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestPublicationNames_FollowSourceName(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"wake_app"}, ManagedPublicationNames("app", false))
	assert.Equal(t, []string{"wake_app", "wake_app_insert"}, ManagedPublicationNames("app", true))
	long58 := "abcdefghij" + "abcdefghij" + "abcdefghij" + "abcdefghij" + "abcdefghij" + "abcdefgh"
	require.NoError(t, validateSource(long58, false))
	require.ErrorIs(t, validateSource(long58, true), ErrInvalidArgument, "wake_<58>_insert exceeds 63 bytes")
	require.NoError(t, validateSource(long58[:51], true))
	require.ErrorIs(t, validateSource("", false), ErrInvalidArgument)
	require.ErrorIs(t, validateSource("App", false), ErrInvalidArgument)
	require.ErrorIs(t, validateSource("a-b", false), ErrInvalidArgument)
}

// fixedResolver resolves by qualified name.
type fixedResolver map[string]pipeline.TableDecision

func (r fixedResolver) Resolve(t pipeline.TableInfo) pipeline.TableDecision {
	return r[t.Schema+"."+t.Table]
}

func tbl(name string, oid uint32, hasKey, owned bool) Table {
	return Table{Schema: "public", Name: name, OID: oid, HasKey: hasKey, Owned: owned}
}

func TestManagedPlan_ClassifiesChecksAndDiffs(t *testing.T) {
	t.Parallel()
	capture := pipeline.TableDecision{Capture: true}
	r := fixedResolver{
		"public.k1": capture, "public.k2": capture, "public.nokey": capture,
		"public.skipkey": {Skip: pipeline.SkipKeyless}, "public.bad": {Skip: pipeline.SkipIdentifierInvalid},
		"public.other": {Skip: pipeline.SkipNotCaptured},
	}
	cands := []Table{
		tbl("k1", 1, true, true), tbl("k2", 2, true, true), tbl("nokey", 3, false, true),
		tbl("skipkey", 4, false, true), tbl("bad", 5, true, true), tbl("other", 6, true, false),
	}
	keyed, unkeyed, skipped := classify(cands, r)
	assert.Equal(t, []string{"public.k1", "public.k2"}, names(keyed))
	assert.Equal(t, []string{"public.nokey"}, names(unkeyed))
	assert.Equal(t, []SkippedTable{
		{Table: cands[3], Reason: pipeline.SkipKeyless}, {Table: cands[4], Reason: pipeline.SkipIdentifierInvalid},
	}, skipped)

	plan := &managedPlan{
		keyed: keyed, unkeyed: unkeyed, canCreate: true,
		main: pubState{
			name: "wake_app", exists: true, owned: true, insert: true, update: true, delete: true, truncate: true,
			viaRoot: true, members: []Table{tbl("k1", 1, true, true), tbl("nokey", 3, false, true)},
		},
		insert: pubState{name: "wake_app_insert"},
	}
	require.NoError(t, plan.check(true))
	stmts := plan.statements(true)
	sqls := make([]string, 0, len(stmts))
	for _, s := range stmts {
		sqls = append(sqls, s.sql)
	}
	assert.Equal(t, []string{
		`CREATE PUBLICATION "wake_app_insert" WITH (publish = 'insert', publish_via_partition_root = true)`,
		`ALTER PUBLICATION "wake_app" DROP TABLE "public"."nokey"`,
		`ALTER PUBLICATION "wake_app" ADD TABLE "public"."k2"`,
		`ALTER PUBLICATION "wake_app_insert" ADD TABLE "public"."nokey"`,
	}, sqls, "a table moving between publications is dropped before it is added")
	assert.Len(t, plan.statements(false), 2, "without emit_unkeyed the insert publication is left alone")

	plan.main.viaRoot = false
	assert.Equal(t, `ALTER PUBLICATION "wake_app" SET (publish = 'insert, update, delete, truncate', `+
		`publish_via_partition_root = true)`, plan.statements(false)[0].sql)
}

func TestManagedPlan_CheckRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		plan managedPlan
		emit bool
		kind error
		text string
	}{
		{
			name: "no create", plan: managedPlan{main: pubState{name: "wake_app"}}, kind: ErrPublicationPrivilege,
			text: "needs CREATE",
		},
		{name: "insert pub needs create", plan: managedPlan{
			main:   pubState{name: "m", exists: true, owned: true},
			insert: pubState{name: "wake_app_insert"},
		}, emit: true, kind: ErrPublicationPrivilege, text: "wake_app_insert"},
		{
			name: "foreign owner", plan: managedPlan{main: pubState{name: "m", exists: true}}, kind: ErrPublicationPrivilege,
			text: "owned by another role",
		},
		{
			name: "all tables", plan: managedPlan{main: pubState{name: "m", exists: true, owned: true, allTables: true}},
			kind: ErrPublicationConflict, text: "FOR ALL TABLES",
		},
		{name: "table not owned", plan: managedPlan{
			canCreate: true, main: pubState{name: "m"},
			unkeyed: []Table{tbl("x", 1, false, false)},
		}, kind: ErrPublicationPrivilege, text: "not owned: public.x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.plan.check(tc.emit)
			require.ErrorIs(t, err, tc.kind)
			assert.Contains(t, err.Error(), tc.text)
		})
	}
}

func TestPubState_HasOptions(t *testing.T) {
	t.Parallel()
	all := pubState{insert: true, update: true, delete: true, truncate: true, viaRoot: true}
	assert.True(t, all.hasOptions(publishAll))
	assert.False(t, all.hasOptions(publishInsert))
	ins := pubState{insert: true, viaRoot: true}
	assert.True(t, ins.hasOptions(publishInsert))
	assert.False(t, ins.hasOptions(publishAll))
}

func TestError_CarriesCode(t *testing.T) {
	t.Parallel()
	cause := errors.New("permission denied")
	e := codedError(CodePublicationPrivilege, ErrPublicationPrivilege, "detail")
	e.cause = cause
	wrapped := fmt.Errorf("outer: %w", e)
	require.ErrorIs(t, wrapped, ErrPublicationPrivilege)
	require.ErrorIs(t, wrapped, cause)
	assert.Equal(t, CodePublicationPrivilege, codeOf(wrapped, "x"))
	assert.Equal(t, "x", codeOf(cause, "x"))
	assert.Equal(t, "pgsql: publication privilege missing (publication_privilege): detail: permission denied", e.Error())
	assert.Equal(t, []error{ErrPublicationMissing}, codedError("c", ErrPublicationMissing, "d").Unwrap())
	other := errors.New("other")
	require.ErrorIs(t, mapDDLError("DROP", other), other)
	require.NotErrorIs(t, mapDDLError("DROP", other), ErrPublicationPrivilege)
}
