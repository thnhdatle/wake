// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgsql_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const appTables = `
	CREATE SCHEMA other;
	CREATE TABLE public.keyed (id int PRIMARY KEY, v text);
	CREATE TABLE public.keyless (a int, b text);
	CREATE TABLE public.full_nopk (a int, b text);
	ALTER TABLE public.full_nopk REPLICA IDENTITY FULL;
	CREATE TABLE public.parted (id int, day date, PRIMARY KEY (id, day)) PARTITION BY RANGE (day);
	CREATE TABLE public.parted_2026 PARTITION OF public.parted FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
	CREATE TABLE public.audit_log (id int PRIMARY KEY);
	CREATE TABLE public."bad name" (id int PRIMARY KEY);
	CREATE UNLOGGED TABLE public.scratch (id int PRIMARY KEY);
	CREATE TABLE other.elsewhere (id int PRIMARY KEY);`

func policy(t *testing.T, emitUnkeyed bool) *pipeline.TablePolicy {
	t.Helper()
	p, err := pipeline.NewTablePolicy(pipeline.TablePolicyConfig{
		DB: "app", Include: []string{"public.*"}, Exclude: []string{"public.audit_*"}, EmitUnkeyed: emitUnkeyed,
	})
	require.NoError(t, err)
	return p
}

// pubTables lists the tables directly in a publication.
func pubTables(t *testing.T, conn *pgx.Conn, pub string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT pr.prrelid::regclass::text FROM pg_publication_rel pr
		JOIN pg_publication p ON p.oid = pr.prpubid WHERE p.pubname = $1 ORDER BY 1`, pub)
	require.NoError(t, err)
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	return names
}

func pubFlags(t *testing.T, conn *pgx.Conn, pub string) string {
	t.Helper()
	var flags string
	require.NoError(t, conn.QueryRow(context.Background(), `SELECT concat_ws(',',
		CASE WHEN pubinsert THEN 'insert' END, CASE WHEN pubupdate THEN 'update' END,
		CASE WHEN pubdelete THEN 'delete' END, CASE WHEN pubtruncate THEN 'truncate' END,
		CASE WHEN pubviaroot THEN 'viaroot' END) FROM pg_publication WHERE pubname = $1`, pub).Scan(&flags))
	return flags
}

func tableNames(ts []pgsql.Table) []string {
	out := make([]string, len(ts))
	for i := range ts {
		out[i] = ts[i].QualifiedName()
	}
	return out
}

func TestPublications_EnsureManaged_NeverPublishesKeylessTables(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, appTables)
	pubs := newPublications(t, newPool(t, db.DSN))
	res, err := pubs.EnsureManaged(ctx, pgsql.ManagedConfig{Source: "app", Resolver: policy(t, false)})
	require.NoError(t, err)
	assert.Equal(t, []string{"wake_app"}, res.Publications)
	assert.Equal(t, []string{"public.full_nopk", "public.keyed", "public.parted"}, tableNames(res.Keyed))
	assert.Empty(t, res.Unkeyed)
	skipped := make([]string, 0, len(res.Skipped))
	for _, s := range res.Skipped {
		skipped = append(skipped, s.Table.QualifiedName()+":"+string(s.Reason))
	}
	assert.Equal(t, []string{"public.bad name:identifier_invalid", "public.keyless:table_keyless"}, skipped)
	assert.Equal(t, []string{"create wake_app", "add public.full_nopk, public.keyed, public.parted to wake_app"},
		res.Changes)

	conn := db.Conn(t)
	assert.Equal(t, []string{"full_nopk", "keyed", "parted"}, pubTables(t, conn, "wake_app"))
	assert.Equal(t, "insert,update,delete,truncate,viaroot", pubFlags(t, conn, "wake_app"))
	// The application's UPDATE and DELETE on the keyless table keep working.
	db.Exec(t, `INSERT INTO keyless VALUES (1, 'a'); UPDATE keyless SET b = 'b'; DELETE FROM keyless`)
	db.Exec(t, `INSERT INTO keyed VALUES (1, 'a'); UPDATE keyed SET v = 'b'; DELETE FROM keyed`)

	again, err := pubs.EnsureManaged(ctx, pgsql.ManagedConfig{Source: "app", Resolver: policy(t, false)})
	require.NoError(t, err)
	assert.Empty(t, again.Changes, "a second run changes nothing")

	// A table that loses its key leaves wake_app at the next refresh.
	db.Exec(t, `ALTER TABLE full_nopk REPLICA IDENTITY DEFAULT`)
	res, err = pubs.EnsureManaged(ctx, pgsql.ManagedConfig{Source: "app", Resolver: policy(t, false)})
	require.NoError(t, err)
	assert.Equal(t, []string{"drop public.full_nopk from wake_app"}, res.Changes)
	db.Exec(t, `INSERT INTO full_nopk VALUES (1, 'a'); UPDATE full_nopk SET b = 'b'`)
}

func TestPublications_EnsureManaged_EmitUnkeyedUsesInsertPublication(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, appTables)
	pubs := newPublications(t, newPool(t, db.DSN))
	cfg := pgsql.ManagedConfig{Source: "app", EmitUnkeyed: true, Resolver: policy(t, true)}
	res, err := pubs.EnsureManaged(ctx, cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"wake_app", "wake_app_insert"}, res.Publications)
	assert.Equal(t, []string{"public.keyless"}, tableNames(res.Unkeyed))
	conn := db.Conn(t)
	assert.Equal(t, []string{"keyless"}, pubTables(t, conn, "wake_app_insert"))
	assert.Equal(t, "insert,viaroot", pubFlags(t, conn, "wake_app_insert"))
	assert.NotContains(t, pubTables(t, conn, "wake_app"), "keyless")

	repl := db.Replication(t)
	_, cp := repl.CreateSlot(t, "wake_emit_unkeyed")
	repl.Start(t, "wake_emit_unkeyed", cp, res.Publications...)
	db.Exec(t, `INSERT INTO keyless VALUES (1, 'a')`)
	db.Exec(t, `UPDATE keyless SET b = 'b'`)
	db.Exec(t, `DELETE FROM keyless`)
	db.Exec(t, `INSERT INTO keyed VALUES (7, 'marker')`)
	img, oid := repl.NextRow(t, waitTimeout)
	rel, ok := repl.Decoder.Relation(oid)
	require.True(t, ok)
	assert.Equal(t, "public.keyless", rel.QualifiedName())
	assert.True(t, rel.IsKeyless(), "the Relation has no key columns")
	assert.Nil(t, img.Key, "key null")
	assert.JSONEq(t, `{"a":1,"b":"a"}`, rowJSON(img.After))
	img, oid = repl.NextRow(t, waitTimeout)
	rel, _ = repl.Decoder.Relation(oid)
	assert.Equal(t, "public.keyed", rel.QualifiedName(), "updates and deletes of the keyless table are not captured")
	assert.JSONEq(t, `{"id":7}`, rowJSON(img.Key))

	// A keyless table that gains a key moves to wake_app.
	db.Exec(t, `ALTER TABLE keyless ADD PRIMARY KEY (a)`)
	res, err = pubs.EnsureManaged(ctx, cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"drop public.keyless from wake_app_insert", "add public.keyless to wake_app"},
		res.Changes)
	assert.Empty(t, pubTables(t, conn, "wake_app_insert"))
	assert.Contains(t, pubTables(t, conn, "wake_app"), "keyless")

	require.NoError(t, pubs.Drop(ctx, "app"))
	var n int
	require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM pg_publication WHERE pubname LIKE 'wake_app%'`).Scan(&n))
	assert.Zero(t, n)
	require.NoError(t, pubs.Drop(ctx, "app"), "dropping absent publications is not an error")
}

func TestPublications_EnsureManaged_FixesOptionsAndMembers(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, appTables+`;
		CREATE PUBLICATION wake_app FOR TABLE public.keyless, public.audit_log WITH (publish = 'insert');`)
	pubs := newPublications(t, newPool(t, db.DSN))
	res, err := pubs.EnsureManaged(ctx, pgsql.ManagedConfig{Source: "app", Resolver: policy(t, false)})
	require.NoError(t, err)
	assert.Equal(t, []string{
		"set options of wake_app", "drop public.audit_log, public.keyless from wake_app",
		"add public.full_nopk, public.keyed, public.parted to wake_app",
	}, res.Changes)
	conn := db.Conn(t)
	assert.Equal(t, []string{"full_nopk", "keyed", "parted"}, pubTables(t, conn, "wake_app"))
	assert.Equal(t, "insert,update,delete,truncate,viaroot", pubFlags(t, conn, "wake_app"))
}

func TestPublications_EnsureManaged_ChecksPrivileges(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, appTables)
	role := db.CreateRole(t, "app", "")
	db.Exec(t, `REVOKE CREATE ON DATABASE `+pgx.Identifier{db.Name}.Sanitize()+` FROM PUBLIC`)
	pubs := newPublications(t, newPool(t, db.RoleDSN(role)))
	cfg := pgsql.ManagedConfig{Source: "app", Resolver: policy(t, false)}

	_, err := pubs.EnsureManaged(ctx, cfg)
	require.ErrorIs(t, err, pgsql.ErrPublicationPrivilege)
	assert.Contains(t, err.Error(), "needs CREATE on the database")

	db.Exec(t, `GRANT CREATE ON DATABASE `+pgx.Identifier{db.Name}.Sanitize()+` TO `+pgx.Identifier{role}.Sanitize())
	_, err = pubs.EnsureManaged(ctx, cfg)
	require.ErrorIs(t, err, pgsql.ErrPublicationPrivilege)
	var coded *pgsql.Error
	require.ErrorAs(t, err, &coded)
	assert.Equal(t, pgsql.CodePublicationPrivilege, coded.ErrorCode())
	assert.Contains(t, err.Error(), "not owned: public.full_nopk, public.keyed, public.parted")

	for _, tbl := range []string{"keyed", "full_nopk", "parted", "keyless"} {
		db.Exec(t, `ALTER TABLE public.`+tbl+` OWNER TO `+pgx.Identifier{role}.Sanitize())
	}
	_, err = pubs.EnsureManaged(ctx, cfg)
	require.NoError(t, err)

	db.Exec(t, `ALTER PUBLICATION wake_app OWNER TO postgres`)
	_, err = pubs.EnsureManaged(ctx, cfg)
	require.ErrorIs(t, err, pgsql.ErrPublicationPrivilege)
	assert.Contains(t, err.Error(), "owned by another role")
	err = pubs.Drop(ctx, "app")
	require.ErrorIs(t, err, pgsql.ErrPublicationPrivilege)

	db.Exec(t, `DROP PUBLICATION wake_app; CREATE PUBLICATION wake_app FOR ALL TABLES`)
	_, err = newPublications(t, newPool(t, db.DSN)).EnsureManaged(ctx, cfg)
	require.ErrorIs(t, err, pgsql.ErrPublicationConflict)
	_, err = pubs.EnsureManaged(ctx, pgsql.ManagedConfig{Source: "App", Resolver: policy(t, false)})
	require.ErrorIs(t, err, pgsql.ErrInvalidArgument)
}

func TestPublications_InspectExisting_ReportsKeylessAndLeafPartitions(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, appTables+`;
		CREATE PUBLICATION ins FOR TABLE public.keyed, public.keyless WITH (publish = 'insert');
		CREATE PUBLICATION leaves FOR TABLE public.parted;`)
	pool := newPool(t, db.DSN)
	pubs := newPublications(t, pool)
	ex, err := pubs.InspectExisting(ctx, "ins")
	require.NoError(t, err)
	assert.True(t, ex.Exists)
	assert.True(t, ex.Owned)
	assert.True(t, ex.Insert)
	assert.False(t, ex.Update)
	assert.Equal(t, []string{"public.keyless"}, tableNames(ex.Keyless()))
	assert.Empty(t, ex.PartitionLeaves())
	infos, err := newCatalog(t, pool).PublicationTables(ctx, []string{"ins"})
	require.NoError(t, err)
	assert.True(t, slices.ContainsFunc(infos, func(i pipeline.TableInfo) bool {
		return i.Table == "keyless" && !i.HasKey
	}))

	ex, err = pubs.InspectExisting(ctx, "leaves")
	require.NoError(t, err)
	assert.False(t, ex.ViaRoot)
	assert.Equal(t, []string{"public.parted_2026"}, tableNames(ex.PartitionLeaves()))

	ex, err = pubs.InspectExisting(ctx, "missing")
	require.NoError(t, err)
	assert.False(t, ex.Exists)
}

// prober is a ReplicationProber with a fixed answer.
type prober struct {
	info pipeline.SystemInfo
	err  error
}

func (p prober) IdentifySystem(context.Context) (pipeline.SystemInfo, error) { return p.info, p.err }

func findings(r *pgsql.Report) map[string]string {
	out := map[string]string{}
	for _, f := range r.Findings {
		key := f.Check
		if _, dup := out[key]; dup {
			key += "#2"
		}
		out[key] = string(f.Status) + ":" + f.Code
	}
	return out
}

func TestRunCheck_ManagedMode(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, appTables)
	cat := newCatalog(t, newPool(t, db.DSN))
	cfg := &pgsql.CheckConfig{SlotName: "wake_check", Managed: true, Source: "app", Resolver: policy(t, false)}
	rep, err := pgsql.RunCheck(ctx, cat, prober{info: pipeline.SystemInfo{SystemID: "1"}}, cfg)
	require.NoError(t, err)
	assert.True(t, rep.Failed())
	assert.Equal(t, map[string]string{
		"server_version": "ok:", "wal_level": "ok:", "replication_privilege": "ok:", "replication_slot": "ok:",
		"wal_senders": "ok:", "publication": "ok:", "tables": "ok:", "keyless_tables": "fail:table_keyless",
		"identifiers": "fail:identifier_invalid", "table_ownership": "ok:",
		"wal_retention": "warn:wal_retention_unlimited",
	}, findings(&rep))

	repErr := pubErr("no")
	cfg.SkipUnkeyed = true
	rep, err = pgsql.RunCheck(ctx, cat, prober{err: repErr}, cfg)
	require.NoError(t, err)
	got := findings(&rep)
	assert.Equal(t, "fail:replication_privilege", got["replication_privilege"])
	assert.Equal(t, "warn:table_keyless", got["keyless_tables"])

	db.Exec(t, `SELECT pg_create_logical_replication_slot('wake_check', 'pgoutput')`)
	_, err = newPublications(t, newPool(t, db.DSN)).EnsureManaged(ctx,
		pgsql.ManagedConfig{Source: "app", Resolver: policy(t, false)})
	require.NoError(t, err)
	rep, err = pgsql.RunCheck(ctx, cat, prober{}, cfg)
	require.NoError(t, err)
	for _, f := range rep.Findings {
		if f.Check == pgsql.CheckSlot || f.Check == pgsql.CheckPublication {
			assert.Equal(t, pgsql.StatusOK, f.Status, f.Message)
			assert.Contains(t, f.Message, "exists")
		}
	}
}

type pubErr string

func (e pubErr) Error() string { return string(e) }

func TestRunCheck_ExistingMode(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, appTables+`;
		CREATE PUBLICATION leaves FOR TABLE public.parted, public.keyed, public.keyless WITH (publish = 'insert');`)
	cat := newCatalog(t, newPool(t, db.DSN))
	pol, err := pipeline.NewTablePolicy(pipeline.TablePolicyConfig{DB: "app", EmitUnkeyed: true})
	require.NoError(t, err)
	cfg := &pgsql.CheckConfig{SlotName: "wake_check_existing", Publication: "leaves", EmitUnkeyed: true, Resolver: pol}
	rep, err := pgsql.RunCheck(ctx, cat, prober{}, cfg)
	require.NoError(t, err)
	got := findings(&rep)
	assert.Equal(t, "ok:", got["publication"])
	assert.Equal(t, "warn:publication_partition_root", got["publication#2"])
	assert.Equal(t, "ok:", got["keyless_tables"])
	assert.False(t, rep.Failed())

	cfg.Publication = "missing"
	rep, err = pgsql.RunCheck(ctx, cat, prober{}, cfg)
	require.NoError(t, err)
	assert.Equal(t, "fail:publication_missing", findings(&rep)["publication"])

	_, err = pgsql.RunCheck(ctx, cat, nil, cfg)
	require.ErrorIs(t, err, pgsql.ErrInvalidArgument)
}

func TestPublications_EnsureManaged_DropsTableThatLosesItsKeyDuringSync(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE racy (id int CONSTRAINT racy_pkey PRIMARY KEY, v text)`)
	// X drops the key but stays open: the sync reads racy as keyed, then its
	// ADD TABLE waits for X's lock; X commits while it waits.
	x, err := db.Conn(t).Begin(ctx)
	require.NoError(t, err)
	_, err = x.Exec(ctx, `ALTER TABLE racy DROP CONSTRAINT racy_pkey`)
	require.NoError(t, err)
	pubs := newPublications(t, newPool(t, db.DSN))
	type result struct {
		res pgsql.SyncResult
		err error
	}
	done := make(chan result, 1)
	cfg := pgsql.ManagedConfig{Source: "app", Resolver: policy(t, false)}
	go func() {
		res, serr := pubs.EnsureManaged(ctx, cfg)
		done <- result{res, serr}
	}()
	admin := db.Conn(t)
	require.Eventually(t, func() bool {
		var waiting int
		require.NoError(t, admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks
			WHERE relation = 'racy'::regclass AND NOT granted`).Scan(&waiting))
		return waiting > 0
	}, waitTimeout, 10*time.Millisecond)
	require.NoError(t, x.Commit(ctx))
	got := <-done
	require.NoError(t, got.err)
	assert.Equal(t, []string{
		"create wake_app", "add public.racy to wake_app",
		"drop public.racy from wake_app (key lost)",
	}, got.res.Changes)
	assert.Empty(t, got.res.Keyed)
	require.Len(t, got.res.Skipped, 1)
	assert.Equal(t, pipeline.SkipKeyless, got.res.Skipped[0].Reason)
	assert.Empty(t, pubTables(t, admin, "wake_app"))
	db.Exec(t, `INSERT INTO racy VALUES (1, 'a'); UPDATE racy SET v = 'b'; DELETE FROM racy`)
}
