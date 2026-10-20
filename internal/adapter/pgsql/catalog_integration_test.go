// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgsql_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestCatalog_ServerSettings_ReadsContainerSettings(t *testing.T) {
	t.Parallel()
	db := srv.NewDatabase(t)
	s, err := newCatalog(t, newPool(t, db.DSN)).ServerSettings(testCtx(t))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, s.ServerVersionNum, 140000)
	assert.Equal(t, "logical", s.WALLevel)
	assert.Equal(t, time.Minute, s.WALSenderTimeout)
	assert.Equal(t, 20, s.MaxReplicationSlots)
	assert.Equal(t, 20, s.MaxWALSenders)
	assert.Equal(t, int64(-1), s.MaxSlotWALKeepSize)
}

func TestCatalog_Slot_ReportsStatusAndLag(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	cat := newCatalog(t, newPool(t, db.DSN))
	st, err := cat.Slot(ctx, "wake_cat")
	require.NoError(t, err)
	assert.Equal(t, pipeline.SlotStatus{}, st)

	db.Exec(t, `SELECT pg_create_logical_replication_slot('wake_cat', 'pgoutput')`)
	db.Exec(t, `CREATE TABLE t (id int PRIMARY KEY); INSERT INTO t SELECT generate_series(1, 1000)`)
	st, err = cat.Slot(ctx, "wake_cat")
	require.NoError(t, err)
	assert.True(t, st.Exists)
	assert.Equal(t, "pgoutput", st.Plugin)
	assert.NotZero(t, st.ConfirmedFlush)
	assert.Equal(t, "reserved", st.WALStatus)
	assert.Zero(t, st.ActivePID)
	assert.Positive(t, st.LagBytes)

	u, err := cat.Usage(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, u.Slots, 1)
	assert.GreaterOrEqual(t, u.Senders, 0)
}

func TestCatalog_CurrentTime_IsServerClock(t *testing.T) {
	t.Parallel()
	db := srv.NewDatabase(t)
	now, err := newCatalog(t, newPool(t, db.DSN)).CurrentTime(testCtx(t))
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), now, time.Minute)
}

func TestCatalog_PublicationTables_DetectsKeys(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `
		CREATE TABLE pk (id int PRIMARY KEY, v text);
		CREATE TABLE nopk (a int, b text);
		CREATE TABLE full_nopk (a int, b text);
		ALTER TABLE full_nopk REPLICA IDENTITY FULL;
		CREATE TABLE idx (a int NOT NULL, b int NOT NULL, c text);
		CREATE UNIQUE INDEX idx_ab ON idx (b, a);
		ALTER TABLE idx REPLICA IDENTITY USING INDEX idx_ab;
		CREATE TABLE nothing (id int PRIMARY KEY);
		ALTER TABLE nothing REPLICA IDENTITY NOTHING;
		CREATE TABLE parted (id int, day date, PRIMARY KEY (id, day)) PARTITION BY RANGE (day);
		CREATE TABLE parted_2026 PARTITION OF parted FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		CREATE PUBLICATION pub FOR TABLE pk, full_nopk, idx, parted WITH (publish_via_partition_root = true);
		CREATE PUBLICATION pub_ins FOR TABLE nopk, nothing WITH (publish = 'insert');`)
	cat := newCatalog(t, newPool(t, db.DSN))
	infos, err := cat.PublicationTables(ctx, []string{"pub", "pub_ins"})
	require.NoError(t, err)
	got := map[string]bool{}
	for _, i := range infos {
		assert.NotZero(t, i.OID)
		got[i.Schema+"."+i.Table] = i.HasKey
	}
	assert.Equal(t, map[string]bool{
		"public.pk": true, "public.nopk": false, "public.full_nopk": true, "public.idx": true,
		"public.nothing": false, "public.parted": true,
	}, got)

	details, err := cat.PublicationTableDetails(ctx, []string{"pub"})
	require.NoError(t, err)
	byName := map[string]pgsql.Table{}
	for _, d := range details {
		byName[d.Name] = d
	}
	assert.True(t, byName["parted"].Partitioned)
	assert.Equal(t, byte('i'), byte(byName["idx"].Identity))
	assert.True(t, byName["pk"].Owned)

	keys, err := cat.KeyColumns(ctx, "public", "parted")
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "day"}, keys)
	keys, err = cat.KeyColumns(ctx, "public", "full_nopk")
	require.NoError(t, err)
	assert.Empty(t, keys)
	keys, err = cat.KeyColumns(ctx, "public", "missing")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestEmitter_EmitMessage_ReturnsMessageLSN(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE PUBLICATION pub`)
	repl := db.Replication(t)
	_, cp := repl.CreateSlot(t, "wake_emit")
	repl.Start(t, "wake_emit", cp, "pub")
	em, err := pgsql.NewEmitter(newPool(t, db.DSN))
	require.NoError(t, err)
	at, err := em.EmitMessage(ctx, "wake.watermark", []byte(`{"mark":"low"}`))
	require.NoError(t, err)
	assert.Greater(t, at, cp)
	empty, err := em.EmitMessage(ctx, "wake.heartbeat", nil)
	require.NoError(t, err)
	assert.Greater(t, empty, at)
	_, err = em.EmitMessage(ctx, "", nil)
	require.ErrorIs(t, err, pgsql.ErrInvalidArgument)

	var got []string
	for len(got) < 2 {
		if lm, ok := repl.Next(t, waitTimeout).(*pgoutput.LogicalMessage); ok {
			assert.True(t, lm.Transactional())
			got = append(got, lm.LSN.String()+" "+lm.Prefix+" "+string(lm.Content))
		}
	}
	assert.Equal(t, []string{at.String() + ` wake.watermark {"mark":"low"}`, empty.String() + " wake.heartbeat "}, got)
}
