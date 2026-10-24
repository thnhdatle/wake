// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgrepl_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/pgrepl"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/pgtest"
)

var srv *pgtest.Server

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &srv)) }

const waitTimeout = 30 * time.Second

func newConn(t *testing.T, dsn string, opts ...pgrepl.Option) *pgrepl.Conn {
	t.Helper()
	c, err := pgrepl.New(dsn, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

type stream struct {
	t    *testing.T
	conn *pgrepl.Conn
	dec  *pgoutput.Decoder
}

func newStream(t *testing.T, c *pgrepl.Conn) *stream {
	t.Helper()
	return &stream{t: t, conn: c, dec: pgoutput.NewDecoder()}
}

func (s *stream) next(ctx context.Context) pgoutput.Message {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()
	for {
		msg, err := s.conn.Receive(ctx)
		require.NoError(s.t, err)
		switch msg.Kind {
		case pipeline.ReplicationMessageKeepalive:
			if msg.Keepalive.ReplyRequested {
				require.NoError(s.t, s.conn.SendStandbyStatus(ctx, pipeline.StandbyStatus{
					Written: msg.Keepalive.ServerWALEnd,
				}))
			}
		case pipeline.ReplicationMessageXLogData:
			m, err := s.dec.Decode(msg.XLogData.Data)
			require.NoError(s.t, err)
			return m
		}
	}
}

func (s *stream) nextRow(ctx context.Context) pgoutput.RowImages {
	s.t.Helper()
	for {
		m := s.next(ctx)
		switch m.(type) {
		case *pgoutput.Insert, *pgoutput.Update, *pgoutput.Delete:
			img, err := s.dec.RowImages(m)
			require.NoError(s.t, err)
			return img
		}
	}
}

func (s *stream) nextCommit(ctx context.Context) *pgoutput.Commit {
	s.t.Helper()
	for {
		if c, ok := s.next(ctx).(*pgoutput.Commit); ok {
			return c
		}
	}
}

func rowJSON(r change.Row) string {
	b := []byte{'{'}
	for i, c := range r {
		if i > 0 {
			b = append(b, ',')
		}
		b = change.AppendString(b, c.Name)
		b = append(b, ':')
		b = append(b, c.Value...)
	}
	return string(append(b, '}'))
}

func startOpts(slot string, start lsn.LSN, pubs ...string) pipeline.StartOptions {
	return pipeline.StartOptions{Slot: slot, Start: start, ProtoVersion: 1, Publications: pubs, Messages: true}
}

func slotRow(t *testing.T, conn *pgx.Conn, slot string) (confirmed lsn.LSN, activePID int32) {
	t.Helper()
	var text *string
	var pid *int32
	err := conn.QueryRow(context.Background(),
		`SELECT confirmed_flush_lsn::text, active_pid FROM pg_replication_slots WHERE slot_name = $1`, slot).
		Scan(&text, &pid)
	require.NoError(t, err)
	if text != nil {
		var perr error
		confirmed, perr = lsn.Parse(*text)
		require.NoError(t, perr)
	}
	if pid != nil {
		activePID = *pid
	}
	return confirmed, activePID
}

func TestConn_IdentifySystem_ReportsSystem(t *testing.T) {
	t.Parallel()
	db := srv.NewDatabase(t)
	c := newConn(t, db.DSN)
	info, err := c.IdentifySystem(testCtx(t))
	require.NoError(t, err)
	assert.NotEmpty(t, info.SystemID)
	assert.Equal(t, db.Name, info.DBName)
	assert.Equal(t, int32(1), info.Timeline)
	assert.NotZero(t, info.XLogPos)
}

func TestConn_IdentifySystem_WithoutReplicationRoleIsPrivilegeError(t *testing.T) {
	t.Parallel()
	db := srv.NewDatabase(t)
	role := db.CreateRole(t, "norepl", "")
	c := newConn(t, db.RoleDSN(role))
	_, err := c.IdentifySystem(testCtx(t))
	require.ErrorIs(t, err, pgrepl.ErrReplicationPrivilege)
	var perr *pgrepl.Error
	require.ErrorAs(t, err, &perr)
	assert.Equal(t, pgrepl.CodeReplicationPrivilege, perr.ErrorCode())
	assert.Contains(t, err.Error(), "ALTER ROLE "+role+" REPLICATION")
	assert.Contains(t, err.Error(), "rds_replication")
	assert.Contains(t, err.Error(), "cloudsql.logical_decoding=on")

	ok := db.CreateRole(t, "repl", "REPLICATION")
	_, err = newConn(t, db.RoleDSN(ok)).IdentifySystem(testCtx(t))
	require.NoError(t, err)
}

func TestConn_CreateSlot_ExportedSnapshotImportedBeforeStart(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY, v text);
		INSERT INTO items SELECT g, 'v' || g FROM generate_series(1, 3) g;
		CREATE PUBLICATION pub FOR TABLE items`)
	c := newConn(t, db.DSN)
	slot, err := c.CreateSlot(ctx, "wake_export")
	require.NoError(t, err)
	assert.Equal(t, "wake_export", slot.Name)
	require.NotEmpty(t, slot.SnapshotName)
	require.NotZero(t, slot.ConsistentPoint)

	db.Exec(t, `INSERT INTO items VALUES (4, 'v4')`)
	session := db.Conn(t)
	tx, err := session.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+slot.SnapshotName+"'")
	require.NoError(t, err)
	var n int
	require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM items`).Scan(&n))
	assert.Equal(t, 3, n)

	require.NoError(t, c.StartReplication(ctx, startOpts("wake_export", slot.ConsistentPoint, "pub")))
	img := newStream(t, c).nextRow(ctx)
	assert.Equal(t, change.OpCreate, img.Op)
	assert.JSONEq(t, `{"id":4,"v":"v4"}`, rowJSON(img.After))
	require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM items`).Scan(&n))
	assert.Equal(t, 3, n, "the imported snapshot stays in place while streaming")
	require.NoError(t, tx.Rollback(ctx))

	_, err = c.CreateSlot(ctx, "other")
	require.ErrorIs(t, err, pgrepl.ErrStreaming)
}

func TestConn_CreateSlot_ExistingSlotFails(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `SELECT pg_create_logical_replication_slot('wake_dup', 'pgoutput')`)
	_, err := newConn(t, db.DSN).CreateSlot(ctx, "wake_dup")
	require.ErrorIs(t, err, pgrepl.ErrSlotExists)
}

func TestConn_Receive_DecodesInsertUpdateDelete(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY, v text, at timestamptz, d interval);
		CREATE PUBLICATION pub FOR TABLE items`)
	c := newConn(t, db.DSN)
	slot, err := c.CreateSlot(ctx, "wake_stream")
	require.NoError(t, err)
	require.NoError(t, c.StartReplication(ctx, startOpts("wake_stream", slot.ConsistentPoint, "pub")))
	db.Exec(t, `INSERT INTO items VALUES (1, 'a', '2026-10-18 12:34:56.5+02', '1 day 02:00:00')`)
	db.Exec(t, `UPDATE items SET v = 'b' WHERE id = 1`)
	db.Exec(t, `UPDATE items SET id = 2 WHERE id = 1`)
	db.Exec(t, `DELETE FROM items WHERE id = 2`)
	db.Exec(t, `SELECT pg_logical_emit_message(true, 'app.test', 'hello')`)

	s := newStream(t, c)
	begin, ok := s.next(ctx).(*pgoutput.Begin)
	require.True(t, ok)
	rel, ok := s.next(ctx).(*pgoutput.Relation)
	require.True(t, ok)
	assert.Equal(t, "public.items", rel.QualifiedName())
	assert.Equal(t, []string{"id"}, rel.KeyColumnNames())
	img := s.nextRow(ctx)
	assert.Equal(t, change.OpCreate, img.Op)
	assert.JSONEq(t, `{"id":1,"v":"a","at":"2026-10-18T10:34:56.5+00:00","d":"P1DT2H"}`, rowJSON(img.After))
	commit := s.nextCommit(ctx)
	assert.Equal(t, begin.FinalLSN, commit.CommitLSN)

	img = s.nextRow(ctx)
	assert.Equal(t, change.OpUpdate, img.Op)
	assert.JSONEq(t, `{"id":1}`, rowJSON(img.Key))
	img = s.nextRow(ctx)
	assert.Equal(t, change.OpUpdate, img.Op)
	assert.JSONEq(t, `{"id":1}`, rowJSON(img.Before), "key-changing update carries the old key")
	assert.JSONEq(t, `{"id":2}`, rowJSON(img.Key))
	img = s.nextRow(ctx)
	assert.Equal(t, change.OpDelete, img.Op)
	assert.JSONEq(t, `{"id":2}`, rowJSON(img.Key))
	for {
		m := s.next(ctx)
		if lm, ok := m.(*pgoutput.LogicalMessage); ok {
			assert.Equal(t, "app.test", lm.Prefix)
			assert.Equal(t, "hello", string(lm.Content))
			assert.True(t, lm.Transactional())
			break
		}
	}
}

func TestConn_SendStandbyStatus_AdvancesConfirmedFlushWhileReceiveBlocks(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY); CREATE PUBLICATION pub FOR TABLE items`)
	c := newConn(t, db.DSN)
	slot, err := c.CreateSlot(ctx, "wake_status")
	require.NoError(t, err)
	require.NoError(t, c.StartReplication(ctx, startOpts("wake_status", slot.ConsistentPoint, "pub")))
	db.Exec(t, `INSERT INTO items VALUES (1)`)
	s := newStream(t, c)
	commit := s.nextCommit(ctx)

	readCtx, stopRead := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var readErr error
	wg.Go(func() {
		for {
			msg, err := c.Receive(readCtx)
			if err != nil {
				readErr = err
				return
			}
			if msg.Kind == pipeline.ReplicationMessageXLogData {
				readErr = errors.New("unexpected data on an idle stream")
				return
			}
		}
	})
	admin := db.Conn(t)
	require.NoError(t, c.SendStandbyStatus(ctx, pipeline.StandbyStatus{Written: commit.EndLSN, Flushed: commit.EndLSN}))
	require.Eventually(t, func() bool {
		confirmed, _ := slotRow(t, admin, "wake_status")
		return confirmed >= commit.EndLSN
	}, waitTimeout, 50*time.Millisecond)
	stopRead()
	wg.Wait()
	require.ErrorIs(t, readErr, context.Canceled)

	db.Exec(t, `INSERT INTO items VALUES (2)`)
	img := s.nextRow(ctx)
	assert.JSONEq(t, `{"id":2}`, rowJSON(img.After))
}

func TestConn_Receive_KeepaliveReplyKeepsStreamAlive(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY); CREATE PUBLICATION pub FOR TABLE items`)
	c := newConn(t, db.DSN, pgrepl.WithRuntimeParam("wal_sender_timeout", "2s"))
	slot, err := c.CreateSlot(ctx, "wake_keepalive")
	require.NoError(t, err)
	require.NoError(t, c.StartReplication(ctx, startOpts("wake_keepalive", slot.ConsistentPoint, "pub")))

	replies := 0
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		rctx, cancel := context.WithDeadline(ctx, until)
		msg, err := c.Receive(rctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		require.NoError(t, err)
		if msg.Kind == pipeline.ReplicationMessageKeepalive && msg.Keepalive.ReplyRequested {
			replies++
			require.NoError(t, c.SendStandbyStatus(ctx, pipeline.StandbyStatus{
				Written: msg.Keepalive.ServerWALEnd, Flushed: slot.ConsistentPoint,
			}))
		}
	}
	assert.GreaterOrEqual(t, replies, 2)
	db.Exec(t, `INSERT INTO items VALUES (1)`)
	img := newStream(t, c).nextRow(ctx)
	assert.JSONEq(t, `{"id":1}`, rowJSON(img.After))
}

func TestConn_StartReplication_SecondConsumerIsSlotInUse(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE items (id int PRIMARY KEY); CREATE PUBLICATION pub FOR TABLE items`)
	first := newConn(t, db.DSN)
	slot, err := first.CreateSlot(ctx, "wake_busy")
	require.NoError(t, err)
	require.NoError(t, first.StartReplication(ctx, startOpts("wake_busy", slot.ConsistentPoint, "pub")))
	_, pid := slotRow(t, db.Conn(t), "wake_busy")
	require.NotZero(t, pid)

	second := newConn(t, db.DSN)
	err = second.StartReplication(ctx, startOpts("wake_busy", slot.ConsistentPoint, "pub"))
	require.ErrorIs(t, err, pgrepl.ErrSlotInUse)
	var perr *pgrepl.Error
	require.ErrorAs(t, err, &perr)
	assert.Equal(t, pgrepl.CodeSlotInUse, perr.Code)
	assert.Equal(t, pid, perr.PID)
	assert.Contains(t, err.Error(), "pid")

	err = second.DropSlot(ctx, "wake_busy")
	require.ErrorIs(t, err, pgrepl.ErrSlotInUse)
	_, err = second.IdentifySystem(ctx)
	require.NoError(t, err)
}

func TestConn_DropSlot_DropsAndIgnoresAbsent(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	c := newConn(t, db.DSN)
	_, err := c.CreateSlot(ctx, "wake_drop")
	require.NoError(t, err)
	require.NoError(t, c.DropSlot(ctx, "wake_drop"))
	var n int
	require.NoError(t, db.Conn(t).QueryRow(ctx,
		`SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'wake_drop'`).Scan(&n))
	assert.Zero(t, n)
	require.NoError(t, c.DropSlot(ctx, "wake_drop"))
	require.ErrorIs(t, c.DropSlot(ctx, "Bad-Name"), pgrepl.ErrInvalidArgument)
}

func TestConn_StartReplication_MissingSlot(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE PUBLICATION pub`)
	err := newConn(t, db.DSN).StartReplication(ctx, startOpts("wake_none", 0, "pub"))
	require.ErrorIs(t, err, pgrepl.ErrSlotNotFound)
}

func TestConn_Lifecycle_StateErrors(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	c := newConn(t, db.DSN)
	_, err := c.Receive(ctx)
	require.ErrorIs(t, err, pgrepl.ErrNotStreaming)
	require.ErrorIs(t, c.SendStandbyStatus(ctx, pipeline.StandbyStatus{}), pgrepl.ErrNotStreaming)
	require.NoError(t, c.Close(ctx))
	_, err = c.IdentifySystem(ctx)
	require.ErrorIs(t, err, pgrepl.ErrClosed)
	_, err = c.Receive(ctx)
	require.ErrorIs(t, err, pgrepl.ErrClosed)
	require.NoError(t, c.Close(ctx))
}

func TestConn_StartReplication_InvalidatedSlotIsSlotInvalidated(t *testing.T) { //nolint:paralleltest // see above
	ctx := testCtx(t)
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE w (v text); CREATE PUBLICATION pub FOR TABLE w`)
	creator := newConn(t, db.DSN)
	slot, err := creator.CreateSlot(ctx, "wake_lost")
	require.NoError(t, err)
	require.NoError(t, creator.Close(ctx))
	db.Exec(t, `ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'`)
	db.Exec(t, `SELECT pg_reload_conf()`)
	t.Cleanup(func() {
		db.Exec(t, `ALTER SYSTEM RESET max_slot_wal_keep_size`)
		db.Exec(t, `SELECT pg_reload_conf()`)
	})
	admin := db.Conn(t)
	require.Eventually(t, func() bool {
		for _, sql := range []string{
			`INSERT INTO w SELECT repeat('x', 1000) FROM generate_series(1, 100)`, `SELECT pg_switch_wal()`,
			`CHECKPOINT`,
		} {
			_, xerr := admin.Exec(ctx, sql)
			require.NoError(t, xerr)
		}
		var status string
		require.NoError(t, admin.QueryRow(ctx,
			`SELECT coalesce(wal_status, '') FROM pg_replication_slots WHERE slot_name = 'wake_lost'`).Scan(&status))
		return status == "lost"
	}, time.Minute, 10*time.Millisecond)

	err = newConn(t, db.DSN).StartReplication(ctx, startOpts("wake_lost", slot.ConsistentPoint, "pub"))
	require.ErrorIs(t, err, pgrepl.ErrSlotInvalidated)
	var perr *pgrepl.Error
	require.ErrorAs(t, err, &perr)
	assert.Equal(t, pgrepl.CodeSlotInvalidated, perr.ErrorCode())
	assert.Equal(t, "wake_lost", perr.Slot)
}
