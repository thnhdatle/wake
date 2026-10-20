// SPDX-License-Identifier: Apache-2.0

package pgtest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
)

// Replication is a raw logical replication client for tests that must not
// depend on the pgrepl adapter: it creates slots with an exported snapshot,
// starts pgoutput streaming and decodes messages with pgoutput.Decoder.
type Replication struct {
	conn *pgconn.PgConn
	// Decoder holds the relation cache of the stream.
	Decoder *pgoutput.Decoder
}

// Replication opens a replication connection (replication=database) to the
// database as the superuser, closed when the test ends.
func (d *Database) Replication(tb testing.TB) *Replication {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	conn, err := pgconn.Connect(ctx, d.DSN+"&replication=database&DateStyle=ISO&IntervalStyle=postgres&TimeZone=UTC")
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()
		_ = conn.Close(ctx) //nolint:errcheck // cleanup
	})
	return &Replication{conn: conn, Decoder: pgoutput.NewDecoder()}
}

// CreateSlot creates a pgoutput slot with EXPORT_SNAPSHOT and returns the
// snapshot name and consistent point.
func (r *Replication) CreateSlot(tb testing.TB, slot string) (snapshot string, cp lsn.LSN) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	res, err := pglogrepl.CreateReplicationSlot(ctx, r.conn, slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT"})
	require.NoError(tb, err)
	cp, err = lsn.Parse(res.ConsistentPoint)
	require.NoError(tb, err)
	return res.SnapshotName, cp
}

// Start runs START_REPLICATION with proto_version 1, the publications and
// messages 'true'.
func (r *Replication) Start(tb testing.TB, slot string, start lsn.LSN, publications ...string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	err := pglogrepl.StartReplication(ctx, r.conn, slot, pglogrepl.LSN(start), pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '1'", fmt.Sprintf("publication_names '%s'", strings.Join(publications, ",")),
			"messages 'true'",
		},
	})
	require.NoError(tb, err)
}

// Next returns the next decoded pgoutput message, answering keepalives. It
// fails the test after timeout.
func (r *Replication) Next(tb testing.TB, timeout time.Duration) pgoutput.Message {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		msg, err := r.conn.ReceiveMessage(ctx)
		require.NoError(tb, err)
		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			require.Failf(tb, "unexpected message", "%T", msg)
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			require.NoError(tb, err)
			if ka.ReplyRequested {
				require.NoError(tb, pglogrepl.SendStandbyStatusUpdate(ctx, r.conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: ka.ServerWALEnd}))
			}
		case pglogrepl.XLogDataByteID:
			x, err := pglogrepl.ParseXLogData(cd.Data[1:])
			require.NoError(tb, err)
			m, err := r.Decoder.Decode(x.WALData)
			require.NoError(tb, err)
			return m
		}
	}
}

// NextRow returns the row images of the next Insert, Update or Delete
// (skipping other messages) and its relation.
func (r *Replication) NextRow(tb testing.TB, timeout time.Duration) (pgoutput.RowImages, uint32) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for {
		m := r.Next(tb, time.Until(deadline))
		var oid uint32
		switch v := m.(type) {
		case *pgoutput.Insert:
			oid = v.RelationOID
		case *pgoutput.Update:
			oid = v.RelationOID
		case *pgoutput.Delete:
			oid = v.RelationOID
		default:
			continue
		}
		img, err := r.Decoder.RowImages(m)
		require.NoError(tb, err)
		return img, oid
	}
}
