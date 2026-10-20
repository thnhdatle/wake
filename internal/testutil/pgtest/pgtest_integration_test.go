// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgtest_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/testutil/pgtest"
)

var srv *pgtest.Server

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &srv)) }

func TestServer_DatabaseRolesAndReplication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := srv.NewDatabase(t)
	db.Exec(t, `CREATE TABLE t (id int PRIMARY KEY); CREATE PUBLICATION pub FOR TABLE t`)
	var level string
	require.NoError(t, db.Conn(t).QueryRow(ctx, `SHOW wal_level`).Scan(&level))
	assert.Equal(t, "logical", level)

	role := db.CreateRole(t, "reader", "REPLICATION")
	var user string
	require.NoError(t, pgtest.Connect(t, db.RoleDSN(role)).QueryRow(ctx, `SELECT current_user`).Scan(&user))
	assert.Equal(t, role, user)

	repl := db.Replication(t)
	snap, cp := repl.CreateSlot(t, "pgtest_slot")
	assert.NotEmpty(t, snap)
	assert.NotZero(t, cp)
	repl.Start(t, "pgtest_slot", cp, "pub")
	db.Exec(t, `INSERT INTO t VALUES (1)`)
	img, oid := repl.NextRow(t, 30*time.Second)
	assert.NotZero(t, oid)
	assert.Equal(t, `1`, string(img.After[0].Value))
	db.Exec(t, `SELECT pg_logical_emit_message(true, 'p', 'x')`)
	for {
		if _, ok := repl.Next(t, 30*time.Second).(*pgoutput.LogicalMessage); ok {
			break
		}
	}
}
