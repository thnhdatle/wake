// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/statefile"
	"github.com/thnhdatle/wake/internal/config"
)

// testKillDuringInitialSnapshot checks `kill -9` during the initial
// snapshot while writers run, restart, final sink state equals Postgres.
// The restarted process keeps the slot, releases the stream at once and
// resumes the unfinished table through watermarks under the same run_id.
func testKillDuringInitialSnapshot(e *env) {
	t := e.t
	e.createItems("items", 60_000)
	stop := e.startWriter("items", 10_000_000, 2)
	cfg := e.config()
	cfg.Snapshot.ChunkRows = 100

	p := e.startWake(&cfg)
	p.waitFor(waitLong, "initial snapshot progress", func() bool {
		doc, ok := p.status()
		if !ok || len(doc.Snapshot.Runs) == 0 {
			return false
		}
		for _, tb := range doc.Snapshot.Runs[0].Tables {
			if tb.Rows > 0 {
				return true
			}
		}
		return false
	})
	p.kill()

	st, err := mustStore(t, cfg.State.Path).Load(context.Background())
	require.NoError(t, err)
	require.Len(t, st.Snapshot.Runs, 1, "killed before the initial snapshot finished")
	run := st.Snapshot.Runs[0]
	require.False(t, run.Tables[0].Done)
	t.Logf("killed at run %d with %d of 60000+ rows acknowledged", run.ID, run.Tables[0].Rows)
	p.waitFor(waitShort, "slot released by the killed process", func() bool {
		_, active, _ := e.slotRow()
		return !active
	})

	p2 := e.startWake(&cfg)
	p2.waitSnapshotsIdle()
	p2.waitReady()
	stop()
	e.sync(p2)

	e.assertSinkEqualsSource("items")
	after, err := mustStore(t, cfg.State.Path).Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, st.ConsistentPoint, after.ConsistentPoint, "the slot was kept")
	assert.Equal(t, run.ID, after.RunCounter, "the resumed run kept its run_id; no new run")
	p2.stop()
}

func mustStore(t *testing.T, path string) *statefile.Store {
	t.Helper()
	s, err := statefile.New(path)
	require.NoError(t, err)
	return s
}

// testAddTableUnderLoad checks that a table created while the pipeline
// runs is published by the managed refresh and snapshotted through
// watermarks while writers keep writing to it and to an existing table.
// Afterwards `wake snapshot table` re-snapshots an existing table.
func testAddTableUnderLoad(e *env) {
	t := e.t
	e.createItems("a", 500)
	stopA := e.startWriter("a", 1_000_000, 3)
	cfg := e.config()

	p := e.startWake(&cfg)
	p.waitSnapshotsIdle()
	e.createItems("b", 3000)
	stopB := e.startWriter("b", 1_000_000, 4)
	p.waitFor(waitLong, "snapshot of the added table", func() bool {
		return p.metric(`wake_snapshot_rows_total{table="public.b"}`) > 0
	})
	p.waitSnapshotsIdle()
	stopA()
	stopB()
	e.sync(p)
	e.assertSinkEqualsSource("a", "b")

	rowsA := `wake_snapshot_rows_total{table="public.a"}`
	before := p.metric(rowsA)
	code, out := e.runCLI("snapshot", "table", "public.a", "--config", e.writeConfig(&cfg))
	require.Equal(t, 0, code, out)
	assert.Contains(t, out, "snapshot of public.a requested")
	p.waitFor(waitLong, "re-snapshot of public.a", func() bool {
		return p.metric(`wake_snapshot_requests_total{table="public.a"}`) >= 1 && p.metric(rowsA) > before
	})
	p.waitSnapshotsIdle()
	e.sync(p)
	e.assertSinkEqualsSource("a", "b")
	p.stop()
}

// testSIGHUPReloadsGlobs checks table detection on SIGHUP: the
// reloaded tables.include adds a table to the managed publication and it is
// snapshotted without waiting for tables.refresh_interval.
func testSIGHUPReloadsGlobs(e *env) {
	t := e.t
	e.createItems("a", 100)
	e.createItems("b", 1000)
	cfg := e.config()
	cfg.Tables.Include = []string{"public.a"}
	cfg.Tables.RefreshInterval = config.Duration(time.Hour)
	p := e.startWake(&cfg)
	p.waitSnapshotsIdle()
	require.Zero(t, e.published("wake_"+e.name, "b"))

	cfg.Tables.Include = []string{"public.*"}
	e.rewriteConfig(p.cfgPath, &cfg)
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGHUP))

	p.waitFor(waitLong, "snapshot of public.b after SIGHUP", func() bool {
		return p.metric(`wake_snapshot_rows_total{table="public.b"}`) > 0
	})
	p.waitSnapshotsIdle()
	e.sync(p)
	assert.Equal(t, 1, e.published("wake_"+e.name, "b"))
	e.assertSinkEqualsSource("a", "b")
	p.stop()
}
