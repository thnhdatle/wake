// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/internal/testutil/m2"
	"github.com/thnhdatle/wake/pkg/event"
)

// WAK-15 scenario parameters. The criterion fixes the new table's size, the
// rate and the single kill; the tables, the split of the load, the kill
// point and the tails are chosen by the test. The new table is created and filled in one
// transaction while Wake runs; Wake detects it at tables.refresh_interval,
// adds it to the managed publication and snapshots it.
const (
	wak15Seed    = uint64(15)
	wak15Conns   = 16
	wak15Timeout = 30 * time.Minute
	wak15MinRate = 0.98
	// wak15Warmup runs the load on the existing tables before the add.
	wak15Warmup = 10 * time.Second
	// wak15Tail runs the load after the resumed snapshot finished.
	wak15Tail = 20 * time.Second
	// wak15KillAt is the share of the new table's rows acknowledged when
	// the kill is sent.
	wak15KillAt = 0.2
	// wak15AddedShare is the share of the load that writes to the new table
	// once it exists.
	wak15AddedShare = 3 // one transaction in three
	// wak15Chunks is the number of snapshot chunks of the added table, so the
	// kill point lies many chunks past the start whatever the table size.
	wak15Chunks = 100
)

// wak15ChunkRows is snapshot.chunk_rows for a table of rows rows.
func wak15ChunkRows(rows int64) int64 { return max(rows/wak15Chunks, 100) }

var (
	wak15A = verTable{Name: "wak15.accounts", Keys: 20000, PayloadBytes: 128, DeletePct: 10}
	wak15B = verTable{Name: "wak15.orders", Keys: 20000, PayloadBytes: 128, DeletePct: 10}
)

// wak15Added is the table added to the running pipeline; Seeded and Keys
// are set from the criterion's row count (the load also inserts new ids).
func wak15Added(rows int64) verTable {
	return verTable{Name: "wak15.added", Keys: rows + rows/5, PayloadBytes: 128, DeletePct: 10, Seeded: rows}
}

// wak15Tx writes to the existing tables and, once added is set, one
// transaction in wak15AddedShare to the added table.
func wak15Tx(rows int64, added *atomic.Bool) stack.TxFunc {
	existing := newVerTx(wak15Conns, wak15A, wak15B)
	toAdded := newVerTx(wak15Conns, wak15Added(rows))
	return func(ctx context.Context, conn *pgx.Conn, worker int, rng *rand.Rand) ([]stack.Change, error) {
		if added.Load() && rng.IntN(wak15AddedShare) == 0 {
			return toAdded(ctx, conn, worker, rng)
		}
		return existing(ctx, conn, worker, rng)
	}
}

// wak15Kill is what happened around the kill.
type wak15Kill struct {
	seen       time.Time // the added table's run first listed
	killedAt   time.Time
	rowsAtKill uint64 // acknowledged rows of the added table at the kill
	// rowsAfter is the progress of the added table at the first status after
	// the restart; -1 when the run was already gone.
	rowsAfter int64
	done      time.Time // the resumed snapshot finished
}

// tableRun returns the progress of table in any snapshot run and whether it
// is listed.
func tableRun(st stack.Status, table string) (rows uint64, done, listed bool) {
	for _, r := range st.Snapshot.Runs {
		for _, tb := range r.Tables {
			if tb.Table == table {
				return tb.Rows, tb.Done, true
			}
		}
	}
	return 0, false, false
}

// killMidSnapshot waits until the added table's run acknowledged
// wak15KillAt of its rows, kills Wake (kill -9), starts the same container
// again and waits for the resumed snapshot to finish.
func killMidSnapshot(ctx context.Context, wake *stack.Wake, table string, rows int64) (wak15Kill, error) {
	k := wak15Kill{rowsAfter: -1}
	var err error
	k.seen, k.rowsAtKill, err = waitKillPoint(ctx, wake, table, uint64(float64(rows)*wak15KillAt))
	if err != nil {
		return k, err
	}
	if err := wake.Kill(ctx); err != nil {
		return k, fmt.Errorf("kill wake: %w", err)
	}
	k.killedAt = time.Now()
	if err := wake.Start(ctx); err != nil {
		return k, fmt.Errorf("start wake after the kill: %w", err)
	}
	if st, err := wake.Admin().Status(ctx); err == nil {
		if n, _, listed := tableRun(st, table); listed {
			k.rowsAfter = int64(n) //nolint:gosec // row counts fit
		}
	}
	if err := waitSnapshotDone(ctx, wake); err != nil {
		return k, err
	}
	k.done = time.Now()
	return k, nil
}

// waitKillPoint polls /v1/status every 50 ms until table's run acknowledged
// threshold rows; it fails when the run finishes first.
func waitKillPoint(ctx context.Context, wake *stack.Wake, table string, threshold uint64,
) (seen time.Time, rows uint64, err error) {
	for {
		st, serr := wake.Admin().Status(ctx)
		if serr == nil {
			n, done, listed := tableRun(st, table)
			if listed && seen.IsZero() {
				seen = time.Now()
			}
			switch {
			case done:
				return seen, n, fmt.Errorf("the snapshot of %s finished (%d rows) before the kill point %d",
					table, n, threshold)
			case listed && n >= threshold:
				return seen, n, nil
			}
		}
		select {
		case <-ctx.Done():
			return seen, 0, fmt.Errorf("snapshot of %s not at the kill point (last status error %w): %w",
				table, serr, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestAcceptance_WAK15_AddTableUnderLoad implements WAK-15: Add a 500k-row table to a running pipeline under
// 500 tx/s, kill -9 mid-snapshot, resume.
func TestAcceptance_WAK15_AddTableUnderLoad(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-15"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	th := readThresholds(t, id, c, "rows", "tx_per_s", "kills", "existing_tables_lost_max")
	matchMin, err := c.Float("state_match_pct_min")
	failIf(t, id, err, "threshold state_match_pct_min")
	if th["kills"] != 1 {
		harness.Failf(t, id, "the scenario sends exactly one kill, criterion asks for %d", th["kills"])
	}
	added := wak15Added(th["rows"])

	stack.EnsureServices(t, "postgres", "kafka")
	pg := stack.ConnectPG(t, 2)
	ctx := withTimeout(t, wak15Timeout)
	pg.Exec(t, "DROP SCHEMA IF EXISTS wak15 CASCADE; CREATE SCHEMA wak15; "+verTableDDL(wak15A)+verTableDDL(wak15B))
	t.Cleanup(func() { dropSchema(t, pg, "wak15") })

	prefix := fmt.Sprintf("w15_%d", time.Now().Unix())
	topics := []string{
		stack.Topic(prefix, "wak15", "accounts"), stack.Topic(prefix, "wak15", "orders"),
		stack.Topic(prefix, "wak15", "added"),
	}
	t.Cleanup(func() { dropTopics(t, append(topics, stack.SchemaTopic(prefix))...) })
	snap := fmt.Sprintf("snapshot:\n  added_tables: true\n  chunk_rows: %d", wak15ChunkRows(th["rows"]))
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID: id, Config: wakeConfig("wak15", []string{"wak15.*"}, kafkaSink(prefix), snap), FreshState: true,
	})
	failIf(t, id, waitSnapshotDone(ctx, wake), "wait for the initial snapshot of the existing tables")

	var addedOn atomic.Bool
	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: float64(th["tx_per_s"]), Conns: wak15Conns, Seed: wak15Seed, Tx: wak15Tx(th["rows"], &addedOn),
	})
	failIf(t, id, err, "start load")
	t.Cleanup(load.Abort)
	time.Sleep(wak15Warmup)

	addStart := time.Now()
	_, err = pg.ExecContext(ctx, fmt.Sprintf(`BEGIN;
%s
INSERT INTO wak15.added SELECT g, 1, repeat(md5(g::text), %d) FROM generate_series(1, %d) g;
COMMIT;
ANALYZE wak15.added;`, verTableDDL(added), max(added.PayloadBytes/32, 1), th["rows"]))
	failIf(t, id, err, "create and fill the added table")
	addedOn.Store(true)
	addDur := time.Since(addStart)

	kill, err := killMidSnapshot(ctx, wake, added.Name, th["rows"])
	failIf(t, id, err, "kill Wake mid-snapshot and resume")
	time.Sleep(wak15Tail)
	load.Stop()
	res := load.Wait()
	loadEnd := time.Now()
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN")
	drain := time.Since(loadEnd)

	finishWak15(ctx, t, id, th, matchMin, wak15Run{
		pg: pg, res: res, kill: kill, topics: topics, added: added, addStart: addStart, addDur: addDur,
		drain: drain,
	})
}

// wak15Run is what the scenario recorded for the evaluation.
type wak15Run struct {
	pg       *stack.PG
	res      stack.LoadResult
	kill     wak15Kill
	topics   []string
	added    verTable
	addStart time.Time
	addDur   time.Duration
	drain    time.Duration
}

// finishWak15 applies M2 to the added table and M3 to the existing ones and
// records the result.
func finishWak15(ctx context.Context, t *testing.T, id string, th map[string]int64, matchMin float64, r wak15Run) {
	t.Helper()
	recs, err := stack.ReadTopics(ctx, r.topics...)
	failIf(t, id, err, "read Kafka topics")
	addedTopic := r.topics[len(r.topics)-1]
	lost, err := existingLost(recs, addedTopic, r.res.Changes, r.added.Name)
	failIf(t, id, err, "M3 lost on the existing tables")
	cmp, err := compareAdded(ctx, r.pg, recs, addedTopic, r.added.Name)
	failIf(t, id, err, "M2 on the added table")
	missing, extra, diff, st := cmp.missing, cmp.extra, cmp.diff, cmp.stats
	match := stack.Pct(cmp.pgRows-missing, cmp.pgRows+extra)
	reads, rereads, err := readCounts(recs, addedTopic)
	failIf(t, id, err, "count snapshot reads")

	k := r.kill
	achieved := r.res.AchievedRate()
	loadOK := r.res.Errors == 0 && r.res.Dropped == 0 && achieved >= wak15MinRate*float64(th["tx_per_s"])
	// Resumed from the last acknowledged key: only the chunks between the
	// last saved position and the kill were read again (a few chunks), while
	// a restart from the first key would read again nearly every row
	// acknowledged before the kill (all but the few the load deleted). The
	// progress at the first status after the restart is informational: a
	// small table can finish before that status is read.
	resumed := k.rowsAtKill > 0 && 2*uint64(rereads) < k.rowsAtKill //nolint:gosec // non-negative count

	midSnapshot := k.rowsAtKill > 0 && k.rowsAtKill < uint64(th["rows"]) //nolint:gosec // positive threshold
	pass := match >= matchMin && missing == 0 && int64(len(lost.Lost)) <= th["existing_tables_lost_max"] &&
		loadOK && resumed && midSnapshot

	details := fmt.Sprintf("load %s; %s created and filled with %d rows in %s; its run listed %s after the "+
		"commit; kill -9 at %d acknowledged rows (%s after the commit); progress after the restart %d; resumed "+
		"snapshot done %s after the kill; final LSN confirmed %s after the load ended; snapshot reads of the "+
		"added table %d (read again %d); M2 added table: Postgres rows %d, sink rows %d, missing or different %d, "+
		"extra %d (events %d, duplicates %d, tombstones %d); M3 existing tables: recorded %d, lost %d, "+
		"unmatched %d; topics %s",
		r.res.Summary(), r.added.Name, th["rows"], r.addDur.Round(time.Millisecond),
		sinceOrNone(k.seen, r.addStart.Add(r.addDur)), k.rowsAtKill,
		sinceOrNone(k.killedAt, r.addStart.Add(r.addDur)), k.rowsAfter, sinceOrNone(k.done, k.killedAt),
		r.drain.Round(time.Millisecond), reads, rereads, cmp.pgRows, cmp.sinkRows, missing, extra,
		st.Events, st.Duplicates, st.Tombstones, lost.Recorded, len(lost.Lost), lost.Unmatched,
		strings.Join(r.topics, ", "))
	if len(diff) > 0 {
		details += "; first differences: " + strings.Join(diff[:min(len(diff), 10)], "; ")
	}
	if len(lost.Lost) > 0 {
		details += "; first lost: " + describeChanges(lost.Lost)
	}
	if len(r.res.FirstErrs) > 0 {
		details += "; load errors: " + strings.Join(r.res.FirstErrs, "; ")
	}
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"rows": th["rows"], "tx_per_s": th["tx_per_s"], "tx_per_s_achieved": achieved, "kills": 1,
			"state_match_pct": match, "missing": missing, "extra": extra,
			"existing_tables_lost": len(lost.Lost), "existing_tables_recorded": lost.Recorded,
			"rows_acked_at_kill": k.rowsAtKill, "rows_after_restart": k.rowsAfter, "snapshot_reads": reads,
			"snapshot_rows_read_again": rereads, "resumed": resumed, "drain_after_load_s": r.drain.Seconds(),
		},
		Pass:    pass,
		Details: details,
	})
}

// existingLost applies M3 to the changes of every table but added, over the
// records of every topic but addedTopic.
func existingLost(recs []stack.Record, addedTopic string, changes []stack.Change, added string,
) (stack.LostResult, error) {
	var vals [][]byte
	for _, rec := range recs {
		if rec.Topic != addedTopic {
			vals = append(vals, rec.Value)
		}
	}
	dec, err := stack.DecodePayloads(vals)
	if err != nil {
		return stack.LostResult{}, err //nolint:wrapcheck // stack errors carry context
	}
	var existing []stack.Change
	for _, ch := range changes {
		if ch.Table != added {
			existing = append(existing, ch)
		}
	}
	return stack.FindLost(existing, dec.Events, "ver") //nolint:wrapcheck // stack errors carry context
}

// addedComparison is the M2 outcome for the added table.
type addedComparison struct {
	pgRows, sinkRows, missing, extra int
	diff                             []string
	stats                            m2.Stats
}

// compareAdded replays addedTopic (M2) and compares it with the table.
func compareAdded(ctx context.Context, pg *stack.PG, recs []stack.Record, addedTopic, table string,
) (addedComparison, error) {
	var (
		cmp addedComparison
		rp  m2.Replayer
	)
	for _, rec := range recs {
		if rec.Topic != addedTopic {
			continue
		}
		if err := rp.Add(rec.Value); err != nil {
			return cmp, err //nolint:wrapcheck // m2 errors carry context
		}
	}
	want, err := pgTableRows(ctx, pg, table)
	if err != nil {
		return cmp, err
	}
	state, err := rp.State()
	if err != nil {
		return cmp, err //nolint:wrapcheck // m2 errors carry context
	}
	cmp.diff = m2.Diff(state[table], want)
	cmp.missing, cmp.extra = diffCounts(cmp.diff)
	cmp.pgRows, cmp.sinkRows, cmp.stats = len(want), len(state[table]), rp.Stats()
	return cmp, nil
}

// readCounts counts the distinct r events of a topic and how many of them
// read a key that an earlier r event had read already.
func readCounts(recs []stack.Record, topic string) (reads, rereads int, err error) {
	var vals [][]byte
	for _, r := range recs {
		if r.Topic == topic {
			vals = append(vals, r.Value)
		}
	}
	dec, err := stack.DecodePayloads(vals)
	if err != nil {
		return 0, 0, err //nolint:wrapcheck // stack errors carry context
	}
	ids, keys := map[string]bool{}, map[string]bool{}
	for _, e := range dec.Events {
		if e.Op != event.OpRead || ids[e.ID] {
			continue
		}
		ids[e.ID] = true
		reads++
		key, kerr := stack.EventKey(e)
		if kerr != nil {
			return 0, 0, kerr //nolint:wrapcheck // stack errors carry context
		}
		if keys[key] {
			rereads++
		}
		keys[key] = true
	}
	return reads, rereads, nil
}
