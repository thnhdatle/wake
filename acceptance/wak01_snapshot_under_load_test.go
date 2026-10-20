// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/internal/testutil/m2"
)

// WAK-01 scenario parameters. The criterion fixes the table size, the rate
// and the load window around the snapshot; the operation mix, the number of
// connections and the snapshot trigger are chosen by the test. The snapshot is the initial
// snapshot: Wake starts with a fresh slot while the load runs, so the
// snapshot starts after the load has run load_before_s, and ends when
// /v1/status reports the initial run's table done (every chunk acknowledged).
const (
	wak01Table   = "wak01.items"
	wak01Seed    = uint64(1)
	wak01Conns   = 32
	wak01Timeout = 50 * time.Minute
	// wak01MinRate is the share of the offered rate the load must achieve
	// for the run to count: the criterion is about writes at that rate.
	wak01MinRate = 0.98
	// wak01NewIDBase separates ids created by the load (inserts and
	// key-changing updates) from the seeded ones; each worker owns the ids
	// congruent to its index modulo wak01Conns.
	wak01NewIDBase = 100_000_000
)

// wak01Tx returns the load body: random single-row transactions over the
// seeded ids and the ids the worker created, 30% inserts (some reinsert a
// deleted seeded id, upserting), 45% updates (5% of all transactions change
// the key), 25% deletes. Updates and deletes of absent rows change nothing.
func wak01Tx(rows int64) stack.TxFunc {
	next := make([]int64, wak01Conns)
	return func(ctx context.Context, conn *pgx.Conn, worker int, rng *rand.Rand) ([]stack.Change, error) {
		newID := func() int64 {
			next[worker]++
			return wak01NewIDBase + int64(worker) + wak01Conns*next[worker]
		}
		anyID := func() int64 {
			if next[worker] > 0 && rng.IntN(4) == 0 {
				return wak01NewIDBase + int64(worker) + wak01Conns*(1+rng.Int64N(next[worker]))
			}
			return 1 + rng.Int64N(rows)
		}
		var err error
		switch p := rng.IntN(100); {
		case p < 20:
			_, err = conn.Exec(ctx, `INSERT INTO wak01.items (id, ver, payload, n) VALUES ($1, 1, md5(random()::text), $2)`,
				newID(), rng.Int32())
		case p < 30:
			_, err = conn.Exec(ctx, `INSERT INTO wak01.items (id, ver, payload, n) VALUES ($1, 1, md5(random()::text), $2)
				ON CONFLICT (id) DO UPDATE SET ver = wak01.items.ver + 1, payload = excluded.payload, n = excluded.n`,
				1+rng.Int64N(rows), rng.Int32())
		case p < 70:
			_, err = conn.Exec(ctx, `UPDATE wak01.items SET ver = ver + 1, payload = md5(random()::text), n = $2
				WHERE id = $1`, anyID(), rng.Int32())
		case p < 75:
			_, err = conn.Exec(ctx, "UPDATE wak01.items SET id = $2, ver = ver + 1 WHERE id = $1", anyID(), newID())
		default:
			_, err = conn.Exec(ctx, "DELETE FROM wak01.items WHERE id = $1", anyID())
		}
		return nil, err //nolint:wrapcheck // reported in the load summary
	}
}

// TestAcceptance_WAK01_SnapshotUnderLoad implements WAK-01: Snapshot of a 1M-row table under 1,000 tx/s of
// mixed writes; final sink state equals Postgres.
func TestAcceptance_WAK01_SnapshotUnderLoad(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-01"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	th := readThresholds(t, id, c, "missing_max", "rows", "tx_per_s", "load_before_s", "load_after_s")
	matchMin, err := c.Float("state_match_pct_min")
	failIf(t, id, err, "threshold state_match_pct_min")
	before := time.Duration(th["load_before_s"]) * time.Second
	after := time.Duration(th["load_after_s"]) * time.Second

	stack.EnsureServices(t, "postgres", "kafka")
	pg := stack.ConnectPG(t, 2)
	ctx := withTimeout(t, wak01Timeout)
	loadStart := time.Now()
	_, err = pg.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS wak01 CASCADE;
CREATE SCHEMA wak01;
CREATE TABLE wak01.items (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text, n integer);
INSERT INTO wak01.items SELECT g, 1, md5(g::text), g FROM generate_series(1, %d) g;
ANALYZE wak01.items;`, th["rows"]))
	failIf(t, id, err, "create and fill the table")
	t.Cleanup(func() { dropSchema(t, pg, "wak01") })
	t.Logf("filled %s with %d rows in %s", wak01Table, th["rows"], time.Since(loadStart).Round(time.Millisecond))

	prefix := fmt.Sprintf("w01_%d", time.Now().Unix())
	topic := stack.Topic(prefix, "wak01", "items")
	t.Cleanup(func() { dropTopics(t, topic, stack.SchemaTopic(prefix)) })

	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: float64(th["tx_per_s"]), Conns: wak01Conns, Seed: wak01Seed, Tx: wak01Tx(th["rows"]),
	})
	failIf(t, id, err, "start load")
	t.Cleanup(load.Abort)
	loadStart = time.Now()
	time.Sleep(before)

	// The snapshot cannot start before Wake does: the slot and its exported
	// snapshot are created at startup.
	wakeStart := time.Now()
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID: id, Config: wakeConfig("wak01", []string{"wak01.*"}, kafkaSink(prefix), wak01Snapshot),
		FreshState: true, NoWait: true,
	})
	win, err := watchInitialSnapshot(ctx, wake)
	failIf(t, id, err, "observe the initial snapshot")
	time.Sleep(after)
	load.Stop()
	res := load.Wait()
	loadEnd := time.Now()
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN")
	drain := time.Since(loadEnd)
	snapRows, err := snapshotRowsMetric(ctx, wake, wak01Table)
	failIf(t, id, err, "read wake_snapshot_rows_total")

	want, err := pgTableRows(ctx, pg, wak01Table)
	failIf(t, id, err, "read the table from Postgres")
	recs, err := stack.ReadTopics(ctx, topic)
	failIf(t, id, err, "read Kafka topic")
	var rp m2.Replayer
	reads := 0
	for _, r := range recs {
		failIf(t, id, rp.Add(r.Value), "M2 replay")
		if bytes.Contains(r.Value, []byte(`"op":"r"`)) {
			reads++
		}
	}
	nRecs := len(recs)
	state, err := rp.State()
	failIf(t, id, err, "M2 state")
	diff := m2.Diff(state[wak01Table], want)
	missing, extra := diffCounts(diff)
	match := stack.Pct(len(want)-missing, len(want)+extra)
	achieved := res.AchievedRate()
	loadOK := res.Dropped == 0 && res.Errors == 0 && achieved >= wak01MinRate*float64(th["tx_per_s"])
	loadBefore, loadAfter := wakeStart.Sub(loadStart), loadEnd.Sub(win.end)

	pass := int64(missing) <= th["missing_max"] && match >= matchMin && loadOK && reads > 0 &&
		loadBefore >= before && loadAfter >= after
	st := rp.Stats()
	details := fmt.Sprintf("table %s seeded with %d rows; load %s; Wake started %s after the load began; "+
		"initial snapshot consistent point seen %s after start, %d rows acknowledged, done %s after start (%s "+
		"after first seen); load ran %s past the snapshot end; final LSN confirmed %s after the load ended; "+
		"Kafka records %d (events %d, snapshot reads %d, duplicates %d, tombstones %d); M2: Postgres rows %d, "+
		"sink rows %d, missing or different %d, extra %d; topic %s",
		wak01Table, th["rows"], res.Summary(), loadBefore.Round(time.Millisecond),
		win.seen.Sub(wakeStart).Round(time.Millisecond), snapRows, win.end.Sub(wakeStart).Round(time.Millisecond),
		win.end.Sub(win.seen).Round(time.Millisecond), loadAfter.Round(time.Millisecond),
		drain.Round(time.Millisecond), nRecs, st.Events, reads, st.Duplicates, st.Tombstones, len(want),
		len(state[wak01Table]), missing, extra, topic)
	if len(diff) > 0 {
		details += "; first differences: " + strings.Join(diff[:min(len(diff), 10)], "; ")
	}
	if len(res.FirstErrs) > 0 {
		details += "; load errors: " + strings.Join(res.FirstErrs, "; ")
	}
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"missing": missing, "extra": extra, "state_match_pct": match, "rows": th["rows"],
			"pg_rows_final": len(want), "tx_per_s": th["tx_per_s"], "tx_per_s_achieved": achieved,
			"tx_committed": res.Committed, "tx_dropped": res.Dropped, "tx_errors": res.Errors,
			"load_before_s": loadBefore.Seconds(), "load_after_s": loadAfter.Seconds(),
			"snapshot_s": win.end.Sub(wakeStart).Seconds(), "snapshot_rows": snapRows, "snapshot_events": reads,
			"drain_after_load_s": drain.Seconds(),
		},
		Pass:    pass,
		Details: details,
	})
}

// wak01Snapshot states the default snapshot settings explicitly: the
// criterion exercises the initial snapshot.
const wak01Snapshot = `snapshot:
  mode: initial
  chunk_rows: 10000
  parallel_tables: 4`

// snapshotWindow is what watchInitialSnapshot observed.
type snapshotWindow struct {
	seen time.Time // first status showing the initial run
	end  time.Time // first status showing it finished
}

// watchInitialSnapshot polls /v1/status: the snapshot is seen once the
// consistent point is set (the slot and its exported snapshot exist) and
// ends when the initial run is gone (Wake removes a finished run) or every
// table of it is done (every chunk acknowledged).
func watchInitialSnapshot(ctx context.Context, w *stack.Wake) (snapshotWindow, error) {
	var win snapshotWindow
	var lastErr error
	for {
		st, err := w.Admin().Status(ctx)
		lastErr = err
		if err == nil && st.Snapshot.ConsistentPoint != "" {
			if win.seen.IsZero() {
				win.seen = time.Now()
			}
			if initialRunDone(st) {
				win.end = time.Now()
				return win, nil
			}
		}
		select {
		case <-ctx.Done():
			return win, fmt.Errorf("initial snapshot not done (seen %t, last status error: %w): %w",
				!win.seen.IsZero(), lastErr, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// initialRunDone reports whether the status lists no unfinished initial
// snapshot run.
func initialRunDone(st stack.Status) bool {
	for _, r := range st.Snapshot.Runs {
		if r.Kind != pipelineKindInitial {
			continue
		}
		for _, tb := range r.Tables {
			if !tb.Done {
				return false
			}
		}
	}
	return true
}

// pipelineKindInitial is the status kind of the initial snapshot run.
const pipelineKindInitial = "initial"

// snapshotRowsMetric reads wake_snapshot_rows_total of a table from
// /metrics.
func snapshotRowsMetric(ctx context.Context, w *stack.Wake, table string) (int64, error) {
	text, err := w.Admin().Metrics(ctx)
	if err != nil {
		return 0, err //nolint:wrapcheck // stack errors carry context
	}
	prefix := fmt.Sprintf("wake_snapshot_rows_total{table=%q} ", table)
	for line := range strings.Lines(text) {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, fmt.Errorf("parse %q: %w", line, err)
			}
			return int64(f), nil
		}
	}
	return 0, fmt.Errorf("no %s sample", strings.TrimSpace(prefix))
}

// dropSchema drops a test schema at cleanup to free the disk; a failure is
// only logged.
func dropSchema(tb testing.TB, pg *stack.PG, schema string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pg.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
		tb.Logf("drop schema %s: %v", schema, err)
	}
}
