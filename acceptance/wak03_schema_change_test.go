// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/event"
)

// WAK-03 scenario parameters. The criterion fixes the DDL list and one write
// after each DDL; the rate and pacing of the background writes are chosen by the test.
const (
	wak03Table     = "wak03.evolve"
	wak03Seed      = uint64(3)
	wak03Rows      = 500
	wak03Rate      = 100 // background transactions per second
	wak03Conns     = 4
	wak03Warmup    = 3 * time.Second
	wak03DDLGap    = 3 * time.Second
	wak03Cooldown  = 3 * time.Second
	wak03ProbeBase = 1_000_000
)

// schemaDDL is one DDL of the scenario with the probe write that follows it
// and a predicate recognizing its schema-change event.
type schemaDDL struct {
	name    string
	sql     string
	probe   func(id int64) string
	matches func(before, after []event.Column) bool
}

func hasColumn(cols []event.Column, name string) bool {
	return slices.ContainsFunc(cols, func(c event.Column) bool { return c.Name == name })
}

func columnType(cols []event.Column, name string) string {
	for _, c := range cols {
		if c.Name == name {
			return c.Type
		}
	}
	return ""
}

var wak03DDLs = []schemaDDL{
	{
		name: "ADD COLUMN c",
		sql:  "ALTER TABLE wak03.evolve ADD COLUMN c text DEFAULT 'd'",
		probe: func(id int64) string {
			return fmt.Sprintf("INSERT INTO wak03.evolve (id, ver, a, b, n, c) VALUES (%d, 1, 1, 'x', 1, 'added')", id)
		},
		matches: func(b, a []event.Column) bool { return !hasColumn(b, "c") && hasColumn(a, "c") },
	},
	{
		name: "DROP COLUMN a",
		sql:  "ALTER TABLE wak03.evolve DROP COLUMN a",
		probe: func(id int64) string {
			return fmt.Sprintf("INSERT INTO wak03.evolve (id, ver, b, n, c) VALUES (%d, 1, 'x', 2, 'dropped')", id)
		},
		matches: func(b, a []event.Column) bool { return hasColumn(b, "a") && !hasColumn(a, "a") },
	},
	{
		name: "RENAME COLUMN b TO b_renamed",
		sql:  "ALTER TABLE wak03.evolve RENAME COLUMN b TO b_renamed",
		probe: func(id int64) string {
			return fmt.Sprintf("INSERT INTO wak03.evolve (id, ver, b_renamed, n, c) VALUES (%d, 1, 'r', 3, 'c')", id)
		},
		matches: func(b, a []event.Column) bool {
			return hasColumn(b, "b") && !hasColumn(a, "b") && hasColumn(a, "b_renamed")
		},
	},
	{
		name: "ALTER COLUMN n TYPE bigint",
		sql:  "ALTER TABLE wak03.evolve ALTER COLUMN n TYPE bigint",
		probe: func(id int64) string {
			return fmt.Sprintf("INSERT INTO wak03.evolve (id, ver, b_renamed, n, c) "+
				"VALUES (%d, 1, 't', 9223372036854775807, 'c')", id)
		},
		matches: func(b, a []event.Column) bool {
			return columnType(b, "n") != columnType(a, "n") && columnType(a, "n") != ""
		},
	},
}

// wak03Tx updates a random seeded row (ver + 1) or inserts a new one; it
// only touches id and ver, which every DDL keeps.
func wak03Tx(ctx context.Context, conn *pgx.Conn, worker int, rng *rand.Rand) ([]stack.Change, error) {
	if rng.IntN(5) == 0 {
		id := int64(100_000*(worker+1)) + rng.Int64N(90_000)
		tag, err := conn.Exec(ctx, "INSERT INTO wak03.evolve (id, ver) VALUES ($1, 1) ON CONFLICT DO NOTHING", id)
		if err != nil || tag.RowsAffected() == 0 {
			return nil, err //nolint:wrapcheck // reported in the load summary
		}
		return []stack.Change{{Table: wak03Table, Key: stack.IDKey(id), Ver: 1, Op: stack.OpCreate}}, nil
	}
	id := 1 + rng.Int64N(wak03Rows)
	var ver int64
	if err := conn.QueryRow(ctx, "UPDATE wak03.evolve SET ver = ver + 1 WHERE id = $1 RETURNING ver", id).
		Scan(&ver); err != nil {
		return nil, err //nolint:wrapcheck // reported in the load summary
	}
	return []stack.Change{{Table: wak03Table, Key: stack.IDKey(id), Ver: ver, Op: stack.OpUpdate}}, nil
}

// TestAcceptance_WAK03_SchemaChange implements WAK-03: ADD/DROP/RENAME COLUMN and ALTER TYPE mid-stream under writes.
func TestAcceptance_WAK03_SchemaChange(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-03"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	th := readThresholds(t, id, c, "ddl_statements", "writes_after_each_ddl", "stops_max", "schema_events_per_ddl",
		"events_with_old_version_after_ddl_max")
	if th["ddl_statements"] != int64(len(wak03DDLs)) {
		harness.Failf(t, id, "criteria.yaml asks for %d DDL statements, the scenario runs %d (ADD, DROP, RENAME, "+
			"ALTER TYPE)", th["ddl_statements"], len(wak03DDLs))
	}

	stack.EnsureUp(t)
	pg := stack.ConnectPG(t, 2)
	pg.Exec(t, fmt.Sprintf(`DROP SCHEMA IF EXISTS wak03 CASCADE;
CREATE SCHEMA wak03;
CREATE TABLE wak03.evolve (id bigint PRIMARY KEY, ver bigint NOT NULL, a int, b text, n int);
INSERT INTO wak03.evolve SELECT g, 1, g, 'b' || g, g FROM generate_series(1, %d) g;`, wak03Rows))

	prefix := fmt.Sprintf("w03_%d", time.Now().Unix())
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID: id, Config: wakeConfig("wak03", []string{"wak03.*"}, kafkaSink(prefix), ""), FreshState: true,
	})
	ctx := withTimeout(t, 8*time.Minute)
	failIf(t, id, waitSnapshotDone(ctx, wake), "wait for the initial snapshot")

	mon := wake.Monitor(250 * time.Millisecond)
	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: wak03Rate, Conns: wak03Conns, Seed: wak03Seed, Tx: wak03Tx,
	})
	failIf(t, id, err, "start load")
	probes := runDDLs(ctx, t, id, pg, th["writes_after_each_ddl"])
	time.Sleep(wak03Cooldown)
	load.Stop()
	res := load.Wait()
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN")
	monRes := mon.Stop()

	recs, err := stack.ReadTopics(ctx, stack.Topic(prefix, "wak03", "evolve"), stack.SchemaTopic(prefix))
	failIf(t, id, err, "read Kafka topics")
	dec, err := stack.DecodePayloads(stack.Values(recs))
	failIf(t, id, err, "decode Kafka records")
	an, err := analyzeSchemaEvents(dec)
	failIf(t, id, err, "analyze events")
	lost, err := stack.FindLost(append(res.Changes, probes...), dec.Events, "ver")
	failIf(t, id, err, "M3 lost")

	perDDLMin, perDDLMax := minMax(an.perDDL)
	pass := monRes.Stops <= int(th["stops_max"]) &&
		perDDLMin == th["schema_events_per_ddl"] && perDDLMax == th["schema_events_per_ddl"] &&
		an.unexpected == 0 && an.oldVersion <= th["events_with_old_version_after_ddl_max"] &&
		an.versionsIncrease && len(lost.Lost) == 0 && res.Committed > 0
	details := fmt.Sprintf("DDLs: %s; schema events per DDL %v (versions %v), unexpected %d; data events %d "+
		"checked for version, %d with an old version; stops %d (%d of %d readiness polls not ready) %v; "+
		"load %s; M3 lost %d of %d recorded; topic prefix %s",
		ddlNames(), an.perDDL, an.versions, an.unexpected, an.checked, an.oldVersion, monRes.Stops,
		monRes.NotReadyPolls, monRes.Polls, monRes.Events, res.Summary(), len(lost.Lost), lost.Recorded, prefix)
	if len(an.examples) > 0 {
		details += "; old-version examples: " + strings.Join(an.examples, "; ")
	}
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"ddl_statements": len(wak03DDLs), "writes_after_each_ddl": th["writes_after_each_ddl"],
			"stops": monRes.Stops, "schema_events_per_ddl": perDDLMin, "schema_events_per_ddl_max": perDDLMax,
			"schema_events_unexpected": an.unexpected, "events_with_old_version_after_ddl": an.oldVersion,
			"data_events_checked": an.checked, "lost": len(lost.Lost), "committed_tx": res.Committed,
		},
		Pass:    pass,
		Details: details,
	})
}

func ddlNames() string {
	names := make([]string, len(wak03DDLs))
	for i, d := range wak03DDLs {
		names[i] = d.name
	}
	return strings.Join(names, ", ")
}

// runDDLs executes each DDL followed by writes probe inserts, pacing them
// under the background load, and returns the probe changes.
func runDDLs(ctx context.Context, t *testing.T, id string, pg *stack.PG, writes int64) []stack.Change {
	t.Helper()
	time.Sleep(wak03Warmup)
	probes := make([]stack.Change, 0, int(writes)*len(wak03DDLs))
	next := int64(wak03ProbeBase)
	for _, d := range wak03DDLs {
		_, err := pg.ExecContext(ctx, d.sql)
		failIf(t, id, err, d.name)
		for range writes {
			_, err = pg.ExecContext(ctx, d.probe(next))
			failIf(t, id, err, "write after "+d.name)
			probes = append(probes, stack.Change{
				Table: wak03Table, Key: stack.IDKey(next), Ver: 1, Op: stack.OpCreate, Commit: time.Now(),
			})
			next++
		}
		time.Sleep(wak03DDLGap)
	}
	return probes
}

type schemaAnalysis struct {
	perDDL           []int64
	versions         []int64
	versionsIncrease bool
	unexpected       int
	checked          int
	oldVersion       int64
	examples         []string
}

type seqItem struct {
	seq    event.Seq
	schema *event.SchemaChange
	data   *event.Event
}

// analyzeSchemaEvents maps the table's schema-change events to the DDLs and
// walks schema and data events in seq order: a data event after a schema
// event must carry at least that event's version.
func analyzeSchemaEvents(dec stack.Decoded) (schemaAnalysis, error) {
	an := schemaAnalysis{perDDL: make([]int64, len(wak03DDLs)), versionsIncrease: true}
	items, err := tableItems(dec)
	if err != nil {
		return an, err
	}
	var cur int64
	ddlIdx := map[int]bool{}
	for _, it := range items {
		if s := it.schema; s != nil {
			an.classify(s, ddlIdx)
			if s.SchemaVersion <= cur && s.ColumnsBefore != nil {
				an.versionsIncrease = false
			}
			cur = max(cur, s.SchemaVersion)
			continue
		}
		an.checked++
		if it.data.SchemaVersion < cur {
			an.oldVersion++
			if len(an.examples) < 10 {
				an.examples = append(an.examples, fmt.Sprintf("%s version %d after version %d", it.data.ID,
					it.data.SchemaVersion, cur))
			}
		}
	}
	return an, nil
}

// tableItems returns the distinct schema and data events of wak03.evolve
// sorted by seq.
func tableItems(dec stack.Decoded) ([]seqItem, error) {
	var items []seqItem
	seen := map[string]bool{}
	for _, s := range dec.SchemaChanges {
		if s.Source.Schema+"."+s.Source.Table != wak03Table || seen[s.ID] {
			continue
		}
		seen[s.ID] = true
		seq, err := event.ParseSeq(s.ID)
		if err != nil {
			return nil, fmt.Errorf("schema event %s: %w", s.ID, err)
		}
		items = append(items, seqItem{seq: seq, schema: s})
	}
	for _, e := range dec.Events {
		if stack.TableName(e) != wak03Table || seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		seq, err := e.Seq()
		if err != nil {
			return nil, fmt.Errorf("event %s: %w", e.ID, err)
		}
		items = append(items, seqItem{seq: seq, data: e})
	}
	slices.SortStableFunc(items, func(a, b seqItem) int { return event.Compare(a.seq, b.seq) })
	return items, nil
}

// classify counts a schema event against the DDL it reflects; the first
// event of the table (columns_before null) is the table's initial version.
func (an *schemaAnalysis) classify(s *event.SchemaChange, done map[int]bool) {
	if s.ColumnsBefore == nil {
		return
	}
	for i, d := range wak03DDLs {
		if d.matches(s.ColumnsBefore, s.ColumnsAfter) {
			an.perDDL[i]++
			if !done[i] {
				done[i] = true
				an.versions = append(an.versions, s.SchemaVersion)
			}
			return
		}
	}
	an.unexpected++
}

func minMax(xs []int64) (lo, hi int64) {
	if len(xs) == 0 {
		return 0, 0
	}
	return slices.Min(xs), slices.Max(xs)
}

// readThresholds reads integer thresholds; a missing key fails the test.
func readThresholds(t *testing.T, id string, c *harness.Entry, keys ...string) map[string]int64 {
	t.Helper()
	out := make(map[string]int64, len(keys))
	for _, k := range keys {
		v, err := c.Int(k)
		failIf(t, id, err, "threshold "+k)
		out[k] = v
	}
	return out
}

// adminer is a Wake under test with an admin client: the compose service
// (*stack.Wake) or a native process (*stack.NativeWake).
type adminer interface {
	Admin() *stack.Admin
}

// waitSnapshotDone polls /v1/status until no snapshot run has an unfinished
// table and the queue is empty.
func waitSnapshotDone(ctx context.Context, w adminer) error {
	for {
		st, err := w.Admin().Status(ctx)
		if err == nil && snapshotDone(st) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("snapshot not done (last status error: %w): %w", err, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func snapshotDone(st stack.Status) bool {
	if len(st.Snapshot.Queue) > 0 {
		return false
	}
	for _, r := range st.Snapshot.Runs {
		for _, tb := range r.Tables {
			if !tb.Done {
				return false
			}
		}
	}
	return true
}
