// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/pkg/event"
)

// testSchemaChanges checks ADD COLUMN, DROP COLUMN, ALTER TYPE and
// RENAME COLUMN, each followed by a write while another writer keeps
// writing. Each produces a schema-change event in <prefix>._schema with a
// higher version, and every data event carries the version of the latest
// schema change before it in seq order.
func testSchemaChanges(e *env) {
	t := e.t
	e.exec("CREATE TABLE s (id bigint PRIMARY KEY, a text DEFAULT 'x')")
	e.exec("INSERT INTO s (id) SELECT generate_series(1, 50)")
	stop := e.startIDWriter("s", 1_000_000)
	cfg := e.config()
	p := e.startWake(&cfg)
	p.waitSnapshotsIdle()

	steps := []struct{ ddl, check string }{
		{"ALTER TABLE s ADD COLUMN c int DEFAULT 7", "c:int4"},
		{"ALTER TABLE s DROP COLUMN a", "-a"},
		{"ALTER TABLE s ALTER COLUMN c TYPE bigint", "c:int8"},
		{"ALTER TABLE s RENAME COLUMN c TO d", "d:int8"},
	}
	for i, st := range steps {
		e.exec(st.ddl)
		e.exec(fmt.Sprintf("INSERT INTO s (id) VALUES (%d)", 100+i))
	}
	stop()
	e.sync(p)

	changes := e.schemaChanges("s")
	require.Len(t, changes, len(steps), "one schema-change event per DDL")
	for i, sc := range changes {
		cols := columns(sc.ColumnsAfter)
		check := steps[i].check
		if name, ok := strings.CutPrefix(check, "-"); ok {
			assert.NotContains(t, cols, name+":", "step %d: %s", i, steps[i].ddl)
		} else {
			assert.Contains(t, cols, check, "step %d: %s", i, steps[i].ddl)
		}
		if i > 0 {
			assert.Greater(t, sc.SchemaVersion, changes[i-1].SchemaVersion, "versions increase")
		}
	}
	e.assertVersionsFollowChanges("s", changes)
	// Rows not written since a DDL keep their old image at the sink (an
	// added column's default produces no WAL row change), so compare keys.
	e.assertSinkKeysEqualSource("s")
	p.stop()
}

// startIDWriter inserts and deletes rows naming only the id column, so it
// keeps working across the DDL of the schema test.
func (e *env) startIDWriter(table string, base int64) (stop func()) {
	e.t.Helper()
	conn := e.db.Conn(e.t)
	next := base
	return e.loop(func() error {
		next++
		_, err := conn.Exec(e.t.Context(), fmt.Sprintf("INSERT INTO %s (id) VALUES (%d)", table, next))
		if err == nil && next%3 == 0 {
			_, err = conn.Exec(e.t.Context(), fmt.Sprintf("DELETE FROM %s WHERE id = %d", table, next-1))
		}
		return err
	})
}

// schemaChanges returns the schema-change events of table from
// <prefix>._schema in topic order.
func (e *env) schemaChanges(table string) []*event.SchemaChange {
	e.t.Helper()
	var out []*event.SchemaChange
	for _, r := range consumeAll(e.t, e.k, e.prefix+"._schema") {
		sc, err := event.DecodeSchemaChange(r.Value)
		require.NoError(e.t, err)
		if sc.Source.Table == table {
			out = append(out, sc)
		}
	}
	slices.SortFunc(out, func(a, b *event.SchemaChange) int {
		sa, _ := a.Seq()
		sb, _ := b.Seq()
		return event.Compare(sa, sb)
	})
	return out
}

// assertVersionsFollowChanges checks that every data event of table carries
// the version of the latest schema change preceding it in seq order.
func (e *env) assertVersionsFollowChanges(table string, changes []*event.SchemaChange) {
	e.t.Helper()
	type item struct {
		seq     event.Seq
		version int64
		data    bool
		id      string
	}
	var items []item
	for _, sc := range changes {
		s, err := sc.Seq()
		require.NoError(e.t, err)
		items = append(items, item{seq: s, version: sc.SchemaVersion})
	}
	for _, r := range consumeAll(e.t, e.k, e.prefix+".public."+table) {
		if event.IsKafkaTombstone(r.Value) {
			continue
		}
		ev, err := event.Decode(r.Value)
		require.NoError(e.t, err)
		s, err := ev.Seq()
		require.NoError(e.t, err)
		items = append(items, item{seq: s, version: ev.SchemaVersion, data: true, id: ev.ID})
	}
	slices.SortStableFunc(items, func(a, b item) int { return event.Compare(a.seq, b.seq) })
	current := int64(-1)
	for _, it := range items {
		if !it.data {
			current = it.version
			continue
		}
		if current < 0 {
			current = it.version // the first version, before any change
		}
		require.Equal(e.t, current, it.version, "event %s", it.id)
	}
}

func columns(cs []event.Column) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = c.Name + ":" + c.Type
	}
	return strings.Join(parts, ",") + ","
}

// testHeartbeat checks that on an idle source the heartbeat advances the
// slot's confirmed_flush_lsn past WAL written for uncaptured tables. It
// then checks `wake status` against the running instance and, after the
// stop, its fallback, and the destructive commands.
func testHeartbeat(e *env) {
	t := e.t
	e.createItems("h", 10)
	e.createItems("other", 0)
	cfg := e.config()
	cfg.Tables.Include = []string{"public.h"}
	cfg.Heartbeat.Interval = config.Duration(time.Second)
	code, out := e.runCLI("check", "--config", e.writeConfig(&cfg))
	require.Equal(t, 0, code, out)
	assert.Contains(t, out, "wal_retention_unlimited", "WAL retention is reported")
	assert.Contains(t, out, "result: OK")
	p := e.startWake(&cfg)
	p.waitSnapshotsIdle()
	p.waitReady()

	e.exec("INSERT INTO other SELECT g, 'x', 0 FROM generate_series(1, 1000) g")
	target := e.currentLSN()
	p.waitFor(waitShort, "confirmed_flush_lsn past "+target.String(), func() bool {
		_, _, confirmed := e.slotRow()
		return confirmed >= target
	})
	assert.Positive(t, p.metric(`wake_slot_lag_bytes`)+1, "lag metric exported")

	path := e.writeConfig(&cfg)
	code, out = e.runCLI("status", "--config", path)
	require.Equal(t, 0, code, out)
	assert.Contains(t, out, `"origin": "admin"`)
	assert.Contains(t, out, `"streaming": true`)
	p.stop()

	code, out = e.runCLI("status", "--config", path)
	require.Equal(t, 0, code, out)
	assert.Contains(t, out, "unreachable")
	assert.Contains(t, out, `"origin": "state_file"`)
	assert.Contains(t, out, `"exists": true`)

	code, out = e.runCLI("slot", "drop", "--config", path)
	assert.Equal(t, exitConfig, code, out)
	assert.Contains(t, out, "confirmation_required")
	code, out = e.runCLI("publication", "drop", "--yes", "--config", path)
	require.Equal(t, 0, code, out)
	code, out = e.runCLI("slot", "drop", "--yes", "--config", path)
	require.Equal(t, 0, code, out)
	exists, _, _ := e.slotRow()
	assert.False(t, exists, "slot dropped")
	var pubs int
	require.NoError(t, e.conn.QueryRow(t.Context(), "SELECT count(*) FROM pg_publication WHERE pubname LIKE $1",
		"wake_f7%").Scan(&pubs))
	assert.Zero(t, pubs, "publications dropped")
}

// testOversizedEventBlocksSink checks sink blocking and readiness through
// `wake run`: an event above the Kafka sink's max_event_bytes blocks the sink with
// event_too_large, /readyz answers 503 naming it, the sink_fatal alert is
// POSTed to lag.alert_webhook, and the confirmed LSN never passes the
// transaction.
func testOversizedEventBlocksSink(e *env) {
	t := e.t
	alerts := &syncList[map[string]any]{}
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var a map[string]any
		if err := json.NewDecoder(r.Body).Decode(&a); err == nil {
			alerts.add(a)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(receiver.Close)

	e.createItems("t", 3)
	cfg := e.config()
	cfg.Sinks[0].MaxEventBytes = 400
	cfg.Lag.AlertWebhook = receiver.URL
	p := e.startWake(&cfg)
	p.waitSnapshotsIdle()
	p.waitReady()
	e.sync(p)
	before := e.currentLSN()

	e.exec("INSERT INTO t VALUES (100, repeat('x', 2000), 0)")

	var env struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Checks []httpadmin.Check `json:"checks"`
	}
	p.waitFor(waitShort, "/readyz 503", func() bool {
		code, body := p.get("/readyz")
		return code == http.StatusServiceUnavailable && json.Unmarshal(body, &env) == nil
	})
	assert.Equal(t, httpadmin.CodeNotReady, env.Error.Code)
	assert.Contains(t, env.Error.Message, "event_too_large")
	sink := sinkCheck(t, env.Checks)
	assert.Equal(t, "kafka-main", sink.Sink)
	assert.Equal(t, "fatal", sink.State)
	assert.Equal(t, "event_too_large", sink.Code)
	assert.Contains(t, sink.Reason, "public.t")
	p.waitFor(waitShort, "sink_fatal alert", func() bool {
		for _, a := range alerts.all() {
			if a["rule"] == "sink_fatal" && a["state"] == "firing" {
				return true
			}
		}
		return false
	})
	require.Never(t, func() bool {
		doc, ok := p.status()
		c, err := lsnOf(doc.ConfirmedLSN)
		_, _, slot := e.slotRow()
		return !ok || err != nil || c > before || slot > before
	}, 3*time.Second, poll, "nothing acknowledged: the confirmed LSN stays before the blocked transaction")
	assert.Len(t, consumeAll(t, e.k, e.prefix+".public.t"), 3, "only the three snapshot rows were delivered")
	p.stop()
}

// sinkCheck returns the sink check of /readyz.
func sinkCheck(t *testing.T, checks []httpadmin.Check) httpadmin.Check {
	t.Helper()
	for _, c := range checks {
		if c.Name == "sink" {
			return c
		}
	}
	require.Fail(t, "no sink check", "%+v", checks)
	return httpadmin.Check{}
}
