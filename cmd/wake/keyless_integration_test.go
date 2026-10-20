// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
	"github.com/thnhdatle/wake/pkg/event"
)

// testKeylessExisting checks keyless tables in existing mode through `wake run`
// startup: a keyless table in the publication fails startup with
// table_keyless unless tables.skip_unkeyed (skipped) or tables.emit_unkeyed
// (inserts emitted with key null) is set.
func testKeylessExisting(e *env) {
	t := e.t
	e.exec("CREATE TABLE k (a int, b text)")
	e.createItems("p", 2)
	e.exec("CREATE PUBLICATION pub_f1e FOR TABLE k, p WITH (publish = 'insert')")
	base := e.config()
	base.Source.Publication = config.Publication{Mode: config.PublicationExisting, Name: "pub_f1e"}
	base.Tables.Include = nil

	p := e.startWake(&base)
	code := p.wait(waitShort)
	assert.Equal(t, exitFailure, code)
	assert.Contains(t, p.output(), "table_keyless")
	assert.Contains(t, p.output(), "public.k")
	exists, _, _ := e.slotRow()
	assert.False(t, exists, "startup failed before creating the slot")

	skip := base
	skip.Tables.SkipUnkeyed = true
	p = e.startWake(&skip)
	p.waitReady()
	p.stop()

	emit := base
	emit.Tables.EmitUnkeyed = true
	p = e.startWake(&emit)
	p.waitReady()
	e.exec("INSERT INTO k VALUES (1, 'one')")
	e.sync(p)
	assertKeylessInsert(t, e.k, e.prefix+".public.k")
	p.stop()
}

// testKeylessManaged checks keyless tables in managed mode: a keyless table
// matching the globs is never added to wake_<name>; startup fails with
// table_keyless; with tables.emit_unkeyed it is in wake_<name>_insert, the
// application's UPDATE and DELETE keep working and its inserts are emitted
// with key null.
func testKeylessManaged(e *env) {
	t := e.t
	e.exec("CREATE TABLE k (a int, b text)")
	e.createItems("p", 2)
	cfg := e.config()
	code, out := e.runCLI("check", "--config", e.writeConfig(&cfg))
	assert.Equal(t, exitFailure, code, out)
	assert.Contains(t, out, "FAIL  keyless_tables")
	assert.Contains(t, out, "- public.k")

	p := e.startWake(&cfg)
	assert.Equal(t, exitFailure, p.wait(waitShort))
	assert.Contains(t, p.output(), "table_keyless")
	assert.Zero(t, e.published("wake_f1m", "k"), "keyless table never in wake_<name>")

	cfg.Tables.EmitUnkeyed = true
	p = e.startWake(&cfg)
	p.waitReady()
	assert.Zero(t, e.published("wake_f1m", "k"), "keyless table never in wake_<name>")
	assert.Equal(t, 1, e.published("wake_f1m_insert", "k"))
	assert.Equal(t, 1, e.published("wake_f1m", "p"))
	e.exec("INSERT INTO k VALUES (1, 'one')")
	e.exec("UPDATE k SET b = 'uno' WHERE a = 1")
	e.exec("DELETE FROM k WHERE a = 1")
	e.sync(p)
	assertKeylessInsert(t, e.k, e.prefix+".public.k")
	p.stop()
}

// published counts table in publication.
func (e *env) published(publication, table string) int {
	e.t.Helper()
	var n int
	require.NoError(e.t, e.conn.QueryRow(e.t.Context(), "SELECT count(*) FROM pg_publication_tables "+
		"WHERE pubname = $1 AND schemaname = 'public' AND tablename = $2", publication, table).Scan(&n))
	return n
}

// assertKeylessInsert checks that the topic holds exactly one create event
// with key null.
func assertKeylessInsert(t *testing.T, k *kafkatest.Kafka, topic string) {
	t.Helper()
	recs := consumeAll(t, k, topic)
	require.Len(t, recs, 1, "only the insert is captured")
	ev, err := event.Decode(recs[0].Value)
	require.NoError(t, err)
	assert.Equal(t, event.OpCreate, ev.Op)
	assert.Nil(t, ev.Key)
}
