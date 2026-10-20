// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
	"github.com/thnhdatle/wake/internal/testutil/m2"
	"github.com/thnhdatle/wake/pkg/event"
)

// createItems creates table name(id bigint primary key, v text, n int)
// with rows 1..rows.
func (e *env) createItems(name string, rows int) {
	e.t.Helper()
	e.exec(fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, v text NOT NULL, n int NOT NULL DEFAULT 0)", name))
	if rows > 0 {
		e.exec(fmt.Sprintf("INSERT INTO %s SELECT g, md5(g::text), 0 FROM generate_series(1, %d) g", name, rows))
	}
}

// writer applies random single- and multi-statement transactions to one
// items table: inserts, updates, deletes and key-changing updates. New ids
// start above base so that writers never collide.
type writer struct {
	table string
	conn  *pgx.Conn
	rng   *rand.Rand
	next  int64
	span  int64
	ops   int
}

// startWriter runs a writer on its own connection until the returned stop
// function is called; stop waits for it and fails the test on SQL errors.
func (e *env) startWriter(table string, base int64, seed uint64) (stop func()) {
	e.t.Helper()
	w := &writer{
		table: table, conn: e.db.Conn(e.t), rng: rand.New(rand.NewPCG(seed, seed^0x5DEECE66D)), //nolint:gosec // test
		next: base, span: base,
	}
	ctx, cancel := context.WithCancel(context.Background())
	var (
		wg  sync.WaitGroup
		err error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = w.run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			require.NoError(e.t, err, "writer on %s", table)
			e.t.Logf("writer on %s committed %d transactions", table, w.ops)
		})
	}
	e.t.Cleanup(stop)
	return stop
}

// loop runs step every 2 ms on its own goroutine until the returned stop
// function is called; stop waits for it and fails the test on an error.
func (e *env) loop(step func() error) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var (
		wg  sync.WaitGroup
		err error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			if err = step(); err != nil {
				return
			}
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			require.NoError(e.t, err)
		})
	}
	e.t.Cleanup(stop)
	return stop
}

// run commits a transaction every 2 ms until ctx ends.
func (w *writer) run(ctx context.Context) error {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		if err := w.step(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		w.ops++
	}
}

func (w *writer) step(ctx context.Context) error {
	n := 1
	if w.rng.IntN(5) == 0 {
		n = 2 + w.rng.IntN(3)
	}
	tx, err := w.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	for range n {
		if _, err := tx.Exec(ctx, w.statement()); err != nil {
			_ = tx.Rollback(ctx)
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "40P01" { // deadlock with another writer: retry later
				return nil
			}
			return fmt.Errorf("%s: %w", w.table, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// statement returns one random write; existing rows are picked as the first
// id at or above a random point.
func (w *writer) statement() string {
	pick := fmt.Sprintf("(SELECT id FROM %s WHERE id >= %d ORDER BY id LIMIT 1)", w.table, w.rng.Int64N(w.span)+1)
	switch r := w.rng.IntN(10); {
	case r < 3:
		w.next++
		return fmt.Sprintf("INSERT INTO %s (id, v, n) VALUES (%d, md5(random()::text), 0)", w.table, w.next)
	case r < 7:
		return fmt.Sprintf("UPDATE %s SET v = md5(random()::text), n = n + 1 WHERE id = %s", w.table, pick)
	case r < 8:
		w.next++
		return fmt.Sprintf("UPDATE %s SET id = %d, n = n + 1 WHERE id = %s", w.table, w.next, pick)
	default:
		return fmt.Sprintf("DELETE FROM %s WHERE id = %s", w.table, pick)
	}
}

// pgRows returns the rows of table keyed per M2 by keyCols.
func (e *env) pgRows(table string, keyCols ...string) m2.Table {
	e.t.Helper()
	rows, err := e.conn.Query(e.t.Context(), fmt.Sprintf("SELECT to_jsonb(t)::text FROM %s t", table))
	require.NoError(e.t, err)
	var out []event.Row
	for rows.Next() {
		var s string
		require.NoError(e.t, rows.Scan(&s))
		var r event.Row
		require.NoError(e.t, json.Unmarshal([]byte(s), &r))
		out = append(out, r)
	}
	require.NoError(e.t, rows.Err())
	t, err := m2.FromRows(keyCols, out)
	require.NoError(e.t, err)
	return t
}

// consumeAll reads every record of the topics up to their current end
// offsets; topics that do not exist yield nothing.
func consumeAll(t *testing.T, k *kafkatest.Kafka, topics ...string) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitShort)
	defer cancel()
	ends, err := k.Admin(t).ListEndOffsets(ctx, topics...)
	require.NoError(t, err)
	want := map[string]map[int32]int64{}
	total := int64(0)
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil || o.Offset == 0 {
			return
		}
		if want[o.Topic] == nil {
			want[o.Topic] = map[int32]int64{}
		}
		want[o.Topic][o.Partition] = o.Offset
		total += o.Offset
	})
	if total == 0 {
		return nil
	}
	var existing []string
	for topic := range want {
		existing = append(existing, topic)
	}
	cl := k.Client(t, kgo.ConsumeTopics(existing...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	var out []*kgo.Record
	for remaining(want) > 0 {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "consume %v: %d records missing", existing, remaining(want))
		fetches.EachError(func(topic string, p int32, err error) {
			t.Fatalf("fetch %s/%d: %v", topic, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if end, ok := want[r.Topic][r.Partition]; ok && r.Offset < end {
				out = append(out, r)
				if r.Offset+1 == end {
					delete(want[r.Topic], r.Partition)
				}
			}
		})
	}
	return out
}

func remaining(want map[string]map[int32]int64) int {
	n := 0
	for _, ps := range want {
		n += len(ps)
	}
	return n
}

// replay applies the records of the topics per M2.
func replay(t *testing.T, recs []*kgo.Record) (m2.State, m2.Stats) {
	t.Helper()
	var r m2.Replayer
	for _, rec := range recs {
		require.NoError(t, r.Add(rec.Value), "%s/%d@%d", rec.Topic, rec.Partition, rec.Offset)
	}
	st, err := r.State()
	require.NoError(t, err)
	return st, r.Stats()
}

// assertSinkEqualsSource compares the replayed Kafka topics of tables with
// Postgres (M2) and returns the replay statistics.
func (e *env) assertSinkEqualsSource(tables ...string) m2.Stats {
	e.t.Helper()
	topics := make([]string, len(tables))
	for i, tb := range tables {
		topics[i] = e.prefix + ".public." + tb
	}
	st, stats := replay(e.t, consumeAll(e.t, e.k, topics...))
	for _, tb := range tables {
		want := e.pgRows(tb, "id")
		diff := m2.Diff(st["public."+tb], want)
		if len(diff) > 10 {
			diff = append(diff[:10], fmt.Sprintf("... %d more", len(diff)-10))
		}
		assert.Empty(e.t, diff, "table %s: sink (M2 replay) differs from Postgres (%d rows)", tb, len(want))
	}
	e.t.Logf("replayed %+v", stats)
	return stats
}

// assertSinkKeysEqualSource compares only the set of keys of the replayed
// topic with Postgres.
func (e *env) assertSinkKeysEqualSource(table string) {
	e.t.Helper()
	st, _ := replay(e.t, consumeAll(e.t, e.k, e.prefix+".public."+table))
	got := st["public."+table]
	want := e.pgRows(table, "id")
	var missing, extra []string
	for k := range want {
		if _, ok := got[k]; !ok {
			missing = append(missing, k)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			extra = append(extra, k)
		}
	}
	assert.Empty(e.t, missing, "keys missing at the sink")
	assert.Empty(e.t, extra, "keys at the sink that Postgres no longer has")
	assert.NotEmpty(e.t, want)
}
