// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/event"
)

// WAK-06 scenario parameters. The criterion fixes the number of updates,
// keys and writers; the partition count is chosen by the test. The writers run closed-loop (each issues its share of
// single-row update transactions back to back), so the update count is exact.
const (
	wak06Table   = "wak06.counters"
	wak06Seed    = uint64(6)
	wak06Timeout = 50 * time.Minute
	// wak06Partitions spreads the keys over several partitions, as the
	// WAK-05 workload (M6) does.
	wak06Partitions = 6
)

// TestAcceptance_WAK06_PerKeyOrder implements WAK-06: Per-key order: 1M updates over 10k keys from 16
// concurrent writers.
func TestAcceptance_WAK06_PerKeyOrder(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-06"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	th := readThresholds(t, id, c, "updates", "keys", "writers", "order_violations_max")

	stack.EnsureServices(t, "postgres", "kafka")
	pg := stack.ConnectPG(t, 2)
	pg.Exec(t, fmt.Sprintf(`DROP SCHEMA IF EXISTS wak06 CASCADE;
CREATE SCHEMA wak06;
CREATE TABLE wak06.counters (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text);
INSERT INTO wak06.counters SELECT g, 0, md5(g::text) FROM generate_series(1, %d) g;`, th["keys"]))

	prefix := fmt.Sprintf("w06_%d", time.Now().Unix())
	topic := stack.Topic(prefix, "wak06", "counters")
	t.Cleanup(func() { dropTopics(t, topic) })
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID: id, Config: wakeConfig("wak06", []string{"wak06.*"}, kafkaSinkPartitions(prefix, wak06Partitions), ""),
		FreshState: true,
	})
	ctx := withTimeout(t, wak06Timeout)
	failIf(t, id, waitSnapshotDone(ctx, wake), "wait for the initial snapshot")

	wr, err := runCounterWriters(ctx, int(th["writers"]), th["updates"], th["keys"])
	failIf(t, id, err, "writers")
	streamStart := time.Now()
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN")
	drain := time.Since(streamStart)

	final, err := counterVers(ctx, pg)
	failIf(t, id, err, "read final vers from Postgres")
	recs, err := stack.ReadTopics(ctx, topic)
	failIf(t, id, err, "read Kafka topic")
	parts := map[int32]bool{}
	for _, r := range recs {
		parts[r.Partition] = true
	}
	dec, err := stack.DecodePayloads(stack.Values(recs))
	failIf(t, id, err, "decode Kafka records")
	order, err := stack.OrderViolations(dec.Events)
	failIf(t, id, err, "M4 order")
	an, err := analyzeCounters(dec.Events, final)
	failIf(t, id, err, "analyze delivered vers")

	pass := int64(order.Violations) <= th["order_violations_max"] && an.updatesInPG == th["updates"] &&
		wr.committed == th["updates"] && an.lost == 0 && an.verRegressions == 0
	details := fmt.Sprintf("writers %d committed %d single-row updates over %d keys in %s (%.0f tx/s); Wake "+
		"confirmed the final LSN %s after the writers ended; Kafka records %d (tombstones %d) on %d partitions; "+
		"M4 checked %d, duplicates %d, violations %d %v; ver regressions in delivery order %d %v; Postgres "+
		"sum(ver) %d; update events missing %d, unexpected %d; topic %s",
		th["writers"], wr.committed, th["keys"], wr.elapsed.Round(time.Millisecond),
		float64(wr.committed)/wr.elapsed.Seconds(), drain.Round(time.Millisecond), len(dec.Events)+dec.Tombstones,
		dec.Tombstones, len(parts), order.Checked, order.Duplicates, order.Violations, order.Examples,
		an.verRegressions, an.examples, an.updatesInPG, an.lost, an.unexpected, topic)
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"updates": wr.committed, "keys": th["keys"], "writers": th["writers"],
			"order_violations": order.Violations, "events_checked": order.Checked, "duplicates": order.Duplicates,
			"ver_regressions": an.verRegressions, "lost_updates": an.lost, "unexpected_events": an.unexpected,
			"partitions": len(parts), "writers_s": wr.elapsed.Seconds(), "drain_after_writers_s": drain.Seconds(),
		},
		Pass:    pass,
		Details: details,
	})
}

type writersResult struct {
	committed int64
	elapsed   time.Duration
}

// runCounterWriters runs writers closed-loop connections that together
// commit updates single-row transactions, each incrementing the ver of a
// random key in 1..keys.
func runCounterWriters(ctx context.Context, writers int, updates, keys int64) (writersResult, error) {
	conns := make([]*pgx.Conn, 0, writers)
	defer func() {
		for _, c := range conns {
			_ = c.Close(context.WithoutCancel(ctx))
		}
	}()
	for range writers {
		c, err := pgx.Connect(ctx, stack.PostgresDSN)
		if err != nil {
			return writersResult{}, fmt.Errorf("connect writer: %w", err)
		}
		conns = append(conns, c)
	}
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		committed int64
		firstErr  error
	)
	start := time.Now()
	for i, c := range conns {
		share := updates / int64(writers)
		if int64(i) < updates%int64(writers) {
			share++
		}
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(wak06Seed+uint64(i), uint64(i)+1)) //nolint:gosec // reproducible load
			var n int64
			var err error
			for ; n < share && err == nil; n++ {
				_, err = c.Exec(ctx, `UPDATE wak06.counters SET ver = ver + 1, payload = md5(random()::text)
					WHERE id = $1`, 1+rng.Int64N(keys))
			}
			if err != nil {
				n--
			}
			mu.Lock()
			defer mu.Unlock()
			committed += n
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("writer %d after %d updates: %w", i, n, err)
			}
		})
	}
	wg.Wait()
	return writersResult{committed: committed, elapsed: time.Since(start)}, firstErr
}

// counterVers reads the final ver of every key.
func counterVers(ctx context.Context, pg *stack.PG) (map[string]int64, error) {
	out := map[string]int64{}
	err := pg.With(ctx, func(c *pgx.Conn) error {
		rows, err := c.Query(ctx, "SELECT id, ver FROM wak06.counters")
		if err != nil {
			return err //nolint:wrapcheck // wrapped below
		}
		var k, v int64
		_, err = pgx.ForEachRow(rows, []any{&k, &v}, func() error {
			out[stack.IDKey(k)] = v
			return nil
		})
		return err //nolint:wrapcheck // wrapped below
	})
	if err != nil {
		return nil, fmt.Errorf("select counters: %w", err)
	}
	return out, nil
}

type counterAnalysis struct {
	updatesInPG    int64
	lost           int64
	unexpected     int
	verRegressions int
	examples       []string
	// seenVer holds the distinct vers of update events per key, lastVer the
	// highest ver delivered so far per key.
	seenVer map[string]map[int64]bool
	lastVer map[string]int64
}

// analyzeCounters checks the delivered update events against the final
// vers: every key's vers 1..final must each arrive once (ver only grows by
// one per update), and in delivery order a key's ver must strictly
// increase.
func analyzeCounters(events []*event.Event, final map[string]int64) (counterAnalysis, error) {
	an := counterAnalysis{
		seenVer: make(map[string]map[int64]bool, len(final)), lastVer: make(map[string]int64, len(final)),
	}
	seenID := make(map[string]bool, len(events))
	for _, e := range events {
		if stack.TableName(e) != wak06Table || seenID[e.ID] {
			continue
		}
		seenID[e.ID] = true
		if err := an.add(e, final); err != nil {
			return an, err
		}
	}
	for key, v := range final {
		an.updatesInPG += v
		an.lost += v - int64(len(an.seenVer[key]))
	}
	return an, nil
}

// add checks one distinct event in delivery order.
func (an *counterAnalysis) add(e *event.Event, final map[string]int64) error {
	key, err := stack.EventKey(e)
	if err != nil {
		return err
	}
	ver, ok := stack.IntColumn(e.After, "ver")
	if !ok {
		an.unexpected++
		return nil
	}
	if prev, ok := an.lastVer[key]; ok && ver <= prev {
		an.verRegressions++
		if len(an.examples) < 10 {
			an.examples = append(an.examples, fmt.Sprintf("%s ver %d (%s) after ver %d", key, ver, e.ID, prev))
		}
	}
	an.lastVer[key] = max(an.lastVer[key], ver)
	if e.Op != event.OpUpdate {
		return nil
	}
	if an.seenVer[key] == nil {
		an.seenVer[key] = map[int64]bool{}
	}
	if ver < 1 || ver > final[key] || an.seenVer[key][ver] {
		an.unexpected++
		return nil
	}
	an.seenVer[key][ver] = true
	return nil
}

// dropTopics deletes test topics at cleanup; a failure is only logged.
func dropTopics(tb testing.TB, topics ...string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := stack.DeleteTopics(ctx, topics...); err != nil {
		tb.Logf("delete topics %s: %v", strings.Join(topics, ", "), err)
	}
}
