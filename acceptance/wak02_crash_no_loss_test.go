// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/event"
)

// WAK-02 scenario parameters. The criterion fixes the kills, the share in
// large transactions and the sizes; the small load, the kill timing and the
// row size of the large transactions are chosen by the test. Kills are mixed in a seeded
// random order between a random point of the small-transaction stream and a
// point inside a large transaction: after Wake started spilling it (wake_spilled_transactions_total
// rose) and before Wake confirmed it (checked after the kill: the slot's
// confirmed_flush_lsn is not past the WAL insert position read just before
// the large transaction's COMMIT).
const (
	wak02Seed     = uint64(2)
	wak02Conns    = 8
	wak02Rate     = 200 // small transactions per second
	wak02Timeout  = 40 * time.Minute
	wak02RowBytes = 1024
	// wak02SmallKillMin and wak02SmallKillMax bound the random time Wake
	// streams small transactions before a small kill.
	wak02SmallKillMin = time.Second
	wak02SmallKillMax = 5 * time.Second
	// wak02LargeKillMax bounds the random delay between seeing the spill
	// start and the kill; Wake finishes a large transaction within a few
	// hundred milliseconds of starting to spill it.
	wak02LargeKillMax = 50 * time.Millisecond
	// wak02LargeFactor sizes the large transactions as a multiple of
	// large_tx_bytes: the kill lands 100-250 ms after the spill starts, and
	// a larger transaction keeps Wake inside it for longer than that.
	wak02LargeFactor = 3
	// wak02SpareKills is the number of kills kept for retrying large kills
	// that missed their transaction.
	wak02SpareKills = 3
	// wak02SpillPoll is the /metrics poll interval while waiting for the
	// spill to start.
	wak02SpillPoll = 5 * time.Millisecond
	// wak02SpillWait bounds waiting for Wake to start spilling.
	wak02SpillWait = 2 * time.Minute
	// wak02Settle lets the broker finish requests of the killed process
	// before the end offsets are listed.
	wak02Settle = time.Second
	wak02Tail   = 10 * time.Second
	wak02Big    = "wak02.big"
)

var (
	wak02A = verTable{Name: "wak02.small_a", Keys: 5000, PayloadBytes: 64, DeletePct: 10}
	wak02B = verTable{Name: "wak02.small_b", Keys: 5000, PayloadBytes: 64, DeletePct: 10}
)

// wak02Kill records one kill.
type wak02Kill struct {
	large bool
	// inside is set for a large kill that landed inside the transaction.
	inside       bool
	confirmed    uint64 // slot confirmed_flush_lsn after the kill
	preCommit    uint64 // large kill: WAL insert position before COMMIT
	txWALBytes   uint64 // large kill: WAL bytes of the large transaction
	ends         stack.Offsets
	spillToKill  time.Duration
	restartTaken time.Duration
}

// largeTx inserts rows ids [first, first+rows) of ~wak02RowBytes in one
// transaction and returns the WAL insert position before COMMIT, the WAL
// bytes from BEGIN to after COMMIT and the recorded changes.
func largeTx(ctx context.Context, first, rows int64) (pre, walBytes uint64, changes []stack.Change, err error) {
	conn, err := pgx.Connect(ctx, stack.PostgresDSN)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var start, preS, end string
	if err = conn.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&start); err != nil {
		return 0, 0, nil, fmt.Errorf("start lsn: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO wak02.big (id, ver, payload)
		SELECT g, 1, repeat(md5(g::text || random()::text), %d) FROM generate_series($1::bigint, $2::bigint) g`,
		wak02RowBytes/32), first, first+rows-1); err != nil {
		return 0, 0, nil, fmt.Errorf("insert: %w", err)
	}
	// Hold the small load back until COMMIT: a small transaction committing
	// between the read below and COMMIT would come before this one in commit
	// order, and Wake confirming it would move confirmed_flush_lsn past pre
	// while this transaction is still unconfirmed.
	if _, err = tx.Exec(ctx, "LOCK TABLE "+wak02A.Name+", "+wak02B.Name+" IN EXCLUSIVE MODE"); err != nil {
		return 0, 0, nil, fmt.Errorf("lock small tables: %w", err)
	}
	if err = tx.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&preS); err != nil {
		return 0, 0, nil, fmt.Errorf("pre-commit lsn: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, 0, nil, fmt.Errorf("commit: %w", err)
	}
	commit := time.Now()
	if err = conn.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&end); err != nil {
		return 0, 0, nil, fmt.Errorf("end lsn: %w", err)
	}
	lsns := make([]uint64, 3)
	for i, s := range []string{start, preS, end} {
		if lsns[i], err = event.ParseLSN(s); err != nil {
			return 0, 0, nil, fmt.Errorf("parse lsn: %w", err)
		}
	}
	changes = make([]stack.Change, 0, rows)
	for id := first; id < first+rows; id++ {
		changes = append(changes, stack.Change{
			Table: wak02Big, Key: stack.IDKey(id), Ver: 1, Op: stack.OpCreate,
			Commit: commit,
		})
	}
	return lsns[1], lsns[2] - lsns[0], changes, nil
}

// wak02Scenario drives the kills.
type wak02Scenario struct {
	pg      *stack.PG
	wake    *stack.Wake
	killer  *stack.Killer // the container keeps its id across restarts
	topics  []string
	rows    int64 // rows per large transaction
	rng     *rand.Rand
	kills   []wak02Kill
	large   []stack.Change
	nextBig int64
}

// smallKill lets Wake stream small transactions for a random time, then
// kills it.
func (s *wak02Scenario) smallKill(ctx context.Context) error {
	d := wak02SmallKillMin + time.Duration(s.rng.Int64N(int64(wak02SmallKillMax-wak02SmallKillMin)))
	time.Sleep(d)
	return s.killer.Kill(ctx) //nolint:wrapcheck // stack errors carry context
}

// largeKill runs a large transaction, waits until Wake starts spilling it,
// waits a random delay and kills Wake.
func (s *wak02Scenario) largeKill(ctx context.Context) (wak02Kill, error) {
	k := wak02Kill{large: true}
	before, err := gaugeValue(ctx, s.wake, "wake_spilled_transactions_total")
	if err != nil {
		return k, err
	}
	type txResult struct {
		pre, wal uint64
		changes  []stack.Change
		err      error
	}
	first := s.nextBig
	s.nextBig += s.rows
	done := make(chan txResult, 1)
	go func() {
		var r txResult
		r.pre, r.wal, r.changes, r.err = largeTx(ctx, first, s.rows)
		done <- r
	}()
	spillCtx, cancel := context.WithTimeout(ctx, wak02SpillWait)
	defer cancel()
	err = waitUntil(spillCtx, wak02SpillPoll, func() bool {
		v, gerr := gaugeValue(spillCtx, s.wake, "wake_spilled_transactions_total")
		return gerr == nil && v > before
	})
	spilled := time.Now()
	if err != nil {
		<-done
		return k, fmt.Errorf("wake did not start spilling the large transaction: %w", err)
	}
	time.Sleep(time.Duration(s.rng.Int64N(int64(wak02LargeKillMax))))
	if err := s.killer.Kill(ctx); err != nil {
		<-done
		return k, err //nolint:wrapcheck // stack errors carry context
	}
	k.spillToKill = time.Since(spilled)
	r := <-done
	if r.err != nil {
		return k, fmt.Errorf("large transaction: %w", r.err)
	}
	k.preCommit, k.txWALBytes = r.pre, r.wal
	s.large = append(s.large, r.changes...)
	return k, nil
}

// afterKill records the slot position and the topics' end offsets of the
// dead process and starts Wake again.
func (s *wak02Scenario) afterKill(ctx context.Context, k wak02Kill) error {
	time.Sleep(wak02Settle)
	info, err := s.pg.Slot(ctx, s.wake.Slot())
	if err != nil {
		return err //nolint:wrapcheck // stack errors carry context
	}
	k.confirmed = info.ConfirmedLSN
	k.inside = k.large && k.confirmed <= k.preCommit
	if k.ends, err = stack.EndOffsets(ctx, s.topics...); err != nil {
		return err //nolint:wrapcheck // stack errors carry context
	}
	start := time.Now()
	if err := s.wake.Start(ctx); err != nil {
		return fmt.Errorf("start wake after kill %d: %w", len(s.kills)+1, err)
	}
	k.restartTaken = time.Since(start)
	s.kills = append(s.kills, k)
	return nil
}

// run performs kills kills of which at least inside land inside a large
// transaction. Each kill is large with probability (inside still needed) /
// (kills left), and always once the remaining kills, less wak02SpareKills,
// must all be; a large kill that misses the transaction counts as an
// ordinary kill.
func (s *wak02Scenario) run(ctx context.Context, kills, inside int64) error {
	hits := int64(0)
	for i := range kills {
		need, left := inside-hits, kills-i
		large := need > 0 && (need+wak02SpareKills >= left || s.rng.Int64N(left) < need)
		var (
			k   wak02Kill
			err error
		)
		if large {
			k, err = s.largeKill(ctx)
		} else {
			err = s.smallKill(ctx)
		}
		if err != nil {
			return fmt.Errorf("kill %d (large %t): %w", i+1, large, err)
		}
		if err := s.afterKill(ctx, k); err != nil {
			return err
		}
		if s.kills[len(s.kills)-1].inside {
			hits++
		}
	}
	return nil
}

// TestAcceptance_WAK02_CrashNoLoss implements WAK-02: kill -9 of Wake 20 times with large and small
// transactions; no loss, duplicates only after last confirmed LSN.
func TestAcceptance_WAK02_CrashNoLoss(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-02"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	th := readThresholds(t, id, c, "kills", "kills_in_large_tx_pct", "large_tx_bytes", "max_tx_bytes", "lost_max",
		"duplicates_before_confirmed_lsn_max")
	removableMin, err := c.Float("duplicates_removable_by_id_pct_min")
	failIf(t, id, err, "threshold duplicates_removable_by_id_pct_min")

	stack.EnsureServices(t, "postgres", "kafka")
	pg := stack.ConnectPG(t, 2)
	ctx := withTimeout(t, wak02Timeout)
	pg.Exec(t, "DROP SCHEMA IF EXISTS wak02 CASCADE; CREATE SCHEMA wak02; "+verTableDDL(wak02A)+verTableDDL(wak02B)+
		"CREATE TABLE wak02.big (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text);"+
		"ALTER TABLE wak02.small_a REPLICA IDENTITY FULL; ALTER TABLE wak02.small_b REPLICA IDENTITY FULL;")
	t.Cleanup(func() { dropSchema(t, pg, "wak02") })

	prefix := fmt.Sprintf("w02_%d", time.Now().Unix())
	topics := []string{
		stack.Topic(prefix, "wak02", "small_a"), stack.Topic(prefix, "wak02", "small_b"),
		stack.Topic(prefix, "wak02", "big"),
	}
	t.Cleanup(func() { dropTopics(t, append(topics, stack.SchemaTopic(prefix))...) })
	streamCfg := fmt.Sprintf("  max_tx_bytes: %d", th["max_tx_bytes"])
	cfg := strings.Replace(wakeConfig("wak02", []string{"wak02.*"}, kafkaSink(prefix), ""),
		"stream:\n", "stream:\n"+streamCfg+"\n", 1)
	wake := stack.ConfigureWake(t, stack.WakeOptions{ID: id, Config: cfg, FreshState: true})

	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: wak02Rate, Conns: wak02Conns, Seed: wak02Seed, Tx: newVerTx(wak02Conns, wak02A, wak02B),
	})
	failIf(t, id, err, "start load")
	t.Cleanup(load.Abort)

	killer, err := wake.NewKiller(ctx)
	failIf(t, id, err, "resolve the wake container")
	t.Cleanup(killer.Close)
	rng := rand.New(rand.NewPCG(wak02Seed, wak02Seed+1)) //nolint:gosec // reproducible kill points
	sc := &wak02Scenario{
		pg: pg, wake: wake, killer: killer, topics: topics, rng: rng, nextBig: 1,
		rows: wak02LargeFactor * th["large_tx_bytes"] / wak02RowBytes,
	}
	runErr := sc.run(ctx, th["kills"], (th["kills"]*th["kills_in_large_tx_pct"]+99)/100)
	time.Sleep(wak02Tail)
	load.Stop()
	res := load.Wait()
	failIf(t, id, runErr, "kill scenario")
	loadEnd := time.Now()
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN")
	drain := time.Since(loadEnd)

	finishWak02(ctx, t, id, th, removableMin, sc, res, drain)
}

// finishWak02 reads the topics, applies M3 and the duplicate checks and
// records the result.
func finishWak02(ctx context.Context, t *testing.T, id string, th map[string]int64, removableMin float64,
	sc *wak02Scenario, res stack.LoadResult, drain time.Duration,
) {
	t.Helper()
	recs, err := stack.ReadTopics(ctx, sc.topics...)
	failIf(t, id, err, "read Kafka topics")
	dups, err := analyzeRedeliveries(recs, sc.kills)
	failIf(t, id, err, "analyze redeliveries")
	changes := append(append([]stack.Change(nil), res.Changes...), sc.large...)
	lost, err := stack.FindLost(changes, dups.events, "ver")
	failIf(t, id, err, "M3 lost")

	inside, minWAL := 0, uint64(0)
	var spillToKill []string
	for _, k := range sc.kills {
		if k.inside {
			inside++
		}
		if k.large {
			if minWAL == 0 || k.txWALBytes < minWAL {
				minWAL = k.txWALBytes
			}
			spillToKill = append(spillToKill, k.spillToKill.Round(time.Millisecond).String())
		}
	}
	insidePct := stack.Pct(inside, len(sc.kills))
	removable := stack.Pct(dups.removable, dups.redeliveries)
	loadOK := res.Errors == 0 && res.Dropped == 0
	pass := int64(len(sc.kills)) == th["kills"] && insidePct >= float64(th["kills_in_large_tx_pct"]) &&
		minWAL >= uint64(th["large_tx_bytes"]) && int64(len(lost.Lost)) <= th["lost_max"] && //nolint:gosec // positive
		int64(dups.beforeConfirmed) <= th["duplicates_before_confirmed_lsn_max"] && removable >= removableMin &&
		loadOK

	details := fmt.Sprintf("kills %d (large %d, inside a large transaction %d = %.1f%%); large transactions of "+
		"%d rows x ~%d bytes, smallest %d WAL bytes, spill start to kill %v; max_tx_bytes %d; small load %s; "+
		"final LSN confirmed %s after the load ended; Kafka records %d, events %d; recorded changes %d, lost %d, "+
		"unmatched %d; redeliveries %d (same id %d = %.2f%%), duplicates by id %d, of which before the confirmed "+
		"LSN of their kill %d %v",
		len(sc.kills), countLarge(sc.kills), inside, insidePct, sc.rows, wak02RowBytes, minWAL, spillToKill,
		th["max_tx_bytes"], res.Summary(), drain.Round(time.Millisecond), len(recs), len(dups.events),
		lost.Recorded, len(lost.Lost), lost.Unmatched, dups.redeliveries, dups.removable, removable,
		dups.duplicates, dups.beforeConfirmed, dups.examples)
	if len(lost.Lost) > 0 {
		details += "; first lost: " + describeChanges(lost.Lost)
	}
	if len(res.FirstErrs) > 0 {
		details += "; load errors: " + strings.Join(res.FirstErrs, "; ")
	}
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"kills": len(sc.kills), "kills_in_large_tx_pct": insidePct, "large_tx_bytes": minWAL,
			"max_tx_bytes": th["max_tx_bytes"], "lost": len(lost.Lost), "recorded_changes": lost.Recorded,
			"duplicates": dups.duplicates, "duplicates_before_confirmed_lsn": dups.beforeConfirmed,
			"redeliveries": dups.redeliveries, "duplicates_removable_by_id_pct": removable,
			"drain_after_load_s": drain.Seconds(),
		},
		Pass:    pass,
		Details: details,
	})
}

func countLarge(kills []wak02Kill) int {
	n := 0
	for _, k := range kills {
		if k.large {
			n++
		}
	}
	return n
}

// redeliveryAnalysis is the outcome of analyzeRedeliveries.
type redeliveryAnalysis struct {
	events []*event.Event
	// duplicates are events whose id was already seen (M3).
	duplicates int
	// beforeConfirmed are duplicates written after kill k whose commit LSN
	// is below the slot's confirmed LSN at kill k, plus duplicates written
	// before the first kill.
	beforeConfirmed int
	examples        []string
	// redeliveries counts, per change (table, key, op, ver), the events
	// beyond the first; removable those of them repeating a seen id.
	redeliveries, removable int
}

// analyzeRedeliveries classifies the records (ordered by topic, partition
// and offset) by the kill after which they were written: the first kill
// whose end offset of the partition is at or below the record's offset.
func analyzeRedeliveries(recs []stack.Record, kills []wak02Kill) (redeliveryAnalysis, error) {
	var an redeliveryAnalysis
	seen := map[string]bool{}
	type change struct {
		table, key, op string
		ver            int64
	}
	perChange := map[change]int{}
	for _, r := range recs {
		dec, err := stack.DecodePayloads([][]byte{r.Value})
		if err != nil {
			return an, err //nolint:wrapcheck // stack errors carry context
		}
		if len(dec.Events) == 0 {
			continue
		}
		e := dec.Events[0]
		an.events = append(an.events, e)
		key, err := stack.EventKey(e)
		if err != nil {
			return an, err //nolint:wrapcheck // stack errors carry context
		}
		img := e.After
		if e.Op == event.OpDelete {
			img = e.Before
		}
		ver, _ := stack.IntColumn(img, "ver")
		ch := change{stack.TableName(e), key, string(e.Op), ver}
		if perChange[ch] > 0 {
			an.redeliveries++
			if seen[e.ID] {
				an.removable++
			}
		}
		perChange[ch]++
		if !seen[e.ID] {
			seen[e.ID] = true
			continue
		}
		an.duplicates++
		if err := an.checkDuplicate(r, e, kills); err != nil {
			return an, err
		}
	}
	return an, nil
}

// checkDuplicate counts a duplicate written before any kill, or after kill
// k with a commit LSN below the confirmed LSN at kill k.
func (an *redeliveryAnalysis) checkDuplicate(r stack.Record, e *event.Event, kills []wak02Kill) error {
	epoch := 0
	for i, k := range kills {
		if r.Offset >= k.ends[r.Topic][r.Partition] {
			epoch = i + 1
		}
	}
	lsn, err := event.ParseLSN(e.LSN)
	if err != nil {
		return fmt.Errorf("event %s lsn: %w", e.ID, err)
	}
	if epoch > 0 && lsn >= kills[epoch-1].confirmed {
		return nil
	}
	an.beforeConfirmed++
	if len(an.examples) < 5 {
		conf := "none (before the first kill)"
		if epoch > 0 {
			conf = event.FormatLSN(kills[epoch-1].confirmed)
		}
		an.examples = append(an.examples, fmt.Sprintf("%s lsn %s after kill %d confirmed %s", e.ID, e.LSN, epoch, conf))
	}
	return nil
}
