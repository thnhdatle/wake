// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/runner"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
	"github.com/thnhdatle/wake/pkg/event"
)

// WAK-05 runs the M6 workload against Wake running natively on the host
// with the compose Postgres and Kafka. Kafka ack time is the broker's append
// time: the topics are created with message.timestamp.type=LogAppendTime.
// The broker and Postgres share the Docker VM clock, so the M6 latency
// ack_host - (ts_ms + offset) equals append_time - ts_ms; the offset (host -
// Postgres clock) is still measured, gates the run (Blocked above
// clock_offset_ms_max) and places the measured window on the broker clock.
const (
	wak05Seed    = uint64(5)
	wak05Timeout = 60 * time.Minute
	// wak05MinRate is the share of the offered rate the generator must
	// achieve for the run to count.
	wak05MinRate = 0.98
	// wak05RowOverhead approximates the bytes of a row besides the payload
	// (bigint id, tuple header), so a row is about row_bytes.
	wak05RowOverhead = 32
)

// wak05Params are the criterion's workload values.
type wak05Params struct {
	rate                           float64 // offered transactions (one event each) per second
	p99Max                         float64
	rowBytes, tables, conns, parts int
	warmup, measure                time.Duration
	runs, offsetSamples            int
	offsetMax                      time.Duration
	eventsPerSMin                  float64
}

func wak05ReadParams(b *testing.B, id string, c *harness.Entry) wak05Params {
	b.Helper()
	ints := map[string]int64{}
	for _, k := range []string{
		"row_bytes", "tables", "loadgen_connections", "topic_partitions", "warmup_s",
		"measure_s", "runs", "clock_offset_samples", "clock_offset_ms_max",
	} {
		v, err := c.Int(k)
		if err != nil {
			harness.Fail(b, id, "threshold "+k+": "+err.Error())
		}
		ints[k] = v
	}
	minRate, err := c.Float("events_per_s_min")
	if err != nil {
		harness.Fail(b, id, "threshold events_per_s_min: "+err.Error())
	}
	// The offered rate is above the threshold: with an offered rate equal to
	// it, a Wake that keeps up passes or fails on the few events whose latency
	// carries them across the edges of the measured window.
	rate, err := c.Float("offered_events_per_s")
	if err != nil {
		harness.Fail(b, id, "threshold offered_events_per_s: "+err.Error())
	}
	if rate <= minRate {
		harness.Failf(b, id, "offered_events_per_s %.0f must exceed events_per_s_min %.0f", rate, minRate)
	}
	p99, err := c.Float("latency_p99_ms_max")
	if err != nil {
		harness.Fail(b, id, "threshold latency_p99_ms_max: "+err.Error())
	}
	return wak05Params{
		rate: rate, eventsPerSMin: minRate, p99Max: p99, rowBytes: int(ints["row_bytes"]),
		tables: int(ints["tables"]), conns: int(ints["loadgen_connections"]),
		parts: int(ints["topic_partitions"]), warmup: time.Duration(ints["warmup_s"]) * time.Second,
		measure: time.Duration(ints["measure_s"]) * time.Second, runs: int(ints["runs"]),
		offsetSamples: int(ints["clock_offset_samples"]),
		offsetMax:     time.Duration(ints["clock_offset_ms_max"]) * time.Millisecond,
	}
}

// wak05Result is one run's outcome.
type wak05Result struct {
	offset     time.Duration
	acked      int // events acked in the measured window
	throughput float64
	p50, p99   float64 // ms
	maxMs      float64
	load       stack.LoadResult
	drain      time.Duration
	records    int
}

func (r wak05Result) String() string {
	return fmt.Sprintf("offset %s, acked in window %d, %.1f events/s, latency p50 %.1f ms p99 %.1f ms max %.1f ms, "+
		"records %d, drain %s, load %s", r.offset.Round(10*time.Microsecond), r.acked, r.throughput, r.p50, r.p99,
		r.maxMs, r.records, r.drain.Round(time.Millisecond), r.load.Summary())
}

// clockOffset implements the M6 offset: the median over n samples of host
// time at the round-trip midpoint minus Postgres clock_timestamp().
func clockOffset(ctx context.Context, pg *stack.PG, n int) (time.Duration, error) {
	offs := make([]float64, 0, n)
	for range n {
		var us int64
		t0 := time.Now()
		if err := pg.QueryRow(ctx, "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint", nil,
			&us); err != nil {
			return 0, err //nolint:wrapcheck // stack errors carry context
		}
		t1 := time.Now()
		mid := t0.Add(t1.Sub(t0) / 2)
		offs = append(offs, float64(mid.Sub(time.UnixMicro(us))))
	}
	return time.Duration(median(offs)), nil
}

// wak05Tx inserts one ~rowBytes row into a random table of n.
func wak05Tx(tables, rowBytes int) stack.TxFunc {
	payload := strings.Repeat("x", max(rowBytes-wak05RowOverhead, 1))
	stmts := make([]string, tables)
	for i := range stmts {
		stmts[i] = fmt.Sprintf("INSERT INTO wak05.t%d (payload) VALUES ($1)", i+1)
	}
	return func(ctx context.Context, conn *pgx.Conn, _ int, rng *rand.Rand) ([]stack.Change, error) {
		_, err := conn.Exec(ctx, stmts[rng.IntN(tables)], payload)
		return nil, err //nolint:wrapcheck // reported in the load summary
	}
}

// wak05Once runs warm-up plus measured window once with a fresh Wake,
// tables and topics. blocked is set when the clock offset is too large.
func wak05Once(ctx context.Context, b *testing.B, id string, pg *stack.PG, p wak05Params, run int,
) (res wak05Result, blocked string) {
	b.Helper()
	var ddl strings.Builder
	ddl.WriteString("DROP SCHEMA IF EXISTS wak05 CASCADE; CREATE SCHEMA wak05;")
	prefix := fmt.Sprintf("w05_%d_%d", time.Now().Unix(), run)
	topics := make([]string, p.tables) //nolint:prealloc // indexed below
	for i := range p.tables {
		fmt.Fprintf(&ddl, "CREATE TABLE wak05.t%d (id bigserial PRIMARY KEY, payload text NOT NULL);", i+1)
		topics[i] = stack.Topic(prefix, "wak05", fmt.Sprintf("t%d", i+1))
	}
	_, err := pg.ExecContext(ctx, ddl.String())
	failIf(b, id, err, "create tables")
	//nolint:contextcheck // cleanup outlives ctx
	b.Cleanup(func() { dropTopics(b, append(slices.Clone(topics), stack.SchemaTopic(prefix))...) })
	err = stack.CreateTopics(ctx, int32(p.parts), //nolint:gosec // small
		map[string]string{"message.timestamp.type": "LogAppendTime"}, topics...)
	failIf(b, id, err, "create topics")

	cfg := stack.NativeConfig(wakeConfig("wak05", []string{"wak05.*"}, kafkaSinkPartitions(prefix, p.parts), ""),
		b.TempDir())
	wake := stack.StartNativeWake(b, stack.NativeOptions{ID: id, Config: cfg}) //nolint:contextcheck // own timeouts

	if res.offset, err = clockOffset(ctx, pg, p.offsetSamples); err != nil {
		harness.Fail(b, id, "clock offset: "+err.Error())
	}
	if res.offset.Abs() > p.offsetMax {
		return res, fmt.Sprintf("run %d: |clock offset| %s > %s", run, res.offset, p.offsetMax)
	}
	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: p.rate, Duration: p.warmup + p.measure, Conns: p.conns, Seed: wak05Seed + uint64(run), //nolint:gosec
		Tx: wak05Tx(p.tables, p.rowBytes),
	})
	failIf(b, id, err, "start load")
	start := time.Now()
	res.load = load.Wait()
	loadEnd := time.Now()
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(b, id, err, "wait for Wake to confirm the final LSN")
	res.drain = time.Since(loadEnd)
	if exited, werr := wake.Exited(); exited {
		harness.Failf(b, id, "native wake exited during the run: %v", werr)
	}
	// The measured window on the broker clock (host time - offset).
	from := start.Add(p.warmup).Add(-res.offset)
	to := from.Add(p.measure)
	lat, records, err := wak05Latencies(ctx, topics, from, to)
	failIf(b, id, err, "read topics")
	res.records, res.acked = records, len(lat)
	res.throughput = float64(len(lat)) / p.measure.Seconds()
	slices.Sort(lat)
	res.p50, res.p99 = quantile(lat, 0.50), quantile(lat, 0.99)
	if len(lat) > 0 {
		res.maxMs = lat[len(lat)-1]
	}
	return res, ""
}

// wak05Latencies scans the topics and returns, for every event whose append
// time lies in [from, to), append time - ts_ms in milliseconds, and the
// number of records scanned.
func wak05Latencies(ctx context.Context, topics []string, from, to time.Time) ([]float64, int, error) {
	var (
		lat     []float64
		records int
		decErr  error
	)
	err := stack.ScanTopics(ctx, func(r stack.Record) {
		records++
		if event.IsKafkaTombstone(r.Value) || r.Timestamp.Before(from) || !r.Timestamp.Before(to) {
			return
		}
		var e struct {
			TsMs int64 `json:"ts_ms"`
		}
		if err := json.Unmarshal(r.Value, &e); err != nil {
			decErr = fmt.Errorf("record %s[%d]@%d: %w", r.Topic, r.Partition, r.Offset, err)
			return
		}
		lat = append(lat, float64(r.Timestamp.Sub(time.UnixMilli(e.TsMs)))/float64(time.Millisecond))
	}, topics...)
	if err == nil {
		err = decErr
	}
	return lat, records, err //nolint:wrapcheck // stack errors carry context
}

// quantile returns the q-quantile of sorted xs (nearest rank).
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[min(max(i, 0), len(sorted)-1)]
}

// BenchmarkAcceptance_WAK05_Throughput implements WAK-05: Throughput and latency Postgres to Kafka, single slot,
// Wake native.
func BenchmarkAcceptance_WAK05_Throughput(b *testing.B) {
	const id = "WAK-05"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(b, id, "load criterion: "+err.Error())
	}
	p := wak05ReadParams(b, id, c)
	stack.EnsureServices(b, "postgres", "kafka")
	pg := stack.ConnectPG(b, 2)
	ctx, cancel := context.WithTimeout(context.Background(), wak05Timeout)
	b.Cleanup(cancel)
	b.Cleanup(func() { dropSchema(b, pg, "wak05") })

	results := make([]wak05Result, 0, p.runs)
	for run := 1; run <= p.runs; run++ {
		r, blocked := wak05Once(ctx, b, id, pg, p, run)
		if blocked != "" {
			harness.Finish(b, harness.Result{
				ID: id, Measured: map[string]any{"clock_offset_ms": float64(r.offset) / float64(time.Millisecond)},
				Details: runner.BlockedPrefix + " " + blocked,
			})
			return
		}
		b.Logf("run %d: %s", run, r)
		results = append(results, r)
	}
	finishWak05(b, id, p, results)
}

// finishWak05 takes the medians over the runs and records the result.
func finishWak05(b *testing.B, id string, p wak05Params, results []wak05Result) {
	b.Helper()
	tput, p99, p50 := make([]float64, 0, len(results)), make([]float64, 0, len(results)),
		make([]float64, 0, len(results))
	runs := make([]string, 0, len(results))
	loadOK := true
	for i, r := range results {
		tput, p99, p50 = append(tput, r.throughput), append(p99, r.p99), append(p50, r.p50)
		runs = append(runs, fmt.Sprintf("run %d: %s", i+1, r))
		if r.load.Errors > 0 || r.load.Dropped > 0 || r.load.AchievedRate() < wak05MinRate*p.rate {
			loadOK = false
		}
	}
	medT, medP99 := median(tput), median(p99)
	b.ReportMetric(medT, "events/s")
	b.ReportMetric(medP99, "p99-ms")
	harness.Finish(b, harness.Result{
		ID: id,
		Measured: map[string]any{
			"events_per_s": medT, "events_per_s_min_run": slices.Min(tput), "events_per_s_max_run": slices.Max(tput),
			"latency_p99_ms": medP99, "latency_p99_ms_min_run": slices.Min(p99),
			"latency_p99_ms_max_run": slices.Max(p99), "latency_p50_ms": median(p50), "runs": len(results),
			"offered_events_per_s": p.rate, "loadgen_ok": loadOK,
		},
		Pass: medT >= p.eventsPerSMin && medP99 < p.p99Max && loadOK,
		Details: fmt.Sprintf("Wake native, compose Postgres and Kafka; %d tables, %d-byte rows, %d loadgen "+
			"connections, %d-partition topics (LogAppendTime), default sink batching; offered %.0f tx/s "+
			"(single-row INSERT), warm-up %s, measured %s; medians over %d runs: %.1f events/s, p99 %.1f ms; %s",
			p.tables, p.rowBytes, p.conns, p.parts, p.rate, p.warmup, p.measure, len(results), medT, medP99,
			strings.Join(runs, "; ")),
	})
}
