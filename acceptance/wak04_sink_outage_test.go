// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
)

// WAK-04 scenario parameters. The criterion fixes the outage, the rate, the
// lag settings and the slot poll; the table, payload size, warm-up and
// recovery tail are chosen by the test.
const (
	wak04Seed    = uint64(4)
	wak04Conns   = 16
	wak04Timeout = 20 * time.Minute
	// wak04Warmup runs the load with Kafka up before the outage, so the
	// topic exists and the pipeline is streaming.
	wak04Warmup = 15 * time.Second
	// wak04Tail runs the load after Kafka is back.
	wak04Tail    = 30 * time.Second
	wak04MinRate = 0.98
	// wak04MetricPoll is how often the test samples wake_slot_lag_bytes.
	wak04MetricPoll = time.Second
)

var wak04Table = verTable{Name: "wak04.items", Keys: 20000, PayloadBytes: 256, DeletePct: 10}

// alertBody is the lag.alert_webhook payload.
type alertBody struct {
	Rule      string  `json:"rule"`
	State     string  `json:"state"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	TS        string  `json:"ts"`
}

// wak04Watch is what the 1 s slot poll and the metric poll observed.
type wak04Watch struct {
	mu         sync.Mutex
	tCross     time.Time // M5: first slot poll with lag above warn_bytes
	crossLag   int64
	maxSlotLag int64
	polls      int
	pollErrs   int
	metric     []float64 // distinct consecutive wake_slot_lag_bytes samples
	metricErrs int
}

// pollSlot runs the M5 poll every interval until ctx ends.
func (w *wak04Watch) pollSlot(ctx context.Context, pg *stack.PG, slot string, warn int64, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		info, err := pg.Slot(ctx, slot)
		now := time.Now()
		w.mu.Lock()
		w.polls++
		if err != nil {
			w.pollErrs++
		} else {
			w.maxSlotLag = max(w.maxSlotLag, info.LagBytes)
			if w.tCross.IsZero() && info.LagBytes > warn {
				w.tCross, w.crossLag = now, info.LagBytes
			}
		}
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// pollMetric samples wake_slot_lag_bytes until ctx ends, keeping the value
// each time it changes.
func (w *wak04Watch) pollMetric(ctx context.Context, wake *stack.Wake) {
	tick := time.NewTicker(wak04MetricPoll)
	defer tick.Stop()
	for {
		v, err := gaugeValue(ctx, wake, "wake_slot_lag_bytes")
		w.mu.Lock()
		if err != nil {
			w.metricErrs++
		} else if n := len(w.metric); n == 0 || w.metric[n-1] != v {
			w.metric = append(w.metric, v)
		}
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// rises counts the samples above the previous one and returns the largest.
func (w *wak04Watch) rises() (n int, peak float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, v := range w.metric {
		if i > 0 && v > w.metric[i-1] {
			n++
		}
		peak = max(peak, v)
	}
	return n, peak
}

// gaugeValue reads an unlabelled sample from /metrics.
func gaugeValue(ctx context.Context, wake *stack.Wake, name string) (float64, error) {
	text, err := wake.Admin().Metrics(ctx)
	if err != nil {
		return 0, err //nolint:wrapcheck // stack errors carry context
	}
	for line := range strings.Lines(text) {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), name+" "); ok {
			f, perr := strconv.ParseFloat(v, 64)
			if perr != nil {
				return 0, fmt.Errorf("parse %q: %w", line, perr)
			}
			return f, nil
		}
	}
	return 0, fmt.Errorf("no %s sample", name)
}

// firstAlert returns the first accepted delivery carrying a firing alert of
// rule, and every alert body received.
func firstAlert(recv *stack.Receiver, rule string) (time.Time, *alertBody, []alertBody) {
	var (
		at    time.Time
		first *alertBody
		all   []alertBody
	)
	for _, d := range recv.Deliveries() {
		for _, it := range d.Items {
			var a alertBody
			if json.Unmarshal(it, &a) != nil || a.Rule == "" {
				continue
			}
			all = append(all, a)
			if first == nil && a.Rule == rule && a.State == "firing" {
				at, first = d.At, &all[len(all)-1]
			}
		}
	}
	if first != nil {
		cp := *first
		first = &cp
	}
	return at, first, all
}

// TestAcceptance_WAK04_SinkOutage implements WAK-04: Kafka down 3 min under 500 tx/s; lag metric rises,
// alert within 60 s of T_cross, nothing lost after recovery.
func TestAcceptance_WAK04_SinkOutage(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-04"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	th := readThresholds(t, id, c, "outage_s", "tx_per_s", "lag_interval_s", "lag_warn_bytes",
		"slot_poll_interval_s", "lag_rises_min", "alert_after_t_cross_s_max", "lost_max")
	outage := time.Duration(th["outage_s"]) * time.Second
	stack.EnsureServices(t, "postgres", "kafka")
	pg := stack.ConnectPG(t, 2)
	ctx := withTimeout(t, wak04Timeout)
	pg.Exec(t, "DROP SCHEMA IF EXISTS wak04 CASCADE; CREATE SCHEMA wak04; "+verTableDDL(wak04Table))
	t.Cleanup(func() { dropSchema(t, pg, "wak04") })
	// Kafka is started again even when the test stops during the outage.
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if serr := stack.StartService(sctx, "kafka"); serr != nil {
			t.Logf("start kafka: %v", serr)
		}
	})

	recv := stack.StartReceiver(t, stack.ReceiverOptions{})
	prefix := fmt.Sprintf("w04_%d", time.Now().Unix())
	topic := stack.Topic(prefix, "wak04", "items")
	t.Cleanup(func() { dropTopics(t, topic, stack.SchemaTopic(prefix)) })
	lagCfg := fmt.Sprintf(`lag:
  interval: %ds
  warn_bytes: %d
  alert_webhook: %q`, th["lag_interval_s"], th["lag_warn_bytes"], recv.URL("alert"))
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID: id, Config: wakeConfig("wak04", []string{"wak04.*"}, kafkaSink(prefix), lagCfg), FreshState: true,
	})

	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: float64(th["tx_per_s"]), Conns: wak04Conns, Seed: wak04Seed, Tx: newVerTx(wak04Conns, wak04Table),
	})
	failIf(t, id, err, "start load")
	t.Cleanup(load.Abort)
	loadStart := time.Now()
	time.Sleep(wak04Warmup)

	var watch wak04Watch
	wctx, stopWatch := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		watch.pollSlot(wctx, pg, wake.Slot(), th["lag_warn_bytes"], time.Duration(th["slot_poll_interval_s"])*time.Second)
	})
	wg.Go(func() { watch.pollMetric(wctx, wake) })

	downAt := time.Now()
	failIf(t, id, stack.StopService(ctx, "kafka"), "stop kafka")
	time.Sleep(outage - time.Since(downAt))
	upStart := time.Now()
	failIf(t, id, stack.StartService(ctx, "kafka"), "start kafka")
	upAt := time.Now()
	time.Sleep(wak04Tail)
	load.Stop()
	res := load.Wait()
	loadEnd := time.Now()
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN after recovery")
	drain := time.Since(loadEnd)
	stopWatch()
	wg.Wait()
	cs, err := wake.State(ctx)
	failIf(t, id, err, "inspect the wake container")

	finishWak04(ctx, t, id, th, wak04Run{
		res: res, recv: recv, watch: &watch, cs: cs, topic: topic, loadStart: loadStart, downAt: downAt,
		upStart: upStart, upAt: upAt, drain: drain,
	})
}

// wak04Run is what the scenario recorded for the evaluation.
type wak04Run struct {
	res                              stack.LoadResult
	recv                             *stack.Receiver
	watch                            *wak04Watch
	cs                               stack.ContainerState
	topic                            string
	loadStart, downAt, upStart, upAt time.Time
	drain                            time.Duration
}

// finishWak04 reads the topic, applies M3 and the alert checks and records
// the result.
func finishWak04(ctx context.Context, t *testing.T, id string, th map[string]int64, r wak04Run) {
	t.Helper()
	res, recv, watch, cs, topic := r.res, r.recv, r.watch, r.cs, r.topic
	loadStart, downAt, upStart, upAt, drain := r.loadStart, r.downAt, r.upStart, r.upAt, r.drain
	outage := time.Duration(th["outage_s"]) * time.Second
	recs, err := stack.ReadTopics(ctx, topic)
	failIf(t, id, err, "read Kafka topic")
	dec, err := stack.DecodePayloads(stack.Values(recs))
	failIf(t, id, err, "decode Kafka records")
	lost, err := stack.FindLost(res.Changes, dec.Events, "ver")
	failIf(t, id, err, "M3 lost")
	dups := stack.Duplicates(dec.Events)

	alertAt, alert, alerts := firstAlert(recv, "slot_lag_warn")
	rises, peak := watch.rises()
	watch.mu.Lock()
	tCross, crossLag, maxSlotLag, polls, pollErrs := watch.tCross, watch.crossLag, watch.maxSlotLag, watch.polls,
		watch.pollErrs
	samples, metricErrs := len(watch.metric), watch.metricErrs
	watch.mu.Unlock()

	alertDelay, haveDelay := 0.0, alert != nil && !tCross.IsZero()
	if haveDelay {
		alertDelay = alertAt.Sub(tCross).Seconds()
	}
	achieved := res.AchievedRate()
	loadOK := res.Errors == 0 && res.Dropped == 0 && achieved >= wak04MinRate*float64(th["tx_per_s"])
	down := upStart.Sub(downAt)
	pass := int64(rises) >= th["lag_rises_min"] && haveDelay &&
		alertDelay <= float64(th["alert_after_t_cross_s_max"]) && int64(len(lost.Lost)) <= th["lost_max"] &&
		down >= outage && loadOK && cs.Running && cs.RestartCount == 0

	details := fmt.Sprintf("load %s; kafka stopped %s after the load began, down %s (start took %s); "+
		"slot polls %d (errors %d): T_cross %s after the stop at lag %d bytes, max lag %d bytes; "+
		"wake_slot_lag_bytes samples %d (errors %d), rises %d, peak %.0f; alerts %+v; first %s firing %s; "+
		"wake running %t restarts %d; final LSN confirmed %s after the load ended; Kafka records %d; "+
		"recorded changes %d, lost %d, duplicates %d, unmatched %d; topic %s",
		res.Summary(), downAt.Sub(loadStart).Round(time.Second), down.Round(time.Millisecond),
		upAt.Sub(upStart).Round(time.Millisecond), polls, pollErrs, sinceOrNone(tCross, downAt), crossLag,
		maxSlotLag, samples, metricErrs, rises, peak, alerts, "slot_lag_warn", delayOrNone(haveDelay, alertDelay),
		cs.Running, cs.RestartCount, drain.Round(time.Millisecond), len(recs), lost.Recorded, len(lost.Lost),
		dups, lost.Unmatched, topic)
	if len(lost.Lost) > 0 {
		details += "; first lost: " + describeChanges(lost.Lost)
	}
	if len(res.FirstErrs) > 0 {
		details += "; load errors: " + strings.Join(res.FirstErrs, "; ")
	}
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			"outage_s": down.Seconds(), "tx_per_s": th["tx_per_s"], "tx_per_s_achieved": achieved,
			"lag_interval_s": th["lag_interval_s"], "lag_warn_bytes": th["lag_warn_bytes"],
			"slot_poll_interval_s": th["slot_poll_interval_s"], "lag_rises": rises, "lag_metric_peak_bytes": peak,
			"slot_lag_max_bytes": maxSlotLag, "alert_received": alert != nil,
			"alert_after_t_cross_s": alertDelay, "t_cross_seen": !tCross.IsZero(), "lost": len(lost.Lost),
			"recorded_changes": lost.Recorded,
			"duplicates":       dups, "wake_restarts": cs.RestartCount, "drain_after_load_s": drain.Seconds(),
		},
		Pass:    pass,
		Details: details,
	})
}

func sinceOrNone(at, from time.Time) string {
	if at.IsZero() {
		return "never"
	}
	return at.Sub(from).Round(time.Millisecond).String()
}

func delayOrNone(ok bool, s float64) string {
	if !ok {
		return "not measured (no alert or no T_cross)"
	}
	return fmt.Sprintf("%.3fs after T_cross", s)
}
