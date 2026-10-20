// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/stack"
)

// WAK-08 scenario parameters. The criterion fixes the failure and slow
// shares; the workload size and the slow delay are chosen by the test (the delay stays
// below the default webhook.timeout of 10 s so a slow response succeeds).
const (
	wak08Table     = "wak08.items"
	wak08Seed      = uint64(8)
	wak08Rate      = 100 // transactions per second
	wak08Duration  = 60 * time.Second
	wak08Conns     = 8
	wak08Keys      = 2000
	wak08SlowDelay = 2 * time.Second
)

// wak08Writer owns the keys k with k % conns == worker, so it knows each
// key's state and ver without reading: a missing key is inserted with the
// next ver, an existing one updated (ver + 1) or, one time in five, deleted.
type wak08Writer struct {
	exists map[int64]bool
	ver    map[int64]int64
}

func newWak08Tx() stack.TxFunc {
	writers := make([]*wak08Writer, wak08Conns)
	for i := range writers {
		writers[i] = &wak08Writer{exists: map[int64]bool{}, ver: map[int64]int64{}}
	}
	return func(ctx context.Context, conn *pgx.Conn, worker int, rng *rand.Rand) ([]stack.Change, error) {
		w := writers[worker]
		key := int64(worker) + int64(wak08Conns)*rng.Int64N(wak08Keys/wak08Conns) + 1
		next := w.ver[key] + 1
		ch := stack.Change{Table: wak08Table, Key: stack.IDKey(key), Ver: next}
		var err error
		switch {
		case !w.exists[key]:
			ch.Op = stack.OpCreate
			_, err = conn.Exec(ctx, `INSERT INTO wak08.items (id, ver, payload)
				VALUES ($1, $2::bigint, md5($2::bigint::text))`, key, next)
		case rng.IntN(5) == 0:
			ch.Op, ch.Ver = stack.OpDelete, w.ver[key]
			_, err = conn.Exec(ctx, "DELETE FROM wak08.items WHERE id = $1", key)
		default:
			ch.Op = stack.OpUpdate
			_, err = conn.Exec(ctx, `UPDATE wak08.items SET ver = $2::bigint, payload = md5($2::bigint::text)
				WHERE id = $1`, key, next)
		}
		if err != nil {
			return nil, err //nolint:wrapcheck // reported in the load summary
		}
		w.exists[key] = ch.Op != stack.OpDelete
		if ch.Op != stack.OpDelete {
			w.ver[key] = next
		}
		return []stack.Change{ch}, nil
	}
}

// TestAcceptance_WAK08_Webhook implements WAK-08: Webhook sink with 30% retryable failures and 5% slow responses.
func TestAcceptance_WAK08_Webhook(t *testing.T) { //nolint:paralleltest // shared compose stack
	const id = "WAK-08"
	c, err := harness.Criterion(id)
	if err != nil {
		harness.Fail(t, id, "load criterion: "+err.Error())
	}
	floats := map[string]float64{}
	for _, k := range []string{"failure_pct", "slow_response_pct", "delivered_pct_min", "valid_signature_pct_min"} {
		floats[k], err = c.Float(k)
		failIf(t, id, err, "threshold "+k)
	}
	outOfOrderMax, err := c.Int("out_of_order_max")
	failIf(t, id, err, "threshold out_of_order_max")

	stack.EnsureUp(t)
	pg := stack.ConnectPG(t, 2)
	pg.Exec(t, `DROP SCHEMA IF EXISTS wak08 CASCADE;
CREATE SCHEMA wak08;
CREATE TABLE wak08.items (id bigint PRIMARY KEY, ver bigint NOT NULL, payload text);
ALTER TABLE wak08.items REPLICA IDENTITY FULL;`)

	secret := "wak08-secret"
	recv := stack.StartReceiver(t, stack.ReceiverOptions{
		Secret: []byte(secret), FailurePct: floats["failure_pct"], SlowPct: floats["slow_response_pct"],
		SlowDelay: wak08SlowDelay, Seed: wak08Seed,
	})
	wake := stack.ConfigureWake(t, stack.WakeOptions{
		ID:         id,
		Config:     wakeConfig("wak08", []string{"wak08.*"}, webhookSink(recv.URL("wak08"), "WAKE_WEBHOOK_SECRET"), ""),
		Env:        map[string]string{"WAKE_WEBHOOK_SECRET": secret},
		FreshState: true,
	})

	ctx := withTimeout(t, 10*time.Minute)
	load, err := stack.StartLoad(ctx, stack.LoadOptions{
		Rate: wak08Rate, Duration: wak08Duration, Conns: wak08Conns, Seed: wak08Seed, Tx: newWak08Tx(),
	})
	failIf(t, id, err, "start load")
	res := load.Wait()
	if res.Committed == 0 {
		harness.Failf(t, id, "no transaction committed: %s %v", res.Summary(), res.FirstErrs)
	}
	_, err = pg.WaitQuiesced(ctx, wake.Slot())
	failIf(t, id, err, "wait for Wake to confirm the final LSN (every event acknowledged by the receiver)")

	dec, err := stack.DecodePayloads(recv.Items())
	failIf(t, id, err, "decode accepted webhook payloads")
	lost, err := stack.FindLost(res.Changes, dec.Events, "ver")
	failIf(t, id, err, "M3 lost")
	order, err := stack.OrderViolations(dec.Events)
	failIf(t, id, err, "M4 order")
	st := recv.Stats()

	delivered := stack.Pct(lost.Recorded-len(lost.Lost), lost.Recorded)
	signed := stack.Pct(st.SignatureValid, st.SignatureValid+st.SignatureFailed)
	failures := st.Faults[stack.Fault500] + st.Faults[stack.Fault503] + st.Faults[stack.Fault429] +
		st.Faults[stack.FaultReset]
	failurePct, slowPct := stack.Pct(failures, st.Requests), stack.Pct(st.Faults[stack.FaultSlow], st.Requests)
	if err := checkInjected(failurePct, slowPct, floats, st.Requests); err != nil {
		harness.Failf(t, id, "fault injection did not reach the criterion's mix: %v", err)
	}
	pass := delivered >= floats["delivered_pct_min"] && signed >= floats["valid_signature_pct_min"] &&
		int64(order.Violations) <= outOfOrderMax && st.SignatureValid > 0
	details := fmt.Sprintf("requests %d: %d accepted, faults %v (failure %.2f%%, slow %.2f%% of %s); "+
		"recorded changes %d, lost %d, duplicates %d; signatures valid %d, invalid %d %v; M4 checked %d, "+
		"violations %d %v; load %s",
		st.Requests, st.Accepted, st.Faults, failurePct, slowPct, wak08SlowDelay, lost.Recorded, len(lost.Lost),
		order.Duplicates, st.SignatureValid, st.SignatureFailed, st.SignatureErrors, order.Checked,
		order.Violations, order.Examples, res.Summary())
	if len(lost.Lost) > 0 {
		details += "; first lost: " + describeChanges(lost.Lost)
	}
	harness.Finish(t, harness.Result{
		ID: id,
		Measured: map[string]any{
			// The receiver deals the configured mix exactly per 100 requests; the
			// realized shares differ only by the partial last deck.
			"failure_pct": floats["failure_pct"], "slow_response_pct": floats["slow_response_pct"],
			"failure_pct_realized": failurePct, "slow_response_pct_realized": slowPct, "delivered_pct": delivered,
			"valid_signature_pct": signed, "out_of_order": order.Violations, "requests": st.Requests,
			"recorded_changes": lost.Recorded, "lost": len(lost.Lost), "duplicates": order.Duplicates,
		},
		Pass:    pass,
		Details: details,
	})
}

// checkInjected verifies the realized fault mix: the receiver deals it from
// 100-request decks, so a partial last deck can shift it by at most the
// deck's share of the run.
func checkInjected(failurePct, slowPct float64, th map[string]float64, requests int) error {
	if requests < 100 {
		return fmt.Errorf("only %d requests", requests)
	}
	slack := 100 * 100 / float64(requests) // one deck's weight in percent
	var errs []error
	if failurePct+slack < th["failure_pct"] {
		errs = append(errs, fmt.Errorf("failure %.2f%% < %.0f%%", failurePct, th["failure_pct"]))
	}
	if slowPct+slack < th["slow_response_pct"] {
		errs = append(errs, fmt.Errorf("slow %.2f%% < %.0f%%", slowPct, th["slow_response_pct"]))
	}
	return errors.Join(errs...)
}

// describeChanges describes the first five changes.
func describeChanges(cs []stack.Change) string {
	const n = 5
	parts := make([]string, 0, n)
	for i, c := range cs {
		if i == n {
			break
		}
		parts = append(parts, fmt.Sprintf("%s %s %s ver %d at %s", c.Table, c.Op, c.Key, c.Ver,
			c.Commit.UTC().Format(time.RFC3339Nano)))
	}
	return strings.Join(parts, ", ")
}
