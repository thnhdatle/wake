// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Alert rule names: the {rule} label of wake_alert_active and the "rule"
// field of alert notifications.
const (
	// RuleSlotLagWarn fires while the slot lag exceeds lag.warn_bytes.
	RuleSlotLagWarn = "slot_lag_warn"
	// RuleSlotLagCritical fires while the slot lag exceeds lag.critical_bytes.
	RuleSlotLagCritical = "slot_lag_critical"
	// RuleMaxAckAge fires while the oldest unacknowledged delivery is older
	// than lag.max_ack_age; value and threshold are in seconds.
	RuleMaxAckAge = "max_ack_age"
	// RuleSinkFatal fires while any sink is blocked by a non-retriable
	// error; value is the number of such sinks, threshold 0.
	RuleSinkFatal = "sink_fatal"
)

// rule is the state machine of one alert rule. It fires while the value of
// the last evaluation is above the threshold and notifies each state change
// once: a change whose delivery fails stays pending and is retried on the
// next delivery; only the latest pending change is kept, and a pending change
// back to the last delivered state is dropped, so the notifier never sees two
// consecutive notifications with the same state. The first notification of
// a rule is always a firing one. Not safe for concurrent use.
type rule struct {
	name      string
	threshold float64
	active    bool                // firing at the last evaluation
	delivered pipeline.AlertState // last state notified
	pending   *pipeline.Alert     // latest undelivered state change
	attempts  int                 // failed deliveries of pending
}

func newRule(name string, threshold float64) *rule {
	return &rule{name: name, threshold: threshold, delivered: pipeline.AlertStateResolved}
}

func (r *rule) state() pipeline.AlertState {
	if r.active {
		return pipeline.AlertStateFiring
	}
	return pipeline.AlertStateResolved
}

// evaluate applies the value observed at now and reports whether the rule
// changed state.
func (r *rule) evaluate(value float64, now time.Time) bool {
	firing := value > r.threshold
	if firing == r.active {
		return false
	}
	r.active = firing
	r.attempts = 0
	st := r.state()
	if st == r.delivered {
		r.pending = nil
		return true
	}
	r.pending = &pipeline.Alert{Rule: r.name, State: st, Value: value, Threshold: r.threshold, Ts: now}
	return true
}

// deliver notifies the pending state change, if any. On failure the change
// stays pending and the error is returned.
func (r *rule) deliver(ctx context.Context, n pipeline.AlertNotifier) error {
	if r.pending == nil {
		return nil
	}
	if err := n.Notify(ctx, *r.pending); err != nil {
		r.attempts++
		return err
	}
	r.delivered = r.pending.State
	r.pending = nil
	r.attempts = 0
	return nil
}
