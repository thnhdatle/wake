// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"context"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	RuleSlotLagWarn     = "slot_lag_warn"
	RuleSlotLagCritical = "slot_lag_critical"
	RuleMaxAckAge       = "max_ack_age"
	RuleSinkFatal       = "sink_fatal"
)

type rule struct {
	name      string
	threshold float64
	active    bool
	delivered pipeline.AlertState
	pending   *pipeline.Alert
	attempts  int
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
