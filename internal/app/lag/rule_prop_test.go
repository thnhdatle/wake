// SPDX-License-Identifier: Apache-2.0

package lag

import (
	"context"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakealert"
)

// TestRule_Evaluate_NotifiesOncePerStateChange checks the invariant
// "POST once per state change" for random value sequences and notifier
// failures: the rule fires iff the value of the last evaluation is above the
// threshold; the notifier never receives two consecutive notifications with
// the same state, the first is a firing one, and each matches its value;
// after a successful delivery the notified state equals the current state,
// so no state change is lost.
func TestRule_Evaluate_NotifiesOncePerStateChange(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		const threshold = 10
		r := newRule("r", threshold)
		n := fakealert.New()
		now := epoch
		steps := rapid.IntRange(1, 60).Draw(t, "steps")
		changes := 0
		for i := range steps {
			value := float64(rapid.IntRange(0, 2*threshold).Draw(t, "value"))
			fail := rapid.Bool().Draw(t, "fail")
			now = now.Add(time.Second)
			if r.evaluate(value, now) {
				changes++
			}
			if r.active != (value > threshold) {
				t.Fatalf("step %d: active %v for value %v", i, r.active, value)
			}
			if fail {
				n.Fail(injectedError{})
			} else {
				n.Fail(nil)
			}
			err := r.deliver(context.Background(), n)
			if !fail && (err != nil || r.pending != nil || r.delivered != r.state()) {
				t.Fatalf("step %d: delivered %s, state %s, err %v", i, r.delivered, r.state(), err)
			}
		}
		checkAlerts(t, n.Alerts(), threshold, changes)
	})
}

func checkAlerts(t *rapid.T, alerts []pipeline.Alert, threshold float64, changes int) {
	if len(alerts) > changes {
		t.Fatalf("%d notifications for %d state changes", len(alerts), changes)
	}
	prev := pipeline.AlertStateResolved
	for i, a := range alerts {
		if a.State == prev {
			t.Fatalf("notification %d repeats state %s", i, a.State)
		}
		if (a.State == pipeline.AlertStateFiring) != (a.Value > threshold) || a.Threshold != threshold {
			t.Fatalf("notification %d: %+v", i, a)
		}
		prev = a.State
	}
}

// injectedError is an injected notifier error.
type injectedError struct{}

func (injectedError) Error() string { return "injected" }
