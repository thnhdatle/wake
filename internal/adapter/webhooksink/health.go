// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

type healthTracker struct {
	slots          []pipeline.SinkHealth
	tooLarge       int
	tooLargeReason string
	current        pipeline.SinkHealth
}

func newHealthTracker(lanes int) healthTracker {
	return healthTracker{slots: make([]pipeline.SinkHealth, max(lanes, 0)+1)}
}

func slotOf(lane int) int {
	if lane == pipeline.BarrierLane {
		return 0
	}
	return lane + 1
}

func (h *healthTracker) set(lane int, st pipeline.SinkState, code, reason string, now time.Time) bool {
	h.slots[slotOf(lane)] = pipeline.SinkHealth{State: st, Code: code, Reason: reason}
	return h.recompute(now)
}

func (h *healthTracker) addTooLarge(reason string, now time.Time) {
	h.tooLarge++
	h.tooLargeReason = reason
	h.recompute(now)
}

func (h *healthTracker) removeTooLarge(now time.Time) {
	h.tooLarge--
	h.recompute(now)
}

func (h *healthTracker) recompute(now time.Time) bool {
	worst := pipeline.SinkHealth{State: pipeline.SinkStateHealthy}
	if h.tooLarge > 0 {
		worst = pipeline.SinkHealth{
			State: pipeline.SinkStateFatal, Code: pipeline.CodeEventTooLarge, Reason: h.tooLargeReason,
		}
	}
	for _, sl := range h.slots {
		if sl.State > worst.State {
			worst = sl
		}
	}
	changed := worst.State != h.current.State
	worst.Since = h.current.Since
	if changed || h.current.Since.IsZero() {
		worst.Since = now
	}
	h.current = worst
	return changed
}
