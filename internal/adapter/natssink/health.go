// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// fault is one current error condition of the sink.
type fault struct {
	v      verdict
	reason string
}

// faultSlot names the independent error conditions folded into Health.
type faultSlot int

const (
	// faultBlock: the dispatcher cannot publish the next event (event too
	// large, stream missing or not creatable, not connected yet).
	faultBlock faultSlot = iota
	// faultWrite: a publish failed; the event waits for re-publication.
	faultWrite
	// faultConn: the connection is down or rejected (watchdog).
	faultConn
	faultSlots
)

// healthTracker folds the fault slots into one SinkHealth: the worst state
// wins (fatal over retrying), and Since changes only when the state does.
// Callers hold Sink.mu.
type healthTracker struct {
	slots   [faultSlots]*fault
	current pipeline.SinkHealth
}

// transition is a state change to log after Sink.mu is released.
type transition struct {
	changed bool
	health  pipeline.SinkHealth
}

func (h *healthTracker) set(slot faultSlot, f *fault, now time.Time) transition {
	h.slots[slot] = f
	return h.recompute(now)
}

// clear removes the faults of slots.
func (h *healthTracker) clear(now time.Time, slots ...faultSlot) transition {
	dirty := false
	for _, slot := range slots {
		dirty = dirty || h.slots[slot] != nil
		h.slots[slot] = nil
	}
	if !dirty {
		return transition{}
	}
	return h.recompute(now)
}

func (h *healthTracker) recompute(now time.Time) transition {
	next := pipeline.SinkHealth{State: pipeline.SinkStateHealthy}
	for _, f := range h.slots {
		if f == nil {
			continue
		}
		st := pipeline.SinkStateRetrying
		if f.v.fatal {
			st = pipeline.SinkStateFatal
		}
		if st > next.State {
			next = pipeline.SinkHealth{State: st, Reason: f.reason}
			if f.v.fatal {
				next.Code = f.v.code
			}
		}
	}
	changed := next.State != h.current.State
	next.Since = h.current.Since
	if changed || next.Since.IsZero() {
		next.Since = now
	}
	h.current = next
	return transition{changed: changed, health: next}
}
