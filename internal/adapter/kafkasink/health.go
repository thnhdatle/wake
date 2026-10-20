// SPDX-License-Identifier: Apache-2.0

package kafkasink

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
	// faultBlock: the dispatcher cannot hand the next record to Kafka
	// (event too large, topic missing or unreadable, record above the
	// topic's limit).
	faultBlock faultSlot = iota
	// faultWrite: Kafka failed a record; it waits for re-production.
	faultWrite
	// faultStall: records are in flight but Kafka acknowledged nothing for
	// the stall timeout (franz-go retries internally, e.g. broker down).
	faultStall
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
