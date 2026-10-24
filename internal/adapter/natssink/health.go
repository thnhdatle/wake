// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

type fault struct {
	v      verdict
	reason string
}

type faultSlot int

const (
	faultBlock faultSlot = iota
	faultWrite
	faultConn
	faultSlots
)

type healthTracker struct {
	slots   [faultSlots]*fault
	current pipeline.SinkHealth
}

type transition struct {
	changed bool
	health  pipeline.SinkHealth
}

func (h *healthTracker) set(slot faultSlot, f *fault, now time.Time) transition {
	h.slots[slot] = f
	return h.recompute(now)
}

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
