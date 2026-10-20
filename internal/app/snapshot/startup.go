// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Mode is snapshot.mode: whether the initial snapshot runs when the slot is
// created.
type Mode string

// Snapshot modes.
const (
	ModeInitial Mode = "initial"
	ModeNever   Mode = "never"
)

// walStatusLost is pg_replication_slots.wal_status of an invalidated slot.
const walStatusLost = "lost"

// Action is what startup does with the slot.
type Action uint8

// Startup actions.
const (
	// ActionResume keeps the slot and streams from its confirmed LSN.
	ActionResume Action = iota + 1
	// ActionCreateSlot creates the slot as on first start.
	ActionCreateSlot
	// ActionRecreateSlot drops the slot, then creates it as on first start:
	// the state file has no consistent point, so nothing was emitted.
	ActionRecreateSlot
)

// String returns the action's name for logs.
func (a Action) String() string {
	switch a {
	case ActionResume:
		return "resume"
	case ActionCreateSlot:
		return "create_slot"
	case ActionRecreateSlot:
		return "recreate_slot"
	default:
		return fmt.Sprintf("action(%d)", uint8(a))
	}
}

// Facts are the inputs of the startup decision.
type Facts struct {
	// SlotName and SystemID identify this source (IDENTIFY_SYSTEM).
	SlotName string
	SystemID string
	// Slot is the slot's row of pg_replication_slots.
	Slot pipeline.SlotStatus
	// StateFound reports whether the state file exists; State is its content.
	StateFound bool
	State      pipeline.State
	// Reinit is `wake run --reinit-state`.
	Reinit bool
	// Mode is snapshot.mode.
	Mode Mode
}

// Decision is the outcome of Decide.
type Decision struct {
	Action Action
	// Reinit applies --reinit-state: schema versions are reseeded with
	// Registry.Reinit(now) and snapshot progress is discarded (unfinished
	// runs are logged).
	Reinit bool
	// FreshState starts from a new state owned by this slot and system,
	// keeping only the schema versions and run counter of the old file.
	FreshState bool
	// Initial runs the initial snapshot after creating the slot.
	Initial bool
	// Reason explains the decision for the startup log.
	Reason string
}

// Decide is the startup decision table:
//
//	slot          state file                 --reinit-state  decision
//	lost          any                        any             error slot_invalidated
//	active        any                        any             error slot_in_use
//	absent        absent                     any             create slot (initial snapshot per mode)
//	present       absent                     no              error state_mismatch
//	present       absent                     yes             resume from the slot, fresh state, reinit
//	present       other slot or system       no              error state_mismatch
//	present       other slot or system       yes             resume from the slot, fresh state, reinit
//	present       ours, no consistent point  any             drop and recreate the slot (nothing emitted)
//	present       ours, consistent point     no              resume (unfinished runs via watermarks)
//	present       ours, consistent point     yes             resume, reinit (progress discarded)
//	absent        ours, no consistent point  any             create slot as on first start
//	absent        ours with consistent point no              error slot_missing
//	              or another slot/system
//	absent        (same)                     yes             create slot as on first start, reinit
//
// Every create and recreate starts a fresh state (keeping schema versions
// and the run counter) and runs the initial snapshot when the mode is
// initial.
func Decide(f *Facts) (Decision, error) {
	if err := checkSlot(f); err != nil {
		return Decision{}, err
	}
	var ownErr error
	if f.StateFound {
		ownErr = f.State.CheckOwnership(f.SlotName, f.SystemID)
	}
	owned := f.StateFound && ownErr == nil
	if f.Slot.Exists {
		return decideWithSlot(f, owned, ownErr)
	}
	create := Decision{Action: ActionCreateSlot, FreshState: true, Initial: f.Mode == ModeInitial, Reinit: f.Reinit}
	switch {
	case !f.StateFound:
		create.Reason = "first start: no slot and no state file"
	case owned && !f.State.HasConsistentPoint():
		create.Reason = "state file has no consistent point and the slot is absent"
	case !f.Reinit:
		return Decision{}, fmt.Errorf("%w: slot %q; pass --reinit-state to create it (changes since its loss are lost)",
			ErrSlotMissing, f.SlotName)
	default:
		create.Reason = "slot missing; recreated with --reinit-state"
	}
	return create, nil
}

// checkSlot rejects an invalidated slot and a slot in use.
func checkSlot(f *Facts) error {
	switch {
	case f.Slot.Exists && f.Slot.WALStatus == walStatusLost:
		return fmt.Errorf("%w: slot %q; drop it with `wake slot drop` and restart",
			ErrSlotInvalidated, f.SlotName)
	case f.Slot.Exists && f.Slot.ActivePID != 0:
		return fmt.Errorf("%w: slot %q is used by pid %d", ErrSlotInUse, f.SlotName, f.Slot.ActivePID)
	default:
		return nil
	}
}

// decideWithSlot covers the rows of the table where the slot exists.
func decideWithSlot(f *Facts, owned bool, ownErr error) (Decision, error) {
	switch {
	case !owned && !f.Reinit:
		if ownErr == nil {
			ownErr = fmt.Errorf("%w: state file missing", pipeline.ErrStateMismatch)
		}
		return Decision{}, fmt.Errorf("%w; pass --reinit-state to adopt slot %q", ownErr, f.SlotName)
	case !owned:
		return Decision{
			Action: ActionResume, Reinit: true, FreshState: true,
			Reason: "slot exists without a matching state file; state reinitialised",
		}, nil
	case !f.State.HasConsistentPoint():
		return Decision{
			Action: ActionRecreateSlot, FreshState: true, Initial: f.Mode == ModeInitial, Reinit: f.Reinit,
			Reason: "state file has no consistent point: slot creation never completed",
		}, nil
	default:
		return Decision{
			Action: ActionResume, Reinit: f.Reinit, FreshState: f.Reinit,
			Reason: "resume from the slot's confirmed LSN",
		}, nil
	}
}
