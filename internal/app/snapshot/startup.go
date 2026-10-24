// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

type Mode string

const (
	ModeInitial Mode = "initial"
	ModeNever   Mode = "never"
)

const walStatusLost = "lost"

type Action uint8

const (
	ActionResume Action = iota + 1
	ActionCreateSlot
	ActionRecreateSlot
)

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

type Facts struct {
	SlotName   string
	SystemID   string
	Slot       pipeline.SlotStatus
	StateFound bool
	State      pipeline.State
	Reinit     bool
	Mode       Mode
}

type Decision struct {
	Action     Action
	Reinit     bool
	FreshState bool
	Initial    bool
	Reason     string
}

// Decide chooses the startup action from the slot, the state file and the reinit flag.
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
