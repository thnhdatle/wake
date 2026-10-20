// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	stSlot   = "wake_orders"
	stSystem = "7000000000000000001"
)

// stFacts returns facts of a healthy resume: the slot exists and is idle, the
// state file is ours and has a consistent point, mode initial. Options adjust
// them.
func stFacts(opts ...func(*Facts)) Facts {
	st := pipeline.NewState(stSlot, stSystem)
	st.ConsistentPoint = lsn.LSN(0x100)
	f := Facts{
		SlotName: stSlot, SystemID: stSystem,
		Slot:       pipeline.SlotStatus{Exists: true, ConfirmedFlush: lsn.LSN(0x200), WALStatus: "reserved"},
		StateFound: true, State: st, Mode: ModeInitial,
	}
	for _, o := range opts {
		o(&f)
	}
	return f
}

func stNoSlot(f *Facts)      { f.Slot = pipeline.SlotStatus{} }
func stNoState(f *Facts)     { f.StateFound, f.State = false, pipeline.State{} }
func stReinit(f *Facts)      { f.Reinit = true }
func stModeNever(f *Facts)   { f.Mode = ModeNever }
func stNoPoint(f *Facts)     { f.State.ConsistentPoint = lsn.InvalidLSN }
func stOtherSlot(f *Facts)   { f.State.SlotName = "wake_other" }
func stOtherSystem(f *Facts) { f.State.SystemID = "7000000000000000002" }
func stLost(f *Facts)        { f.Slot.WALStatus = "lost" }
func stActive(f *Facts)      { f.Slot.ActivePID = 4242 }
func TestDecide_Table(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		facts       Facts
		wantErr     error
		wantAction  Action
		wantReinit  bool
		wantFresh   bool
		wantInitial bool
	}{
		// slot lost / active: errors regardless of the rest.
		{name: "lost slot with state", facts: stFacts(stLost), wantErr: ErrSlotInvalidated},
		{name: "lost slot without state", facts: stFacts(stLost, stNoState), wantErr: ErrSlotInvalidated},
		{name: "lost slot with reinit", facts: stFacts(stLost, stReinit), wantErr: ErrSlotInvalidated},
		{name: "lost slot other system", facts: stFacts(stLost, stOtherSystem), wantErr: ErrSlotInvalidated},
		{name: "active slot with state", facts: stFacts(stActive), wantErr: ErrSlotInUse},
		{name: "active slot without state", facts: stFacts(stActive, stNoState), wantErr: ErrSlotInUse},
		{name: "active slot with reinit", facts: stFacts(stActive, stReinit), wantErr: ErrSlotInUse},
		{name: "active slot other slot state", facts: stFacts(stActive, stOtherSlot), wantErr: ErrSlotInUse},
		{name: "lost wins over active", facts: stFacts(stLost, stActive), wantErr: ErrSlotInvalidated},

		// absent slot, absent state: first start.
		{
			name: "first start initial", facts: stFacts(stNoSlot, stNoState),
			wantAction: ActionCreateSlot, wantFresh: true, wantInitial: true,
		},
		{
			name: "first start never", facts: stFacts(stNoSlot, stNoState, stModeNever),
			wantAction: ActionCreateSlot, wantFresh: true,
		},
		{
			name: "first start initial reinit", facts: stFacts(stNoSlot, stNoState, stReinit),
			wantAction: ActionCreateSlot, wantFresh: true, wantInitial: true, wantReinit: true,
		},
		{
			name: "first start never reinit", facts: stFacts(stNoSlot, stNoState, stModeNever, stReinit),
			wantAction: ActionCreateSlot, wantFresh: true, wantReinit: true,
		},

		// present slot, absent state.
		{name: "slot without state", facts: stFacts(stNoState), wantErr: pipeline.ErrStateMismatch},
		{
			name: "slot without state reinit", facts: stFacts(stNoState, stReinit),
			wantAction: ActionResume, wantReinit: true, wantFresh: true,
		},
		{
			name: "slot without state reinit never", facts: stFacts(stNoState, stReinit, stModeNever),
			wantAction: ActionResume, wantReinit: true, wantFresh: true,
		},

		// present slot, state of another slot or system.
		{name: "state of another slot", facts: stFacts(stOtherSlot), wantErr: pipeline.ErrStateMismatch},
		{name: "state of another system", facts: stFacts(stOtherSystem), wantErr: pipeline.ErrStateMismatch},
		{
			name: "state of another slot and system", facts: stFacts(stOtherSlot, stOtherSystem),
			wantErr: pipeline.ErrStateMismatch,
		},
		{
			name: "state of another slot reinit", facts: stFacts(stOtherSlot, stReinit),
			wantAction: ActionResume, wantReinit: true, wantFresh: true,
		},
		{
			name: "state of another system reinit", facts: stFacts(stOtherSystem, stReinit),
			wantAction: ActionResume, wantReinit: true, wantFresh: true,
		},

		// present slot, our state without a consistent point.
		{
			name: "recreate initial", facts: stFacts(stNoPoint),
			wantAction: ActionRecreateSlot, wantFresh: true, wantInitial: true,
		},
		{
			name: "recreate never", facts: stFacts(stNoPoint, stModeNever),
			wantAction: ActionRecreateSlot, wantFresh: true,
		},
		{
			name: "recreate initial reinit", facts: stFacts(stNoPoint, stReinit),
			wantAction: ActionRecreateSlot, wantFresh: true, wantInitial: true, wantReinit: true,
		},
		{
			name: "recreate never reinit", facts: stFacts(stNoPoint, stModeNever, stReinit),
			wantAction: ActionRecreateSlot, wantFresh: true, wantReinit: true,
		},

		// present slot, our state with a consistent point.
		{name: "resume", facts: stFacts(), wantAction: ActionResume},
		{name: "resume never", facts: stFacts(stModeNever), wantAction: ActionResume},
		{
			name: "resume reinit", facts: stFacts(stReinit),
			wantAction: ActionResume, wantReinit: true, wantFresh: true,
		},

		// absent slot, our state without a consistent point.
		{
			name: "slot absent state without point", facts: stFacts(stNoSlot, stNoPoint),
			wantAction: ActionCreateSlot, wantFresh: true, wantInitial: true,
		},
		{
			name: "slot absent state without point never", facts: stFacts(stNoSlot, stNoPoint, stModeNever),
			wantAction: ActionCreateSlot, wantFresh: true,
		},
		{
			name: "slot absent state without point reinit", facts: stFacts(stNoSlot, stNoPoint, stReinit),
			wantAction: ActionCreateSlot, wantFresh: true, wantInitial: true, wantReinit: true,
		},

		// absent slot, state with a consistent point or of another source.
		{name: "slot missing", facts: stFacts(stNoSlot), wantErr: ErrSlotMissing},
		{name: "slot missing other slot", facts: stFacts(stNoSlot, stOtherSlot), wantErr: ErrSlotMissing},
		{name: "slot missing other system", facts: stFacts(stNoSlot, stOtherSystem), wantErr: ErrSlotMissing},
		{
			name:  "slot missing other system without point",
			facts: stFacts(stNoSlot, stOtherSystem, stNoPoint), wantErr: ErrSlotMissing,
		},
		{
			name: "slot missing reinit", facts: stFacts(stNoSlot, stReinit),
			wantAction: ActionCreateSlot, wantFresh: true, wantInitial: true, wantReinit: true,
		},
		{
			name: "slot missing other system reinit", facts: stFacts(stNoSlot, stOtherSystem, stReinit),
			wantAction: ActionCreateSlot, wantFresh: true, wantInitial: true, wantReinit: true,
		},
		{
			name: "slot missing reinit never", facts: stFacts(stNoSlot, stReinit, stModeNever),
			wantAction: ActionCreateSlot, wantFresh: true, wantReinit: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, err := Decide(&tc.facts)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Equal(t, Decision{}, d)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantAction, d.Action)
			assert.Equal(t, tc.wantReinit, d.Reinit, "reinit")
			assert.Equal(t, tc.wantFresh, d.FreshState, "fresh state")
			assert.Equal(t, tc.wantInitial, d.Initial, "initial")
			assert.NotEmpty(t, d.Reason)
		})
	}
}

func TestDecide_StateMismatchIsNotSlotMissing(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]Facts{
		"missing state":   stFacts(stNoState),
		"other slot":      stFacts(stOtherSlot),
		"other system":    stFacts(stOtherSystem),
		"state w/o point": stFacts(stOtherSystem, stNoPoint),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := Decide(&f)
			require.ErrorIs(t, err, pipeline.ErrStateMismatch)
			require.NotErrorIs(t, err, ErrSlotMissing)
			assert.Contains(t, err.Error(), "--reinit-state")
		})
	}
}

func TestDecide_ModeSetsInitialOnlyOnCreate(t *testing.T) {
	t.Parallel()
	creating := map[string][]func(*Facts){
		"create":   {stNoSlot, stNoState},
		"recreate": {stNoPoint},
	}
	for name, opts := range creating {
		for _, mode := range []Mode{ModeInitial, ModeNever} {
			t.Run(name+"/"+string(mode), func(t *testing.T) {
				t.Parallel()
				f := stFacts(opts...)
				f.Mode = mode
				d, err := Decide(&f)
				require.NoError(t, err)
				assert.Equal(t, mode == ModeInitial, d.Initial)
			})
		}
	}
	t.Run("resume never sets initial", func(t *testing.T) {
		t.Parallel()
		f := stFacts()
		d, err := Decide(&f)
		require.NoError(t, err)
		assert.False(t, d.Initial)
	})
}

func TestDecide_ErrorMessagesNameTheSlot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		facts Facts
		want  string
	}{
		{name: "invalidated", facts: stFacts(stLost), want: stSlot},
		{name: "in use", facts: stFacts(stActive), want: "4242"},
		{name: "missing", facts: stFacts(stNoSlot), want: stSlot},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := Decide(&tc.facts)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestAction_String(t *testing.T) {
	t.Parallel()
	tests := []struct {
		action Action
		want   string
	}{
		{ActionResume, "resume"},
		{ActionCreateSlot, "create_slot"},
		{ActionRecreateSlot, "recreate_slot"},
		{Action(0), "action(0)"},
		{Action(99), "action(99)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.action.String())
		})
	}
}
