// SPDX-License-Identifier: Apache-2.0

package fakestate_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakestate"
)

func TestStore_LoadSave(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := fakestate.New()
	_, err := st.Load(ctx)
	require.ErrorIs(t, err, pipeline.ErrStateNotFound)
	_, ok := st.State()
	assert.False(t, ok)

	s := pipeline.NewState("wake_app", "1")
	s.Snapshot.Queue = []string{"public.a"}
	require.NoError(t, st.Save(ctx, s))
	s.Snapshot.Queue[0] = "changed"
	got, err := st.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"public.a"}, got.Snapshot.Queue, "stored a copy")
	got.Snapshot.Queue[0] = "changed"
	held, ok := st.State()
	require.True(t, ok)
	assert.Equal(t, []string{"public.a"}, held.Snapshot.Queue, "returned a copy")
	assert.Equal(t, 1, st.Saves())
}

func TestStore_Failures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := fakestate.NewWith(pipeline.NewState("wake_app", "1"))
	st.FailLoad(assert.AnError)
	_, err := st.Load(ctx)
	require.ErrorIs(t, err, assert.AnError)
	st.FailLoad(nil)
	got, err := st.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, "wake_app", got.SlotName)

	st.FailSave(assert.AnError)
	require.ErrorIs(t, st.Save(ctx, pipeline.State{}), assert.AnError)
	assert.Zero(t, st.Saves())
}
