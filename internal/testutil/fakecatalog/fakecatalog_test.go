// SPDX-License-Identifier: Apache-2.0

package fakecatalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakecatalog"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestCatalog_Slot_QueuedThenFixed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := fakecatalog.New()
	st, err := c.Slot(ctx, "wake_a")
	require.NoError(t, err)
	assert.False(t, st.Exists, "missing slot by default")

	fixed := pipeline.SlotStatus{Exists: true, LagBytes: 42, WALStatus: "reserved"}
	c.SetSlot(fixed, nil)
	c.QueueSlot(
		fakecatalog.SlotResult{Err: assert.AnError},
		fakecatalog.SlotResult{Status: pipeline.SlotStatus{LagBytes: 1}},
	)
	_, err = c.Slot(ctx, "wake_a")
	require.ErrorIs(t, err, assert.AnError)
	st, err = c.Slot(ctx, "wake_a")
	require.NoError(t, err)
	assert.Equal(t, int64(1), st.LagBytes)
	st, err = c.Slot(ctx, "wake_b")
	require.NoError(t, err)
	assert.Equal(t, fixed, st)
	assert.Equal(t, []string{"wake_a", "wake_a", "wake_a", "wake_b"}, c.SlotCalls())
}

func TestCatalog_WaitSlotCalls(t *testing.T) {
	t.Parallel()
	c := fakecatalog.New()
	done := make(chan error, 1)
	go func() {
		_, err := c.WaitSlotCalls(context.Background(), 2)
		done <- err
	}()
	for range 2 {
		_, err := c.Slot(context.Background(), "wake_a")
		require.NoError(t, err)
	}
	require.NoError(t, <-done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := c.WaitSlotCalls(ctx, 3)
	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, got, 2)
}

func TestCatalog_CanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := fakecatalog.New()
	_, err := c.ServerSettings(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.Slot(ctx, "wake_a")
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.PublicationTables(ctx, []string{"p"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.CurrentTime(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, c.SlotCalls())
	assert.Empty(t, c.PublicationCalls())
}

func TestCatalog_OtherAnswers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := fakecatalog.New()
	settings := pipeline.ServerSettings{ServerVersionNum: 170002, WALLevel: "logical", WALSenderTimeout: time.Minute}
	c.SetServerSettings(settings, nil)
	got, err := c.ServerSettings(ctx)
	require.NoError(t, err)
	assert.Equal(t, settings, got)

	tables := []pipeline.TableInfo{{Schema: "public", Table: "t", OID: 16384, HasKey: true}}
	c.SetPublicationTables(tables, nil)
	gotTables, err := c.PublicationTables(ctx, []string{"wake_app"})
	require.NoError(t, err)
	assert.Equal(t, tables, gotTables)
	c.SetPublicationTables(nil, assert.AnError)
	_, err = c.PublicationTables(ctx, []string{"other"})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, [][]string{{"wake_app"}, {"other"}}, c.PublicationCalls())

	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	c.SetCurrentTime(now, nil)
	gotNow, err := c.CurrentTime(ctx)
	require.NoError(t, err)
	assert.Equal(t, now, gotNow)
}
