// SPDX-License-Identifier: Apache-2.0

package fakerepl_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakerepl"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSource_SlotCommands(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := fakerepl.New(0)
	s.SetSystem(pipeline.SystemInfo{SystemID: "7", DBName: "app"})
	info, err := s.IdentifySystem(ctx)
	require.NoError(t, err)
	assert.Equal(t, "7", info.SystemID)

	s.SetSlot(pipeline.SlotInfo{ConsistentPoint: 0x100, SnapshotName: "00000003-1"}, nil)
	slot, err := s.CreateSlot(ctx, "wake_app")
	require.NoError(t, err)
	assert.Equal(t, pipeline.SlotInfo{Name: "wake_app", ConsistentPoint: 0x100, SnapshotName: "00000003-1"}, slot)
	s.SetSlot(pipeline.SlotInfo{}, assert.AnError)
	_, err = s.CreateSlot(ctx, "wake_b")
	require.ErrorIs(t, err, assert.AnError)
	require.NoError(t, s.DropSlot(ctx, "wake_app"))
	created, dropped := s.Slots()
	assert.Equal(t, []string{"wake_app"}, created)
	assert.Equal(t, []string{"wake_app"}, dropped)

	opts := pipeline.StartOptions{Slot: "wake_app", Start: 0x100, ProtoVersion: 1, Messages: true}
	require.NoError(t, s.StartReplication(ctx, opts))
	assert.Equal(t, []pipeline.StartOptions{opts}, s.Started())
}

func TestSource_Receive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := fakerepl.New(2)
	ka := pipeline.ReplicationMessage{
		Kind:      pipeline.ReplicationMessageKeepalive,
		Keepalive: pipeline.Keepalive{ServerWALEnd: 9, ReplyRequested: true},
	}
	x := pipeline.ReplicationMessage{
		Kind:     pipeline.ReplicationMessageXLogData,
		XLogData: pipeline.XLogData{WALStart: 5, Data: []byte("B")},
	}
	require.NoError(t, s.Push(ctx, x))
	require.NoError(t, s.Push(ctx, ka))
	dead, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, s.Push(dead, x), context.Canceled, "queue full")

	s.Close()
	s.Close()
	got, err := s.Receive(ctx)
	require.NoError(t, err)
	assert.Equal(t, x, got)
	got, err = s.Receive(ctx)
	require.NoError(t, err)
	assert.Equal(t, ka, got)
	_, err = s.Receive(ctx)
	require.ErrorIs(t, err, fakerepl.ErrClosed)
}

func TestSource_Receive_BlocksUntilPushOrCancel(t *testing.T) {
	t.Parallel()
	s := fakerepl.New(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Receive(ctx)
	require.ErrorIs(t, err, context.Canceled)

	got := make(chan pipeline.ReplicationMessage, 1)
	go func() {
		m, _ := s.Receive(context.Background())
		got <- m
	}()
	msg := pipeline.ReplicationMessage{Kind: pipeline.ReplicationMessageKeepalive}
	require.NoError(t, s.Push(context.Background(), msg))
	assert.Equal(t, msg, <-got)
}

func TestSource_SendStandbyStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := fakerepl.New(1)
	st := pipeline.StandbyStatus{Written: 2, Flushed: 1}
	done := make(chan []pipeline.StandbyStatus, 1)
	go func() {
		got, _ := s.WaitStatuses(ctx, 1)
		done <- got
	}()
	require.NoError(t, s.SendStandbyStatus(ctx, st))
	assert.Equal(t, []pipeline.StandbyStatus{st}, <-done)

	s.FailStatus(assert.AnError)
	require.ErrorIs(t, s.SendStandbyStatus(ctx, st), assert.AnError)
	assert.Len(t, s.Statuses(), 1)

	dead, cancel := context.WithCancel(ctx)
	cancel()
	got, err := s.WaitStatuses(dead, 2)
	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, got, 1)
}
