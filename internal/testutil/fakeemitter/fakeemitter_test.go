// SPDX-License-Identifier: Apache-2.0

package fakeemitter_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/testutil/fakeemitter"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestEmitter_EmitMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e := fakeemitter.New(0x1000)
	content := []byte(`{"ts":1}`)
	at, err := e.EmitMessage(ctx, "wake.heartbeat", content)
	require.NoError(t, err)
	assert.Equal(t, lsn.LSN(0x1000+fakeemitter.Step), at)
	content[0] = 'x'

	e.Fail(assert.AnError)
	_, err = e.EmitMessage(ctx, "wake.heartbeat", nil)
	require.ErrorIs(t, err, assert.AnError)
	e.Fail(nil)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = e.EmitMessage(canceled, "wake.heartbeat", nil)
	require.ErrorIs(t, err, context.Canceled)

	at, err = e.EmitMessage(ctx, "wake.watermark", []byte("w"))
	require.NoError(t, err)
	assert.Equal(t, lsn.LSN(0x1000+2*fakeemitter.Step), at)
	assert.Equal(t, 4, e.Attempts())
	assert.Equal(t, []fakeemitter.Message{
		{Prefix: "wake.heartbeat", Content: []byte(`{"ts":1}`), LSN: 0x1100},
		{Prefix: "wake.watermark", Content: []byte("w"), LSN: 0x1200},
	}, e.Messages())
}

func TestEmitter_WaitAttempts(t *testing.T) {
	t.Parallel()
	e := fakeemitter.New(0)
	done := make(chan error, 1)
	go func() {
		_, err := e.WaitAttempts(context.Background(), 2)
		done <- err
	}()
	e.Fail(assert.AnError)
	for range 2 {
		_, err := e.EmitMessage(context.Background(), "p", nil)
		require.ErrorIs(t, err, assert.AnError)
	}
	require.NoError(t, <-done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := e.WaitAttempts(ctx, 3)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, n)
}
