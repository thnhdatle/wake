// SPDX-License-Identifier: Apache-2.0

package fakesink_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/fakesink"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type acker struct{ got []pipeline.AckToken }

func (a *acker) Ack(tok pipeline.AckToken) error {
	a.got = append(a.got, tok)
	if tok.Tx == 99 {
		return assert.AnError
	}
	return nil
}

func rec(a pipeline.Acker, n uint64) pipeline.Record {
	return pipeline.Record{ID: change.StreamID(1, n), Ack: pipeline.NewAckHandle(a, pipeline.AckToken{Tx: n})}
}

func TestSink_Send_RecordsAndAcks(t *testing.T) {
	t.Parallel()
	a := &acker{}
	s := fakesink.New("k")
	assert.Equal(t, "k", s.Name())
	assert.True(t, s.Health().IsHealthy())
	for n := range uint64(4) {
		require.NoError(t, s.Send(context.Background(), rec(a, n)))
	}
	assert.Len(t, s.Received(), 4)
	require.Len(t, s.Pending(), 4)

	require.NoError(t, s.AckAt(2))
	n, err := s.AckNext(1)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.NoError(t, s.AckAll())
	assert.Equal(t, []pipeline.AckToken{{Tx: 2}, {Tx: 0}, {Tx: 1}, {Tx: 3}}, a.got)
	assert.Empty(t, s.Pending())
	assert.Len(t, s.Received(), 4)

	n, err = s.AckNext(5)
	require.NoError(t, err)
	assert.Zero(t, n)
	require.ErrorIs(t, s.AckAt(0), fakesink.ErrNoPending)
	require.ErrorIs(t, s.AckAt(-1), fakesink.ErrNoPending)
}

func TestSink_AckNext_JoinsErrors(t *testing.T) {
	t.Parallel()
	a := &acker{}
	s := fakesink.New("k")
	require.NoError(t, s.Send(context.Background(), rec(a, 99)))
	require.NoError(t, s.Send(context.Background(), rec(a, 1)))
	_, err := s.AckNext(2)
	require.ErrorIs(t, err, assert.AnError)
	assert.Len(t, a.got, 2, "every record is acknowledged despite an error")
}

func TestSink_Send_AutoAck(t *testing.T) {
	t.Parallel()
	a := &acker{}
	s := fakesink.New("k", fakesink.WithAutoAck(), fakesink.WithCapacity(1))
	for n := range uint64(3) {
		require.NoError(t, s.Send(context.Background(), rec(a, n)))
	}
	assert.Empty(t, s.Pending())
	assert.Len(t, a.got, 3)
	require.ErrorIs(t, s.Send(context.Background(), rec(a, 99)), assert.AnError)
}

func TestSink_Send_BlocksAtCapacity(t *testing.T) {
	t.Parallel()
	s := fakesink.New("k", fakesink.WithCapacity(0))
	require.NoError(t, s.Send(context.Background(), pipeline.Record{}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.Send(ctx, pipeline.Record{}), context.Canceled)

	done := make(chan error, 1)
	changed := s.Changed()
	go func() { done <- s.Send(context.Background(), pipeline.Record{}) }()
	select {
	case <-done:
		t.Fatal("Send did not block")
	default:
	}
	_, err := s.AckNext(1)
	require.NoError(t, err)
	<-changed
	require.NoError(t, <-done)
	require.NoError(t, s.WaitReceived(context.Background(), 2))
}

func TestSink_WaitReceived(t *testing.T) {
	t.Parallel()
	s := fakesink.New("k")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.WaitReceived(ctx, 1), context.Canceled)
	done := make(chan error, 1)
	go func() { done <- s.WaitReceived(context.Background(), 1) }()
	require.NoError(t, s.Send(context.Background(), pipeline.Record{}))
	require.NoError(t, <-done)
}

func TestSink_HealthAndFailures(t *testing.T) {
	t.Parallel()
	s := fakesink.New("k")
	h := pipeline.SinkHealth{State: pipeline.SinkStateFatal, Code: pipeline.CodeEventTooLarge}
	s.SetHealth(h)
	assert.Equal(t, h, s.Health())
	s.FailSends(assert.AnError)
	require.ErrorIs(t, s.Send(context.Background(), pipeline.Record{}), assert.AnError)
	s.FailSends(nil)
	require.NoError(t, s.Send(context.Background(), pipeline.Record{}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, s.Run(ctx))
}
