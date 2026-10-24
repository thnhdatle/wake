// SPDX-License-Identifier: Apache-2.0

package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/platform/clock"
)

var epoch = time.Unix(1_760_000_000, 0)

func fired(ch <-chan time.Time) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestSystem(t *testing.T) {
	t.Parallel()
	var c clock.Clock = clock.System{}
	assert.WithinDuration(t, time.Now(), c.Now(), time.Second)
	<-c.After(time.Microsecond)
	tk := c.NewTicker(time.Millisecond)
	<-tk.C()
	tk.Reset(time.Millisecond)
	<-tk.C()
	tk.Stop()
}

func TestFake_After(t *testing.T) {
	t.Parallel()
	f := clock.NewFake(epoch)
	ch := f.After(time.Second)
	assert.Equal(t, 1, f.Waiters())
	f.Advance(999 * time.Millisecond)
	assert.False(t, fired(ch))
	f.Advance(time.Millisecond)
	select {
	case got := <-ch:
		assert.Equal(t, epoch.Add(time.Second), got)
	default:
		t.Fatal("timer did not fire")
	}
	assert.Equal(t, 0, f.Waiters())
}

func TestFake_AfterNonPositiveFiresImmediately(t *testing.T) {
	t.Parallel()
	f := clock.NewFake(epoch)
	assert.True(t, fired(f.After(0)))
	assert.Equal(t, 0, f.Waiters())
}

func TestFake_Ticker(t *testing.T) {
	t.Parallel()
	f := clock.NewFake(epoch)
	tk := f.NewTicker(time.Second)
	f.Advance(time.Second)
	assert.True(t, fired(tk.C()))
	f.Advance(3 * time.Second)
	assert.True(t, fired(tk.C()))
	assert.False(t, fired(tk.C()))
	tk.Reset(10 * time.Second)
	f.Advance(9 * time.Second)
	assert.False(t, fired(tk.C()))
	f.Advance(time.Second)
	assert.True(t, fired(tk.C()))
	tk.Stop()
	assert.Equal(t, 0, f.Waiters())
	f.Advance(time.Hour)
	assert.False(t, fired(tk.C()))
}

func TestFake_SetNeverMovesBackwards(t *testing.T) {
	t.Parallel()
	f := clock.NewFake(epoch)
	f.Set(epoch.Add(-time.Hour))
	assert.Equal(t, epoch, f.Now())
	ch := f.After(time.Minute)
	f.Set(epoch.Add(time.Minute))
	assert.True(t, fired(ch))
	assert.Equal(t, epoch.Add(time.Minute), f.Now())
}

func TestFake_WaiterChange(t *testing.T) {
	t.Parallel()
	f := clock.NewFake(epoch)
	changed := f.WaiterChange()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-f.After(time.Second)
	}()
	<-changed
	f.Advance(time.Second)
	<-done
}

func TestFake_TickerPanicsOnNonPositive(t *testing.T) {
	t.Parallel()
	f := clock.NewFake(epoch)
	require.Panics(t, func() { f.NewTicker(0) })
	tk := f.NewTicker(time.Second)
	require.Panics(t, func() { tk.Reset(0) })
}
