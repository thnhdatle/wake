// SPDX-License-Identifier: Apache-2.0

package redisl2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

func TestMillis_RoundsUp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{"zero means no expiry", 0, 0},
		{"negative", -time.Second, 0},
		{"sub-millisecond", time.Microsecond, 1},
		{"exact", 300 * time.Millisecond, 300},
		{"fraction", 1500 * time.Microsecond, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, millis(tc.in))
		})
	}
}

func TestParseUint_Replies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      any
		want    uint64
		wantErr bool
	}{
		{"decimal string", "1760000000000001", 1760000000000001, false},
		{"max uint64", "18446744073709551615", 1<<64 - 1, false},
		{"integer", int64(42), 42, false},
		{"negative integer", int64(-1), 0, true},
		{"not a number", "1e5", 0, true},
		{"nil", nil, 0, true},
		{"array", []any{"1"}, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseUint(tc.in)
			if tc.wantErr {
				require.ErrorIs(t, err, l2.ErrMalformed)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseRead_Shapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      any
		want    l2.ReadResult
		wantErr bool
	}{
		{"miss", []any{"7"}, l2.ReadResult{Gen: 7}, false},
		{
			"hit",
			[]any{"7", "1500", "a|b\x00"},
			l2.ReadResult{Gen: 7, Hit: true, TTL: 1500 * time.Millisecond, Payload: []byte("a|b\x00")},
			false,
		},
		{
			"hit empty payload",
			[]any{"7", "1", ""},
			l2.ReadResult{Gen: 7, Hit: true, TTL: time.Millisecond, Payload: []byte{}},
			false,
		},
		{"not an array", "7", l2.ReadResult{}, true},
		{"two elements", []any{"7", "1"}, l2.ReadResult{}, true},
		{"bad gen", []any{"x"}, l2.ReadResult{}, true},
		{"bad ttl", []any{"7", "x", "p"}, l2.ReadResult{}, true},
		{"payload not a string", []any{"7", "1", int64(3)}, l2.ReadResult{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseRead(tc.in)
			if tc.wantErr {
				require.ErrorIs(t, err, l2.ErrMalformed)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsNoScript_Variants(t *testing.T) {
	t.Parallel()
	assert.True(t, isNoScript(redis.ErrNoScript))
	assert.True(t, isNoScript(fmt.Errorf("wrapped: %w", redis.ErrNoScript)))
	assert.False(t, isNoScript(errors.New("ERR other")))
	assert.False(t, isNoScript(nil))
}

func TestUnavailable_WrapsCauseAndKind(t *testing.T) {
	t.Parallel()
	cause := context.DeadlineExceeded
	for _, err := range []error{unavailable("read", "k", cause), unavailable("epoch", "", cause)} {
		require.ErrorIs(t, err, ErrUnavailable)
		require.ErrorIs(t, err, cause)
	}
	assert.Contains(t, unavailable("read", "user:1", cause).Error(), `read "user:1"`)
}

func TestInvalidatePrefix_MatchesWireFormat(t *testing.T) {
	t.Parallel()
	ev := l2.Event{Kind: l2.EventInvalidate, Epoch: 1760000000000000, Gen: 1760000000000123, Key: "a|b"}
	got := invalidatePrefix(ev.Epoch) + "1760000000000123" + "|" + ev.Key
	assert.Equal(t, l2.EncodeMessage(ev), got)
}

func TestBackoff_BoundedAndGrowing(t *testing.T) {
	t.Parallel()
	for n := 1; n <= 40; n++ {
		d := backoff(n)
		ceiling := min(backoffMin<<min(n-1, 10), backoffMax)
		assert.GreaterOrEqual(t, d, ceiling/2, "attempt %d", n)
		assert.Less(t, d, ceiling, "attempt %d", n)
	}
}

func TestIsTimeout_NetErrorsOnly(t *testing.T) {
	t.Parallel()
	assert.True(t, isTimeout(timeoutErr{}))
	assert.True(t, isTimeout(fmt.Errorf("read: %w", timeoutErr{})))
	assert.False(t, isTimeout(errors.New("EOF")))
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func offlineDialer(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("offline")
}
