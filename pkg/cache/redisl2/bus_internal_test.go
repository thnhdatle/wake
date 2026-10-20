// SPDX-License-Identifier: Apache-2.0

package redisl2

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

// drain returns the events handle queued so far.
func drain(b *bus) []l2.Event {
	var got []l2.Event
	for {
		select {
		case ev := <-b.events:
			got = append(got, ev)
		default:
			return got
		}
	}
}

// TestBusHandle_EventMapping drives handle with the replies go-redis
// produces, without a connection: the first "subscribe" confirmation is
// EventConnected, every later one (go-redis reconnected by itself) is a
// disconnect followed by a connect; messages are decoded, foreign channels,
// garbage, pongs and other subscription kinds are skipped.
func TestBusHandle_EventMapping(t *testing.T) {
	t.Parallel()
	const ch = "wake:inval:ns"
	b := &bus{channel: ch, events: make(chan l2.Event, 16)}
	ctx := t.Context()
	confirmed := false
	steps := []struct {
		name string
		msg  any
		want []l2.Event
	}{
		{"first subscribe", &redis.Subscription{Kind: "subscribe", Channel: ch, Count: 1}, []l2.Event{
			{Kind: l2.EventConnected},
		}},
		{"invalidate", &redis.Message{Channel: ch, Payload: "i|3|9|a|b"}, []l2.Event{
			{Kind: l2.EventInvalidate, Epoch: 3, Gen: 9, Key: "a|b"},
		}},
		{"loaded", &redis.Message{Channel: ch, Payload: "l|3|k"}, []l2.Event{{Kind: l2.EventLoaded, Epoch: 3, Key: "k"}}},
		{"epoch", &redis.Message{Channel: ch, Payload: "e|4"}, []l2.Event{{Kind: l2.EventEpoch, Epoch: 4}}},
		{"garbage", &redis.Message{Channel: ch, Payload: "hello"}, nil},
		{"other channel", &redis.Message{Channel: "other", Payload: "e|5"}, nil},
		{"pong", &redis.Pong{}, nil},
		{"unsubscribe", &redis.Subscription{Kind: "unsubscribe", Channel: ch}, nil},
		{"subscribe to another channel", &redis.Subscription{Kind: "subscribe", Channel: "other", Count: 2}, nil},
		{"second subscribe", &redis.Subscription{Kind: "subscribe", Channel: ch, Count: 1}, []l2.Event{
			{Kind: l2.EventDisconnected}, {Kind: l2.EventConnected},
		}},
	}
	for _, st := range steps {
		require.True(t, b.handle(ctx, st.msg, &confirmed), st.name)
		assert.Equal(t, st.want, drain(b), st.name)
	}
	assert.True(t, confirmed)
	assert.True(t, b.connected)
}

func TestBusHandle_StopsWhenContextEnds(t *testing.T) {
	t.Parallel()
	b := &bus{channel: "c", events: make(chan l2.Event)} // unbuffered, nobody receives
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	confirmed := false
	assert.False(t, b.handle(ctx, &redis.Subscription{Kind: "subscribe", Channel: "c"}, &confirmed))
	assert.False(t, b.handle(ctx, &redis.Message{Channel: "c", Payload: "e|1"}, &confirmed))
	b.connected = true
	assert.False(t, b.handle(ctx, &redis.Subscription{Kind: "subscribe", Channel: "c"}, &confirmed),
		"the disconnect of a reconnect cannot be delivered either")
	assert.False(t, sleep(ctx, backoffMax), "sleep returns at once when ctx ended")
}

func TestStartBus_HealthCheckBounds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"default", 0, defaultHealthCheck},
		{"floor", time.Nanosecond, minHealthCheck},
		{"configured", 2 * defaultHealthCheck, 2 * defaultHealthCheck},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rdb := redis.NewClient(&redis.Options{
				Addr: "127.0.0.1:0", DialerRetries: 1, MaxRetries: -1,
				Dialer: offlineDialer,
			})
			defer func() { _ = rdb.Close() }()
			b := startBus(rdb, "c", tc.in)
			defer b.close()
			assert.Equal(t, tc.want, b.health)
		})
	}
}
