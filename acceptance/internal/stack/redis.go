// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisService is the compose service running Redis.
const RedisService = "redis"

// redisPingTimeout bounds one PING.
const redisPingTimeout = 2 * time.Second

// NewRedisClient returns a go-redis client for the compose Redis (RedisAddr),
// closed at cleanup. Every cache instance of a test gets its own client, as
// separate processes would.
func NewRedisClient(tb testing.TB) *redis.Client {
	tb.Helper()
	c := redis.NewClient(&redis.Options{Addr: RedisAddr})
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// PingRedis sends one PING to the compose Redis on a fresh connection, so a
// broken pooled connection never hides that the server is back (or gone).
func PingRedis(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, redisPingTimeout)
	defer cancel()
	c := redis.NewClient(&redis.Options{Addr: RedisAddr, MaxRetries: -1, DialTimeout: redisPingTimeout})
	defer func() { _ = c.Close() }()
	if err := c.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("stack: ping redis: %w", err)
	}
	return nil
}

// KillRedis sends SIGKILL to the Redis container (an abrupt outage: no
// shutdown save, connections reset).
func KillRedis(ctx context.Context) error { return KillService(ctx, RedisService) }

// StartRedis starts the Redis container again and waits for its healthcheck.
func StartRedis(ctx context.Context) error { return StartService(ctx, RedisService) }

// WaitRedis polls PingRedis every interval until it succeeds and returns the
// time of the first successful PING, or the context's error.
func WaitRedis(ctx context.Context, interval time.Duration) (time.Time, error) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if err := PingRedis(ctx); err == nil {
			return time.Now(), nil
		}
		select {
		case <-ctx.Done():
			return time.Time{}, fmt.Errorf("stack: wait for redis: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
