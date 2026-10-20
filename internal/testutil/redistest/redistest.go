// SPDX-License-Identifier: Apache-2.0

package redistest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
)

// Image is the Redis image every helper uses.
const Image = "redis:7"

const (
	startTimeout = 3 * time.Minute
	// Client timeouts: short, so an unreachable server fails commands fast.
	dialTimeout = time.Second
	ioTimeout   = time.Second
)

// clientOptions returns test client options: short timeouts, one dial
// attempt and no command retries, so outages surface quickly and
// deterministically.
func clientOptions(dialer func(ctx context.Context, network, addr string) (net.Conn, error)) *redis.Options {
	return &redis.Options{
		Dialer:        dialer,
		DialTimeout:   dialTimeout,
		DialerRetries: 1,
		ReadTimeout:   ioTimeout,
		WriteTimeout:  ioTimeout,
		MaxRetries:    -1,
	}
}

// endpoint returns the current host:port published for port.
func endpoint(ctx context.Context, ctr testcontainers.Container, port string) (string, error) {
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", err
	}
	p, err := ctr.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, p.Port()), nil
}

func startContainer(tb testing.TB, opts ...testcontainers.ContainerCustomizer) *testcontainers.DockerContainer {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, Image, opts...)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("redistest: start %s: %v", Image, err)
	}
	return ctr
}

// waitPing polls PING until the server answers or the start timeout ends.
func waitPing(tb testing.TB, rdb redis.UniversalClient) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		err := rdb.Ping(ctx).Err()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			tb.Fatalf("redistest: server did not answer PING: %v", err)
		case <-tick.C:
		}
	}
}
