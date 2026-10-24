// SPDX-License-Identifier: Apache-2.0

package redistest

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const singlePort = "6379/tcp"

// Single is a single-node Redis container.
type Single struct {
	ctr *testcontainers.DockerContainer

	mu   sync.Mutex
	addr string
}

// StartSingle starts a single node and terminates it when tb ends.
func StartSingle(tb testing.TB) *Single {
	tb.Helper()
	ctr := startContainer(tb,
		testcontainers.WithExposedPorts(singlePort),
		testcontainers.WithCmd("redis-server", "--enable-debug-command", "yes"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort(singlePort).WithStartupTimeout(startTimeout)),
	)
	s := &Single{ctr: ctr}
	s.refresh(tb)
	return s
}

func (s *Single) refresh(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	addr, err := endpoint(ctx, s.ctr, singlePort)
	if err != nil {
		tb.Fatalf("redistest: endpoint: %v", err)
	}
	s.mu.Lock()
	s.addr = addr
	s.mu.Unlock()
}

func (s *Single) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func (s *Single) dial(ctx context.Context, network, _ string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, s.Addr())
}

func (s *Single) Options() *redis.Options {
	opt := clientOptions(s.dial)
	opt.Addr = "redis:6379"
	return opt
}

func (s *Single) Client(tb testing.TB) *redis.Client {
	tb.Helper()
	return newClient(tb, s.Options())
}

func (s *Single) Stop(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	timeout := 10 * time.Second
	if err := s.ctr.Stop(ctx, &timeout); err != nil {
		tb.Fatalf("redistest: stop: %v", err)
	}
}

func (s *Single) Start(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	if err := s.ctr.Start(ctx); err != nil {
		tb.Fatalf("redistest: start: %v", err)
	}
	s.refresh(tb)
	rdb := redis.NewClient(s.Options())
	defer func() { _ = rdb.Close() }()
	waitPing(tb, rdb)
}

func (s *Single) Freeze(tb testing.TB, d time.Duration) <-chan error {
	tb.Helper()
	opt := s.Options()
	opt.ReadTimeout = d + 30*time.Second
	rdb := redis.NewClient(opt)
	waitPing(tb, rdb)
	done := make(chan error, 1)
	result := make(chan error, 1)
	go func() {
		err := rdb.Do(context.Background(), "DEBUG", "SLEEP", strconv.FormatFloat(d.Seconds(), 'f', 3, 64)).Err()
		_ = rdb.Close()
		done <- err
		result <- err
	}()
	tb.Cleanup(func() { <-done })
	return result
}

func newClient(tb testing.TB, opt *redis.Options) *redis.Client {
	tb.Helper()
	rdb := redis.NewClient(opt)
	tb.Cleanup(func() { _ = rdb.Close() })
	return rdb
}
