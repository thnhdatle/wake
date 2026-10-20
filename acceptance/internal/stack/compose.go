// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Endpoints of the compose stack as seen from the host.
const (
	// PostgresDSN connects to the compose Postgres (database app).
	PostgresDSN = "postgres://postgres:postgres@127.0.0.1:5432/app?sslmode=disable"
	// KafkaBroker is the EXTERNAL listener of the compose Kafka.
	KafkaBroker = "localhost:9092"
	// KafkaInternalBroker is the listener Wake uses inside compose.
	KafkaInternalBroker = "kafka:29092"
	// RedisAddr is the compose Redis.
	RedisAddr = "127.0.0.1:6379"
	// NATSURL is the compose NATS.
	NATSURL = "nats://127.0.0.1:4222"
	// AdminAddr is the Wake admin endpoint published on the host.
	AdminAddr = "127.0.0.1:9093"
	// HostFromContainer is the host name containers use to reach the host.
	HostFromContainer = "host.docker.internal"
	// ComposeProject is the compose project name (name: in the compose file).
	ComposeProject = "wake"
	// WakeService is the compose service running Wake.
	WakeService = "wake"
)

// composeRel is the compose file relative to the repository root.
var composeRel = filepath.Join("deploy", "docker-compose.yml")

// upTimeout bounds `docker compose up --wait` of the whole stack.
const upTimeout = 6 * time.Minute

// cmdTimeout bounds short docker commands.
const cmdTimeout = 3 * time.Minute

// ErrNoRoot is returned when no go.mod of this module is found above cwd.
var ErrNoRoot = errors.New("stack: repository root (go.mod of github.com/thnhdatle/wake) not found")

// Root returns the repository root: the first directory at or above the
// working directory whose go.mod declares module github.com/thnhdatle/wake.
func Root() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("stack: getwd: %w", err)
	}
	for {
		data, rerr := os.ReadFile(filepath.Join(dir, "go.mod")) //nolint:gosec // fixed file name in a parent dir
		if rerr == nil && isWakeModule(data) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", ErrNoRoot
		}
		dir = parent
	}
}

func isWakeModule(gomod []byte) bool {
	sc := bufio.NewScanner(bytes.NewReader(gomod))
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 && f[0] == "module" {
			return f[1] == "github.com/thnhdatle/wake"
		}
	}
	return false
}

// ComposeFile returns the absolute path of deploy/docker-compose.yml.
func ComposeFile() (string, error) {
	root, err := Root()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, composeRel), nil
}

// Docker runs the docker CLI from the repository root and returns its
// combined output. The error includes the output.
func Docker(ctx context.Context, args ...string) (string, error) {
	root, err := Root()
	if err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = root
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("stack: docker %s: %w: %s", strings.Join(args, " "), err,
			strings.TrimSpace(out.String()))
	}
	return out.String(), nil
}

// Compose runs `docker compose -f <base> [-f extra...] args...`.
func Compose(ctx context.Context, extraFiles []string, args ...string) (string, error) {
	base, err := ComposeFile()
	if err != nil {
		return "", err
	}
	full := []string{"compose", "-f", base}
	for _, f := range extraFiles {
		full = append(full, "-f", f)
	}
	return Docker(ctx, append(full, args...)...)
}

// serviceAddrs are the host endpoints of the dependency services.
var serviceAddrs = map[string]string{
	"postgres": "127.0.0.1:5432", "kafka": "127.0.0.1:9092", "nats": "127.0.0.1:4222", "redis": RedisAddr,
}

// dependencyServices are the services EnsureUp checks.
var dependencyServices = []string{"postgres", "kafka", "nats", "redis"}

// reachable reports which of the services do not accept TCP connections.
func reachable(ctx context.Context, services []string) []string {
	var down []string
	d := net.Dialer{Timeout: 2 * time.Second}
	for _, name := range services {
		c, err := d.DialContext(ctx, "tcp", serviceAddrs[name])
		if err != nil {
			down = append(down, name)
			continue
		}
		_ = c.Close()
	}
	return down
}

// servicesUp reports whether the services are healthy: every endpoint
// answers and, when listed, Postgres accepts queries.
func servicesUp(ctx context.Context, services []string) error {
	if down := reachable(ctx, services); len(down) > 0 {
		return fmt.Errorf("stack: not reachable: %s", strings.Join(down, ", "))
	}
	if !slices.Contains(services, "postgres") {
		return nil
	}
	return PingPostgres(ctx)
}

// EnsureUp verifies that the compose dependencies are reachable and, when
// they are not, runs `docker compose -f deploy/docker-compose.yml up -d
// --wait` (the acceptance runner normally did that already), so that a
// single test can be run directly.
func EnsureUp(tb testing.TB) {
	tb.Helper()
	ensure(tb, dependencyServices, nil)
}

// EnsureServices is EnsureUp restricted to the listed dependency services
// (for example "postgres", "kafka"): only they are checked and, when one is
// down, only they are started (`docker compose up -d --wait <services>`).
// Tests use it so that a service another test stopped on purpose (a Redis
// or Kafka outage) is not started behind its back.
func EnsureServices(tb testing.TB, services ...string) {
	tb.Helper()
	for _, s := range services {
		if _, ok := serviceAddrs[s]; !ok {
			tb.Fatalf("stack: unknown service %q", s)
		}
	}
	ensure(tb, services, services)
}

// ensure checks services and, when one is down, runs compose up for start
// (every service when start is nil).
func ensure(tb testing.TB, services, start []string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), upTimeout)
	defer cancel()
	if err := servicesUp(ctx, services); err == nil {
		return
	}
	tb.Logf("stack: compose services not reachable, running docker compose up -d --wait %s",
		strings.Join(start, " "))
	if out, err := Compose(ctx, nil, append([]string{"up", "-d", "--wait"}, start...)...); err != nil {
		tb.Fatalf("stack: compose up: %v\n%s", err, out)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		err := servicesUp(ctx, services)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("stack: services still unreachable after compose up: %v", err)
		}
		time.Sleep(time.Second)
	}
}

// StopService stops a compose service (for example kafka for a sink outage).
func StopService(ctx context.Context, service string) error {
	_, err := Compose(ctx, nil, "stop", service)
	return err
}

// KillService sends SIGKILL to a compose service's container.
func KillService(ctx context.Context, service string) error {
	_, err := Compose(ctx, nil, "kill", "-s", "KILL", service)
	return err
}

// StartService starts a stopped compose service and waits for its
// healthcheck. Do not use it for the wake service; use Wake.Start, which keeps
// the test configuration.
func StartService(ctx context.Context, service string) error {
	_, err := Compose(ctx, nil, "up", "-d", "--no-deps", "--no-recreate", "--wait", service)
	return err
}
