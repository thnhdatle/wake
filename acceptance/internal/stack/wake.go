// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// wakeStartTimeout bounds starting the Wake container until /readyz is 200.
const wakeStartTimeout = 4 * time.Minute

// WakeOptions configure the Wake container of one test.
type WakeOptions struct {
	// ID is the criterion id; the container log is saved to
	// acceptance/logs/<ID>-wake.log.
	ID string
	// Config is the YAML written to /etc/wake/config.yaml. source.name is read
	// from it to find the slot and the publications.
	Config string
	// Env adds environment variables (for example a webhook secret).
	Env map[string]string
	// FreshState removes the container and its state volume and drops the
	// slot and publications of the source before starting.
	FreshState bool
	// NoWait starts the container without waiting for /readyz.
	NoWait bool
}

// Wake controls the Wake compose service started with a test configuration.
type Wake struct {
	id       string
	source   string
	override string
	admin    *Admin
}

// logState tracks, per criterion id, whether a container was configured and
// how many log parts were saved in this process.
var logState = struct {
	sync.Mutex
	configured map[string]bool
	parts      map[string]int
	latest     map[string]*Wake
}{configured: map[string]bool{}, parts: map[string]int{}, latest: map[string]*Wake{}}

// isLatest reports whether w is the last Wake configured for its id.
func isLatest(w *Wake) bool {
	logState.Lock()
	defer logState.Unlock()
	return logState.latest[w.id] == w
}

// markConfigured records that id configured a container and reports whether
// it had done so before.
func markConfigured(id string) bool {
	logState.Lock()
	defer logState.Unlock()
	before := logState.configured[id]
	logState.configured[id] = true
	return before
}

// nextLogPart returns the 1-based part number of the next saved log.
func nextLogPart(id string) int {
	logState.Lock()
	defer logState.Unlock()
	logState.parts[id]++
	return logState.parts[id]
}

// ConfigureWake writes the configuration and a compose override file into a
// temporary directory, recreates the wake service with them (`docker compose
// -f deploy/docker-compose.yml -f <override> up -d --force-recreate --wait
// wake`) and registers a cleanup that saves the container log and stops the
// service.
func ConfigureWake(tb testing.TB, opts WakeOptions) *Wake {
	tb.Helper()
	source, err := sourceName(opts.Config)
	if err != nil {
		failf(tb, opts.ID, "stack: %v", err)
	}
	dir := tb.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(opts.Config), 0o644); err != nil { //nolint:gosec // read by the container user
		failf(tb, opts.ID, "stack: write wake config: %v", err)
	}
	override := filepath.Join(dir, "compose.override.yaml")
	if err := os.WriteFile(override, overrideYAML(cfgPath, opts.Env), 0o600); err != nil {
		failf(tb, opts.ID, "stack: write compose override: %v", err)
	}
	w := &Wake{id: opts.ID, source: source, override: override, admin: NewAdmin(AdminAddr)}

	ctx, cancel := context.WithTimeout(context.Background(), wakeStartTimeout)
	defer cancel()
	if markConfigured(opts.ID) {
		w.saveLogs(ctx, tb) // the container this test configured before
	}
	logState.Lock()
	logState.latest[opts.ID] = w
	logState.Unlock()
	if opts.FreshState {
		if err := w.resetState(ctx); err != nil {
			failf(tb, opts.ID, "stack: reset wake state: %v", err)
		}
	}
	tb.Cleanup(func() { w.cleanup(tb) })
	args := []string{"up", "-d", "--force-recreate", "--no-deps"}
	if !opts.NoWait {
		args = append(args, "--wait", "--wait-timeout", "180")
	}
	if out, err := w.compose(ctx, append(args, WakeService)...); err != nil {
		failf(tb, opts.ID, "stack: start wake: %v\n%s", err, out)
	}
	if !opts.NoWait {
		if err := w.WaitReady(ctx); err != nil {
			failf(tb, opts.ID, "stack: %v", err)
		}
	}
	return w
}

// failf records a failing result for id (when set) and stops the test.
func failf(tb testing.TB, id, format string, args ...any) {
	tb.Helper()
	if id != "" {
		harness.Failf(tb, id, format, args...)
		return
	}
	tb.Fatalf(format, args...)
}

// sourceName reads source.name from the configuration.
func sourceName(cfg string) (string, error) {
	var doc struct {
		Source struct {
			Name string `yaml:"name"`
		} `yaml:"source"`
	}
	if err := yaml.Unmarshal([]byte(cfg), &doc); err != nil {
		return "", fmt.Errorf("parse wake config: %w", err)
	}
	if doc.Source.Name == "" {
		return "", errors.New("wake config has no source.name")
	}
	return doc.Source.Name, nil
}

// overrideYAML mounts the test configuration over the default one, adds env
// and maps host.docker.internal to the host (Linux Docker; Docker Desktop
// resolves it already).
func overrideYAML(cfgPath string, env map[string]string) []byte {
	var b strings.Builder
	b.WriteString("services:\n  wake:\n    volumes:\n")
	fmt.Fprintf(&b, "      - %s\n", quoteYAML(cfgPath+":/etc/wake/config.yaml:ro"))
	b.WriteString("      - wake-data:/var/lib/wake\n")
	b.WriteString("    extra_hosts:\n      - \"host.docker.internal:host-gateway\"\n")
	if len(env) > 0 {
		keys := make([]string, 0, len(env))
		for k := range env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString("    environment:\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "      %s: %s\n", k, quoteYAML(env[k]))
		}
	}
	return []byte(b.String())
}

func quoteYAML(s string) string {
	q, _ := json.Marshal(s)
	return string(q)
}

func (w *Wake) compose(ctx context.Context, args ...string) (string, error) {
	return Compose(ctx, []string{w.override}, args...)
}

// resetState removes the container and the wake-data volume and drops the
// source's slot and publications.
func (w *Wake) resetState(ctx context.Context) error {
	if out, err := w.compose(ctx, "rm", "-s", "-f", WakeService); err != nil {
		return fmt.Errorf("compose rm wake: %w: %s", err, out)
	}
	vols, err := Docker(ctx, "volume", "ls", "-q",
		"--filter", "label=com.docker.compose.project="+ComposeProject,
		"--filter", "label=com.docker.compose.volume=wake-data")
	if err != nil {
		return err
	}
	for _, v := range strings.Fields(vols) {
		if _, rmErr := Docker(ctx, "volume", "rm", "-f", v); rmErr != nil {
			return rmErr
		}
	}
	pg, err := openPG(ctx, 2)
	if err != nil {
		return err
	}
	defer pg.Close(ctx)
	return pg.DropSource(ctx, w.source)
}

// Source returns source.name of the configuration.
func (w *Wake) Source() string { return w.source }

// Slot returns the replication slot name of the source.
func (w *Wake) Slot() string { return SlotName(w.source) }

// Admin returns the admin client.
func (w *Wake) Admin() *Admin { return w.admin }

// ContainerID returns the id of the wake container, running or not.
func (w *Wake) ContainerID(ctx context.Context) (string, error) {
	out, err := w.compose(ctx, "ps", "-a", "-q", WakeService)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(out)
	if id == "" {
		return "", errors.New("stack: no wake container")
	}
	return strings.Fields(id)[0], nil
}

// Kill sends SIGKILL to the Wake container (kill -9).
func (w *Wake) Kill(ctx context.Context) error {
	id, err := w.ContainerID(ctx)
	if err != nil {
		return err
	}
	_, err = Docker(ctx, "kill", "-s", "KILL", id)
	return err
}

// Stop stops the container gracefully (SIGTERM, compose timeout).
func (w *Wake) Stop(ctx context.Context) error {
	_, err := w.compose(ctx, "stop", WakeService)
	return err
}

// Start starts the existing container with the same configuration and
// waits until /readyz is 200.
func (w *Wake) Start(ctx context.Context) error {
	if _, err := w.compose(ctx, "up", "-d", "--no-deps", "--no-recreate", WakeService); err != nil {
		return err
	}
	return w.WaitReady(ctx)
}

// WaitReady polls /readyz until it returns 200 or ctx ends.
func (w *Wake) WaitReady(ctx context.Context) error {
	var last ReadyState
	for {
		st, err := w.admin.Ready(ctx)
		if err == nil && st.OK {
			return nil
		}
		last = st
		select {
		case <-ctx.Done():
			return fmt.Errorf("wake not ready: %s: %w", last.Body, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// ContainerState is the subset of `docker inspect` the tests use.
type ContainerState struct {
	Running      bool
	RestartCount int
	StartedAt    string
	ExitCode     int
	OOMKilled    bool
}

// State inspects the container.
func (w *Wake) State(ctx context.Context) (ContainerState, error) {
	var st ContainerState
	id, err := w.ContainerID(ctx)
	if err != nil {
		return st, err
	}
	out, err := Docker(ctx, "inspect", "--format",
		"{{json .State.Running}} {{json .RestartCount}} {{json .State.StartedAt}} {{json .State.ExitCode}} "+
			"{{json .State.OOMKilled}}", id)
	if err != nil {
		return st, err
	}
	f := strings.Fields(out)
	if len(f) != 5 {
		return st, fmt.Errorf("stack: unexpected inspect output %q", out)
	}
	for i, dst := range []any{&st.Running, &st.RestartCount, &st.StartedAt, &st.ExitCode, &st.OOMKilled} {
		if err := json.Unmarshal([]byte(f[i]), dst); err != nil {
			return st, fmt.Errorf("stack: inspect field %d: %w", i, err)
		}
	}
	return st, nil
}

// Logs returns the container log (stdout and stderr).
func (w *Wake) Logs(ctx context.Context) (string, error) {
	id, err := w.ContainerID(ctx)
	if err != nil {
		return "", err
	}
	return Docker(ctx, "logs", "--timestamps", id)
}

// saveLogs appends the current container's log to acceptance/logs/<ID>-wake.log.
func (w *Wake) saveLogs(ctx context.Context, tb testing.TB) {
	tb.Helper()
	if w.id == "" {
		return
	}
	logs, err := w.Logs(ctx)
	if err != nil || strings.TrimSpace(logs) == "" {
		return
	}
	dir, err := harness.LogsDir()
	if err != nil {
		tb.Logf("stack: logs dir: %v", err)
		return
	}
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		tb.Logf("stack: create logs dir: %v", mkErr)
		return
	}
	path := filepath.Join(dir, w.id+"-wake.log")
	part := nextLogPart(w.id)
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if part == 1 {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o600) //nolint:gosec // acceptance logs dir
	if err != nil {
		tb.Logf("stack: open %s: %v", path, err)
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = fmt.Fprintf(f, "==== wake container log, part %d, saved %s ====\n%s", part,
		time.Now().UTC().Format(time.RFC3339), logs)
}

// SaveLogs appends the current container log to acceptance/logs/<ID>-wake.log;
// call it before replacing the container within one test.
func (w *Wake) SaveLogs(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()
	w.saveLogs(ctx, tb)
}

// cleanup saves the log, then removes the container and its state volume
// and drops the source's slot, so a finished test leaves no WAL retained and
// no state that the default compose configuration would refuse.
func (w *Wake) cleanup(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()
	if isLatest(w) {
		w.saveLogs(ctx, tb)
	}
	if err := w.resetState(ctx); err != nil {
		tb.Logf("stack: reset wake state: %v", err)
	}
}
