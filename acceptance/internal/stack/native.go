// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// nativeStopTimeout bounds a graceful stop (SIGTERM) before SIGKILL.
const nativeStopTimeout = 30 * time.Second

// nativeBinary is the wake binary built once per test process.
var nativeBinary struct {
	once sync.Once
	path string
	err  error
}

// buildNative builds ./cmd/wake into a temporary directory once per process
// (performance criteria measure a natively running Wake, not one in a container).
func buildNative(ctx context.Context) (string, error) {
	nativeBinary.once.Do(func() {
		root, err := Root()
		if err != nil {
			nativeBinary.err = err
			return
		}
		dir, err := os.MkdirTemp("", "wake-native-*")
		if err != nil {
			nativeBinary.err = fmt.Errorf("stack: temp dir: %w", err)
			return
		}
		out := filepath.Join(dir, "wake")
		cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", out, "./cmd/wake") //nolint:gosec // fixed args
		cmd.Dir = root
		if b, err := cmd.CombinedOutput(); err != nil {
			nativeBinary.err = fmt.Errorf("stack: go build ./cmd/wake: %w: %s", err, b)
			return
		}
		nativeBinary.path = out
	})
	return nativeBinary.path, nativeBinary.err
}

// NativeOptions configure a natively running Wake.
type NativeOptions struct {
	// ID is the criterion id; the process output is appended to
	// acceptance/logs/<ID>-wake.log.
	ID string
	// Config is the YAML configuration. NativeConfig rewrites a compose
	// configuration for the host.
	Config string
	// Env adds environment variables.
	Env map[string]string
}

// NativeWake is a Wake process on the host, with the compose dependencies.
type NativeWake struct {
	id     string
	source string
	cmd    *exec.Cmd
	admin  *Admin
	done   chan struct{}
	err    error
}

// NativeConfig rewrites a configuration written for the compose service
// (paths under /var/lib/wake, admin on 0.0.0.0:9093, Kafka at kafka:29092)
// for a process on the host using dir for state and spill files.
func NativeConfig(cfg, dir string) string {
	return strings.NewReplacer(
		"/var/lib/wake", dir,
		"0.0.0.0:9093", AdminAddr,
		KafkaInternalBroker, KafkaBroker,
	).Replace(cfg)
}

// StartNativeWake stops the compose wake service (its admin port is the
// same), drops the source's slot and publications, starts the binary with
// the configuration and waits until /readyz is 200. Cleanup stops the
// process and drops the source again.
func StartNativeWake(tb testing.TB, opts NativeOptions) *NativeWake {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wakeStartTimeout)
	defer cancel()
	source, err := sourceName(opts.Config)
	if err != nil {
		failf(tb, opts.ID, "stack: %v", err)
	}
	bin, err := buildNative(ctx)
	if err != nil {
		failf(tb, opts.ID, "%v", err)
	}
	if out, cerr := Compose(ctx, nil, "stop", WakeService); cerr != nil {
		failf(tb, opts.ID, "stack: stop compose wake: %v\n%s", cerr, out)
	}
	if err = dropSource(ctx, source); err != nil {
		failf(tb, opts.ID, "stack: %v", err)
	}
	dir := tb.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err = os.WriteFile(cfgPath, []byte(opts.Config), 0o600); err != nil {
		failf(tb, opts.ID, "stack: write config: %v", err)
	}
	logFile, err := openNativeLog(opts.ID)
	if err != nil {
		failf(tb, opts.ID, "stack: %v", err)
	}
	w := &NativeWake{id: opts.ID, source: source, admin: NewAdmin(AdminAddr), done: make(chan struct{})}
	w.cmd = exec.Command(bin, "run", "--config", cfgPath) //nolint:gosec,noctx // built binary; stopped at cleanup
	w.cmd.Env = append(os.Environ(), "WAKE_SOURCE_DSN="+PostgresDSN, "WAKE_LOG_FORMAT=text")
	for k, v := range opts.Env {
		w.cmd.Env = append(w.cmd.Env, k+"="+v)
	}
	w.cmd.Stdout, w.cmd.Stderr = logFile, logFile
	if err := w.cmd.Start(); err != nil {
		_ = logFile.Close()
		failf(tb, opts.ID, "stack: start native wake: %v", err)
	}
	go func() {
		w.err = w.cmd.Wait()
		_ = logFile.Close()
		close(w.done)
	}()
	tb.Cleanup(func() { w.cleanup(tb) })
	if err := w.waitReady(ctx); err != nil {
		failf(tb, opts.ID, "stack: %v", err)
	}
	return w
}

// openNativeLog opens acceptance/logs/<id>-wake.log for appending.
func openNativeLog(id string) (*os.File, error) {
	dir, err := harness.LogsDir()
	if err != nil {
		return nil, err //nolint:wrapcheck // harness errors carry context
	}
	if err = os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create logs dir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, id+"-wake.log"), //nolint:gosec // acceptance logs dir
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wake log: %w", err)
	}
	_, _ = fmt.Fprintf(f, "==== native wake, started %s ====\n", time.Now().UTC().Format(time.RFC3339))
	return f, nil
}

// waitReady polls /readyz until 200, the process exits or ctx ends.
func (w *NativeWake) waitReady(ctx context.Context) error {
	var last ReadyState
	for {
		st, err := w.admin.Ready(ctx)
		if err == nil && st.OK {
			return nil
		}
		last = st
		select {
		case <-w.done:
			return fmt.Errorf("native wake exited before ready: %w", w.err)
		case <-ctx.Done():
			return fmt.Errorf("native wake not ready: %s: %w", last.Body, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Source returns source.name of the configuration.
func (w *NativeWake) Source() string { return w.source }

// Slot returns the replication slot name of the source.
func (w *NativeWake) Slot() string { return SlotName(w.source) }

// Admin returns the admin client.
func (w *NativeWake) Admin() *Admin { return w.admin }

// Exited reports whether the process has exited, with its wait error.
func (w *NativeWake) Exited() (bool, error) {
	select {
	case <-w.done:
		return true, w.err
	default:
		return false, nil
	}
}

// Stop sends SIGTERM and waits for the exit, then SIGKILL after
// nativeStopTimeout.
func (w *NativeWake) Stop() error {
	if exited, _ := w.Exited(); exited {
		return nil
	}
	if err := w.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("stack: SIGTERM wake: %w", err)
	}
	select {
	case <-w.done:
		return nil
	case <-time.After(nativeStopTimeout):
		_ = w.cmd.Process.Kill()
		<-w.done
		return errors.New("stack: native wake did not stop within 30s; killed")
	}
}

func (w *NativeWake) cleanup(tb testing.TB) {
	tb.Helper()
	if err := w.Stop(); err != nil {
		tb.Logf("%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()
	if err := dropSource(ctx, w.source); err != nil {
		tb.Logf("stack: %v", err)
	}
}

// dropSource drops the slot and publications of source.
func dropSource(ctx context.Context, source string) error {
	pg, err := openPG(ctx, 2)
	if err != nil {
		return err
	}
	defer pg.Close(ctx)
	return pg.DropSource(ctx, source)
}
