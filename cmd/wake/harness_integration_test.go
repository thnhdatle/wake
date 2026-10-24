// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
	"github.com/thnhdatle/wake/internal/testutil/pgtest"
)

var (
	srv     *pgtest.Server
	wakeBin string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wake-it-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "temp dir:", err)
		os.Exit(1)
	}
	wakeBin = filepath.Join(dir, "wake")
	args := []string{"build", "-o", wakeBin}
	if raceEnabled {
		args = append(args, "-race")
	}
	build := exec.CommandContext(context.Background(), "go", append(args, ".")...)
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build wake: %v\n%s", err, out)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := pgtest.Main(m, &srv)
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

const (
	waitLong  = 4 * time.Minute
	waitShort = time.Minute
	poll      = 100 * time.Millisecond
)

type env struct {
	t      *testing.T
	k      *kafkatest.Kafka
	db     *pgtest.Database
	conn   *pgx.Conn
	name   string
	prefix string
	dir    string
}

func newEnv(t *testing.T, k *kafkatest.Kafka, name string) *env {
	t.Helper()
	db := srv.NewDatabase(t)
	return &env{t: t, k: k, db: db, conn: db.Conn(t), name: name, prefix: "cdc_" + name, dir: t.TempDir()}
}

func (e *env) config() config.Config {
	cfg := config.Default()
	cfg.Source.Name = e.name
	cfg.Tables.Include = []string{"public.*"}
	cfg.Tables.RefreshInterval = config.Duration(time.Second)
	cfg.Snapshot.ChunkRows = 200
	cfg.Snapshot.ParallelTables = 2
	cfg.Stream.SpillDir = filepath.Join(e.dir, "spill")
	cfg.Stream.StatusInterval = config.Duration(time.Second)
	cfg.State.Path = filepath.Join(e.dir, "state.json")
	cfg.Lag.Interval = config.Duration(time.Second)
	cfg.Admin.Listen = freeAddr(e.t)
	cfg.Sinks = []config.Sink{{
		Name: "kafka-main", Type: config.SinkKafka, Tables: []string{"*"}, MaxEvents: 1000,
		MaxBytes: config.ByteSize(1 << 20), Linger: config.Duration(5 * time.Millisecond),
		MaxEventBytes: config.ByteSize(1 << 20),
		Kafka: &config.KafkaSink{
			Brokers: e.k.Brokers, TopicPrefix: e.prefix, Tombstones: true, AutoCreateTopics: true,
			Partitions: 3, ReplicationFactor: 1,
		},
	}}
	return cfg
}

func (e *env) writeConfig(cfg *config.Config) string {
	e.t.Helper()
	p := filepath.Join(e.dir, fmt.Sprintf("wake-%d.yaml", time.Now().UnixNano()))
	e.rewriteConfig(p, cfg)
	return p
}

func (e *env) rewriteConfig(path string, cfg *config.Config) {
	e.t.Helper()
	require.NoError(e.t, cfg.Validate())
	out, err := cfg.YAML()
	require.NoError(e.t, err)
	require.NoError(e.t, os.WriteFile(path, out, 0o600))
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func (e *env) exec(sql string) {
	e.t.Helper()
	_, err := e.conn.Exec(e.t.Context(), sql)
	require.NoError(e.t, err, sql)
}

func (e *env) currentLSN() lsn.LSN {
	e.t.Helper()
	var s string
	require.NoError(e.t, e.conn.QueryRow(e.t.Context(), "SELECT pg_current_wal_lsn()::text").Scan(&s))
	l, err := lsn.Parse(s)
	require.NoError(e.t, err)
	return l
}

func (e *env) slotRow() (exists, active bool, confirmed lsn.LSN) {
	e.t.Helper()
	var s *string
	err := e.conn.QueryRow(e.t.Context(), "SELECT active, confirmed_flush_lsn::text FROM pg_replication_slots "+
		"WHERE slot_name = $1", "wake_"+e.name).Scan(&active, &s)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, 0
	}
	require.NoError(e.t, err)
	if s != nil {
		confirmed, err = lsn.Parse(*s)
		require.NoError(e.t, err)
	}
	return true, active, confirmed
}

type wakeProc struct {
	t       *testing.T
	cmd     *exec.Cmd
	admin   string
	cfgPath string
	log     string
	done    chan struct{}
	err     error
}

func (e *env) startWake(cfg *config.Config) *wakeProc {
	e.t.Helper()
	path := e.writeConfig(cfg)
	p := e.spawn("run", "--config", path)
	p.admin, p.cfgPath = cfg.Admin.Listen, path
	return p
}

func (e *env) spawn(args ...string) *wakeProc {
	e.t.Helper()
	logPath := filepath.Join(e.dir, fmt.Sprintf("wake-%d.log", time.Now().UnixNano()))
	logFile, err := os.Create(logPath)
	require.NoError(e.t, err)
	cmd := exec.CommandContext(context.WithoutCancel(e.t.Context()), wakeBin, args...)
	cmd.Env = append(os.Environ(), "WAKE_SOURCE_DSN="+e.db.DSN, "GORACE=halt_on_error=0")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(e.t, cmd.Start())
	p := &wakeProc{t: e.t, cmd: cmd, log: logPath, done: make(chan struct{})}
	go func() {
		defer close(p.done)
		p.err = cmd.Wait()
		_ = logFile.Close()
	}()
	e.t.Cleanup(func() {
		p.terminate()
		p.checkLog()
	})
	return p
}

func (p *wakeProc) terminate() {
	if p.exited() {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(waitShort):
		p.t.Errorf("wake did not stop within %s after SIGTERM", waitShort)
		p.kill()
	}
}

func (p *wakeProc) stop() {
	p.t.Helper()
	p.terminate()
	require.NoError(p.t, p.err, "wake exit\n%s", p.tail())
}

func (p *wakeProc) kill() {
	_ = p.cmd.Process.Kill()
	<-p.done
}

func (p *wakeProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *wakeProc) wait(timeout time.Duration) int {
	p.t.Helper()
	select {
	case <-p.done:
	case <-time.After(timeout):
		p.t.Fatalf("wake still running after %s\n%s", timeout, p.tail())
	}
	var ee *exec.ExitError
	if errors.As(p.err, &ee) {
		return ee.ExitCode()
	}
	require.NoError(p.t, p.err)
	return 0
}

func (p *wakeProc) output() string {
	b, err := os.ReadFile(p.log)
	require.NoError(p.t, err)
	return string(b)
}

func (p *wakeProc) tail() string {
	b, err := os.ReadFile(p.log)
	if err != nil {
		return err.Error()
	}
	if len(b) > 8000 {
		b = b[len(b)-8000:]
	}
	return string(b)
}

func (p *wakeProc) checkLog() {
	out, err := os.ReadFile(p.log)
	if err != nil {
		return
	}
	if bytes.Contains(out, []byte("WARNING: DATA RACE")) {
		p.t.Errorf("data race in wake:\n%s", out)
	}
	if p.t.Failed() || os.Getenv("WAKE_IT_VERBOSE") != "" {
		p.t.Logf("wake log %s (tail):\n%s", strings.Join(p.cmd.Args, " "), p.tail())
	}
}

func (p *wakeProc) get(path string) (int, []byte) {
	ctx, cancel := context.WithTimeout(p.t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+p.admin+path, http.NoBody)
	require.NoError(p.t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(p.t, err)
	return resp.StatusCode, body
}

func (p *wakeProc) status() (statusDoc, bool) {
	code, body := p.get("/v1/status")
	if code != http.StatusOK {
		return statusDoc{}, false
	}
	var doc statusDoc
	require.NoError(p.t, json.Unmarshal(body, &doc))
	return doc, true
}

func (p *wakeProc) waitReady() {
	p.t.Helper()
	p.waitFor(waitShort, "ready", func() bool {
		code, _ := p.get("/readyz")
		return code == http.StatusOK
	})
}

func (p *wakeProc) waitFor(timeout time.Duration, what string, cond func() bool) {
	p.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for !cond() {
		select {
		case <-p.done:
			p.t.Fatalf("wake exited while waiting for %s: %v\n%s", what, p.err, p.tail())
		case <-deadline.C:
			p.t.Fatalf("timeout after %s waiting for %s\n%s", timeout, what, p.tail())
		case <-tick.C:
		}
	}
}

func (p *wakeProc) waitSnapshotsIdle() {
	p.t.Helper()
	p.waitFor(waitLong, "snapshots to finish", func() bool {
		doc, ok := p.status()
		return ok && doc.Snapshot.ConsistentPoint != "" && len(doc.Snapshot.Runs) == 0 &&
			len(doc.Snapshot.Queue) == 0
	})
}

func (e *env) sync(p *wakeProc) {
	e.t.Helper()
	var s string
	require.NoError(e.t, e.conn.QueryRow(e.t.Context(),
		"SELECT pg_logical_emit_message(true, 'test.sync', 'x')::text").Scan(&s))
	target, err := lsn.Parse(s)
	require.NoError(e.t, err)
	p.waitFor(waitLong, "confirmed LSN past "+s, func() bool {
		doc, ok := p.status()
		if !ok {
			return false
		}
		c, err := lsn.Parse(doc.ConfirmedLSN)
		return err == nil && c > target
	})
}

func (p *wakeProc) metric(series string) float64 {
	_, body := p.get("/metrics")
	for _, line := range strings.Split(string(body), "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			var v float64
			_, err := fmt.Sscan(rest, &v)
			require.NoError(p.t, err)
			return v
		}
	}
	return 0
}

func (e *env) runCLI(args ...string) (int, string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), waitShort)
	defer cancel()
	cmd := exec.CommandContext(ctx, wakeBin, args...)
	cmd.Env = append(os.Environ(), "WAKE_SOURCE_DSN="+e.db.DSN)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), out.String()
	}
	require.NoError(e.t, err, out.String())
	return 0, out.String()
}

type syncList[T any] struct {
	mu    sync.Mutex
	items []T
}

func (l *syncList[T]) add(v T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = append(l.items, v)
}

func (l *syncList[T]) all() []T {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]T(nil), l.items...)
}

func lsnOf(s string) (lsn.LSN, error) { return lsn.Parse(s) }
