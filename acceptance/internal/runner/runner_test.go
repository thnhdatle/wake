// SPDX-License-Identifier: Apache-2.0

package runner_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/runner"
)

const fixtureCriteria = `
project: wake
machine: m
criteria:
  - {id: T-01, title: a, test: TestAcceptance_T01_A, kind: functional, real_deps: [postgres],
     requires: [docker], pass: {mac: {n: 1}, ci: {n: 2}}}
  - {id: T-02, title: b, test: BenchmarkAcceptance_T02_B, kind: performance, real_deps: [postgres, wake],
     requires: [docker, mac], pass: {mac: {n: 3}, ci: {n: 4}}}
  - {id: T-03, title: c, test: TestAcceptance_T03_C, kind: functional, real_deps: [], requires: [],
     pass: {mac: {n: 5}, ci: skip}}
  - {id: T-04, title: d, test: TestAcceptance_T04_D, kind: functional, real_deps: [redis],
     requires: [docker, bogus], pass: {mac: {n: 6}, ci: {n: 6}}}
`

const fixtureSource = `package acceptance
func TestAcceptance_T01_A(t *testing.T) {}
func BenchmarkAcceptance_T02_B(b *testing.B) {}
func TestAcceptance_T03_C(t *testing.T) {}
func TestAcceptance_T04_D(t *testing.T) {}
`

func newRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	acc := filepath.Join(root, "acceptance")
	require.NoError(t, os.MkdirAll(acc, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(acc, "criteria.yaml"), []byte(fixtureCriteria), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(acc, "a_test.go"), []byte(fixtureSource), 0o600))
	return root
}

// stackHandler simulates a healthy environment; testResult decides per test name.
func stackHandler(
	t *testing.T, root string, testResult func(name string) (harness.Result, int),
) func(runner.Cmd) (runner.Output, error) {
	t.Helper()
	return func(c runner.Cmd) (runner.Output, error) {
		if !strings.HasPrefix(c.String(), "go test") {
			return runner.Output{Stdout: "ok"}, nil
		}
		name := ""
		for i, a := range c.Args {
			if a == "-run" || a == "-bench" {
				name = strings.Trim(c.Args[i+1], "^$")
			}
		}
		res, code := testResult(name)
		if res.ID != "" {
			require.NoError(t, res.Write(filepath.Join(root, "acceptance", "logs")))
		}
		out := runner.Output{Stdout: `{"Action":"output","Output":"ran ` + name + `\n"}` + "\n", ExitCode: code}
		if code != 0 {
			return out, errBoom
		}
		return out, nil
	}
}

func options(root string, out *bytes.Buffer) runner.Options {
	now := func() time.Time { return time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC) }
	return runner.Options{Root: root, Version: "v9.9.9", Env: harness.EnvMac, Out: out, Now: now}
}

func newProber(ex runner.Exec, root string) *runner.Prober {
	return &runner.Prober{Exec: ex, Root: root, Env: harness.EnvMac, GOOS: "darwin", GOARCH: "arm64"}
}

func noEnv(string) string { return "" }

func TestRunner_FullFlow(t *testing.T) {
	t.Parallel()
	root := newRoot(t)
	fail := func(name string) (harness.Result, int) {
		id := map[string]string{
			"TestAcceptance_T01_A":      "T-01",
			"BenchmarkAcceptance_T02_B": "T-02",
			"TestAcceptance_T03_C":      "T-03",
			"TestAcceptance_T04_D":      "T-04",
		}[name]
		if id == "T-01" {
			return harness.Result{ID: id, Pass: true, Measured: map[string]any{"n": 1.0}}, 0
		}
		return harness.Result{ID: id, Pass: false, Details: "not implemented"}, 1
	}
	ex := &fakeExec{handler: stackHandler(t, root, fail)}
	var out bytes.Buffer
	rep, err := runner.New(options(root, &out), ex, newProber(ex, root)).Run(context.Background())
	require.NoError(t, err)

	byID := map[string]runner.Row{}
	for _, r := range rep.Rows {
		byID[r.ID] = r
	}
	assert.Equal(t, runner.StatusPass, byID["T-01"].Status)
	assert.Equal(t, "n=1", byID["T-01"].Measured)
	assert.Equal(t, runner.StatusFail, byID["T-02"].Status)
	assert.Equal(t, "not implemented", byID["T-02"].Detail)
	assert.Equal(t, runner.StatusFail, byID["T-03"].Status)
	assert.Equal(t, runner.StatusBlocked, byID["T-04"].Status)
	assert.Equal(t, `unknown requirement "bogus"`, byID["T-04"].Detail)
	assert.True(t, rep.Failed())
	gotIDs := make([]string, 0, len(rep.Rows))
	for _, row := range rep.Rows {
		gotIDs = append(gotIDs, row.ID)
	}
	assert.Equal(t, []string{"T-01", "T-02", "T-03", "T-04"}, gotIDs)
	assert.Empty(t, rep.Problems)

	// build from scratch, stack up, restarts, compose wake stopped for the performance criterion, final down -v.
	assert.Equal(t, 1, ex.called("go build -a ./..."))
	assert.Equal(t, 1, ex.called("make docker"))
	assert.Equal(t, 1, ex.called("docker compose -f "+filepath.Join("deploy", "docker-compose.yml")+" stop wake"))
	assert.Equal(t, 2, ex.called("docker compose -f deploy/docker-compose.yml up -d --wait")) // initial + 1 restart
	last := ex.calls[len(ex.calls)-1]
	assert.Equal(t, "docker compose -f deploy/docker-compose.yml down -v", last)
	assert.Equal(t, 0, ex.called("go test -tags acceptance -count=1 -timeout 120m -run ^TestAcceptance_T04_D$"))

	md, err := os.ReadFile(filepath.Join(root, "acceptance", "report.md"))
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(md), "# Acceptance report — wake v9.9.9 — 2026-10-18\nEnvironment: mac\n"))
	txt, err := os.ReadFile(filepath.Join(root, "acceptance", "logs", "T-01.txt"))
	require.NoError(t, err)
	assert.Equal(t, "ran TestAcceptance_T01_A\n", string(txt))
	unitCmd := "go test -tags acceptance -count=1 -timeout 120m -run ^TestAcceptance_T01_A$ -json ./acceptance/..."
	assert.Contains(t, ex.calls[indexOf(ex.calls, unitCmd)], "-json")
	benchCmd := "go test -tags acceptance -count=1 -timeout 120m -run ^$ " +
		"-bench ^BenchmarkAcceptance_T02_B$ -benchtime 1x ./acceptance/..."
	assert.GreaterOrEqual(t, indexOf(ex.calls, benchCmd), 0)
}

func indexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}

func TestRunner_ComposeFailureBlocksStackCriteria(t *testing.T) {
	t.Parallel()
	root := newRoot(t)
	pass := func(_ string) (harness.Result, int) {
		return harness.Result{ID: "T-03", Pass: true}, 0
	}
	base := stackHandler(t, root, pass)
	ex := &fakeExec{handler: func(c runner.Cmd) (runner.Output, error) {
		if strings.Contains(c.String(), " up -d --wait") {
			return runner.Output{}, errBoom
		}
		return base(c)
	}}
	opts := options(root, &bytes.Buffer{})
	opts.Only = []string{"T-01", "T-03"}
	rep, err := runner.New(opts, ex, newProber(ex, root)).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, rep.Rows, 2)
	assert.Equal(t, runner.StatusBlocked, rep.Rows[0].Status)
	assert.Contains(t, rep.Rows[0].Detail, "compose stack unavailable")
	assert.Equal(t, runner.StatusPass, rep.Rows[1].Status)
	assert.False(t, rep.Failed() && len(rep.Problems) == 0)
}

func TestRunner_ProbeBlock(t *testing.T) {
	t.Parallel()
	root := newRoot(t)
	ex := &fakeExec{handler: stackHandler(t, root, func(string) (harness.Result, int) { return harness.Result{}, 0 })}
	opts := options(root, &bytes.Buffer{})
	opts.Only = []string{"T-02", "T-03", "T-04"}
	opts.NoBuild = true
	p := newProber(ex, root)
	p.GOOS = "linux"
	rep, err := runner.New(opts, ex, p).Run(context.Background())
	require.NoError(t, err)
	byID := map[string]runner.Row{}
	for _, r := range rep.Rows {
		byID[r.ID] = r
	}
	assert.Equal(t, runner.StatusBlocked, byID["T-02"].Status)
	assert.Contains(t, byID["T-02"].Detail, "linux/arm64")
	assert.Equal(t, runner.StatusBlocked, byID["T-04"].Status)
	assert.Contains(t, byID["T-04"].Detail, "unknown requirement")
	// test exited 0 without writing a result file: FAIL
	assert.Equal(t, runner.StatusFail, byID["T-03"].Status)
	assert.Contains(t, byID["T-03"].Detail, "no result file")
	assert.Equal(t, 0, ex.called("go build"))
	assert.Equal(t, 0, ex.called("make docker"))
}

func TestRunner_CISkipAndBlockedPrefix(t *testing.T) {
	t.Parallel()
	root := newRoot(t)
	blocked := func(_ string) (harness.Result, int) {
		return harness.Result{ID: "T-01", Details: runner.BlockedPrefix + " clock offset 31ms"}, 1
	}
	ex := &fakeExec{handler: stackHandler(t, root, blocked)}
	opts := options(root, &bytes.Buffer{})
	opts.Env = harness.EnvCI
	opts.Only = []string{"T-01", "T-03"}
	p := newProber(ex, root)
	p.Env = harness.EnvCI
	rep, err := runner.New(opts, ex, p).Run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, runner.StatusBlocked, rep.Rows[0].Status)
	assert.Equal(t, "clock offset 31ms", rep.Rows[0].Detail)
	assert.Equal(t, runner.StatusSkipped, rep.Rows[1].Status)
	assert.Equal(t, "n=2", mustThreshold(t, root, "T-01"))
	assert.False(t, rep.Failed())
}

func mustThreshold(t *testing.T, root, id string) string {
	t.Helper()
	f, err := harness.Load(filepath.Join(root, "acceptance", "criteria.yaml"), harness.EnvCI)
	require.NoError(t, err)
	c, err := f.Criterion(id)
	require.NoError(t, err)
	th, err := c.Thresholds()
	require.NoError(t, err)
	return runner.FormatThresholds(th)
}

func TestRunner_BuildFailureFailsRows(t *testing.T) {
	t.Parallel()
	root := newRoot(t)
	ex := &fakeExec{handler: func(c runner.Cmd) (runner.Output, error) {
		if strings.HasPrefix(c.String(), "go build") {
			return runner.Output{}, errBoom
		}
		return runner.Output{}, nil
	}}
	opts := options(root, &bytes.Buffer{})
	opts.Only = []string{"T-01"}
	rep, err := runner.New(opts, ex, newProber(ex, root)).Run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, runner.StatusFail, rep.Rows[0].Status)
	assert.Contains(t, rep.Rows[0].Detail, "go build -a ./... failed")
	assert.Equal(t, 0, ex.called("go test"))
}

func TestRunner_KeepUpAndSourceCheckFailure(t *testing.T) {
	t.Parallel()
	root := newRoot(t)
	skipSrc := "package acceptance\nfunc TestAcceptance_T01_A(t *testing.T) { t.Skip() }\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, "acceptance", "a_test.go"), []byte(skipSrc), 0o600))
	ex := &fakeExec{handler: stackHandler(t, root, func(string) (harness.Result, int) {
		return harness.Result{ID: "T-01", Pass: true}, 0
	})}
	opts := options(root, &bytes.Buffer{})
	opts.Only = []string{"T-01"}
	opts.KeepUp = true
	rep, err := runner.New(opts, ex, newProber(ex, root)).Run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, ex.called("docker compose -f deploy/docker-compose.yml down"))
	assert.NotEmpty(t, rep.Problems)
	assert.True(t, rep.Failed())
}

func TestSelect(t *testing.T) {
	t.Parallel()
	f, err := harness.Parse([]byte(fixtureCriteria), harness.EnvMac)
	require.NoError(t, err)
	ids := func(list []*harness.Entry) []string {
		r := make([]string, 0, len(list))
		for _, c := range list {
			r = append(r, c.ID)
		}
		return r
	}
	all, err := runner.Select(f.Criteria, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"T-01", "T-02", "T-03", "T-04"}, ids(all))
	sub, err := runner.Select(f.Criteria, []string{"T-04", "T-01"})
	require.NoError(t, err)
	assert.Equal(t, []string{"T-01", "T-04"}, ids(sub))
	_, err = runner.Select(f.Criteria, []string{"NOPE"})
	require.Error(t, err)
}

func TestMain_HelpAndErrors(t *testing.T) {
	t.Parallel()
	var out, errOut bytes.Buffer
	assert.Equal(t, 0, runner.Main(context.Background(), []string{"--help"}, noEnv, &out, &errOut, &fakeExec{}))
	assert.Contains(t, errOut.String(), "--keep-up")
	assert.Equal(t, 2, runner.Main(context.Background(), []string{"--bogus"}, noEnv, &out, &errOut, &fakeExec{}))

	root := newRoot(t)
	errOut.Reset()
	args := []string{"--root", root, "--only", "NOPE"}
	code := runner.Main(context.Background(), args, noEnv, &out, &errOut, &fakeExec{})
	assert.Equal(t, 2, code)
	assert.Contains(t, errOut.String(), "unknown criterion NOPE")
}

func TestMain_OnlyFromEnvAndExitCode(t *testing.T) {
	t.Parallel()
	root := newRoot(t)
	ex := &fakeExec{handler: stackHandler(t, root, func(string) (harness.Result, int) {
		return harness.Result{ID: "T-03", Pass: true}, 0
	})}
	var out, errOut bytes.Buffer
	getenv := func(k string) string {
		if k == "ONLY" {
			return "T-03"
		}
		return ""
	}
	// source check fails (fixture has T-01 etc. mapped fine) -> pass overall
	code := runner.Main(context.Background(), []string{"--root", root, "--no-build"}, getenv, &out, &errOut, ex)
	assert.Equal(t, 0, code, errOut.String())
	assert.Contains(t, out.String(), "| T-03 | PASS |")
	assert.NotContains(t, out.String(), "T-01 |")
}

func TestTestCmd(t *testing.T) {
	t.Parallel()
	unit := runner.TestCmd("/r", &harness.Entry{Test: "TestAcceptance_X01_A"}, []string{"CI=1"})
	assert.Equal(t,
		"go test -tags acceptance -count=1 -timeout 120m -run ^TestAcceptance_X01_A$ -json ./acceptance/...",
		unit.String())
	assert.Equal(t, "/r", unit.Dir)
	bench := runner.TestCmd("/r", &harness.Entry{Test: "BenchmarkAcceptance_X02_B"}, nil)
	assert.Equal(t,
		"go test -tags acceptance -count=1 -timeout 120m -run ^$ "+
			"-bench ^BenchmarkAcceptance_X02_B$ -benchtime 1x ./acceptance/...",
		bench.String())
}
