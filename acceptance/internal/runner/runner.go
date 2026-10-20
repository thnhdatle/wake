// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// Options configure a run.
type Options struct {
	Root     string   // repository root
	Version  string   // version shown in the report heading
	Only     []string // criterion ids to run; empty means all
	KeepUp   bool     // leave the compose stack running at the end
	NoBuild  bool     // skip the from-scratch build (iteration only)
	Env      string   // harness.EnvMac or harness.EnvCI
	Out      io.Writer
	Now      func() time.Time
	Compose  string // compose file relative to Root
	TestWait time.Duration
}

// KindPerformance is the criteria.yaml kind of criteria that measure speed on
// the reference machine.
const KindPerformance = "performance"

// BlockedPrefix marks a failed result whose details start with it as an
// environment problem the test detected itself (for example the WAK-05 clock
// offset above 20 ms); the row becomes Blocked instead of Fail.
const BlockedPrefix = "BLOCKED:"

// Runner executes the protocol.
type Runner struct {
	Opts   Options
	Exec   Exec
	Prober *Prober

	composeUp  bool // the stack was started by this run
	stackErr   string
	fresh      bool
	probeCache map[string]ProbeResult
}

// New builds a Runner; Prober may be replaced in tests.
func New(opts Options, ex Exec, p *Prober) *Runner {
	if opts.Out == nil {
		opts.Out = io.Discard
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}
	if opts.Compose == "" {
		opts.Compose = filepath.Join("deploy", "docker-compose.yml")
	}
	return &Runner{Opts: opts, Exec: ex, Prober: p, probeCache: map[string]ProbeResult{}}
}

func (r *Runner) logf(format string, args ...any) {
	fmt.Fprintf(r.Opts.Out, format+"\n", args...)
}

func (r *Runner) accDir() string  { return filepath.Join(r.Opts.Root, "acceptance") }
func (r *Runner) logsDir() string { return filepath.Join(r.accDir(), "logs") }

func (r *Runner) run(ctx context.Context, name string, args ...string) (Output, error) {
	return r.Exec.Run(ctx, Cmd{Dir: r.Opts.Root, Name: name, Args: args})
}

func (r *Runner) compose(ctx context.Context, args ...string) (Output, error) {
	return r.run(ctx, "docker", append([]string{"compose", "-f", r.Opts.Compose}, args...)...)
}

// Select returns the criteria to run, in file order.
func Select(all []*harness.Entry, only []string) ([]*harness.Entry, error) {
	if len(only) == 0 {
		return slices.Clone(all), nil
	}
	for _, id := range only {
		if !slices.ContainsFunc(all, func(c *harness.Entry) bool { return c.ID == id }) {
			return nil, fmt.Errorf("runner: unknown criterion %s in ONLY", id)
		}
	}
	var picked []*harness.Entry
	for _, c := range all {
		if slices.Contains(only, c.ID) {
			picked = append(picked, c)
		}
	}
	return picked, nil
}

// usesStack reports whether the criterion needs the compose stack.
func usesStack(c *harness.Entry) bool {
	return len(c.RealDeps) > 0 || slices.Contains(c.Requires, "docker")
}

// Run executes the whole protocol and returns the report (already written).
func (r *Runner) Run(ctx context.Context) (Report, error) {
	path := filepath.Join(r.accDir(), "criteria.yaml")
	file, err := harness.Load(path, r.Opts.Env)
	if err != nil {
		return Report{}, fmt.Errorf("runner: %w", err)
	}
	picked, err := Select(file.Criteria, r.Opts.Only)
	if err != nil {
		return Report{}, err
	}
	if mkErr := os.MkdirAll(r.logsDir(), 0o750); mkErr != nil {
		return Report{}, fmt.Errorf("runner: create logs dir: %w", mkErr)
	}

	rep := Report{
		Project: file.Project,
		Env:     r.Opts.Env,
		Date:    r.Opts.Now().UTC().Format("2006-01-02"),
		Version: r.Opts.Version,
	}
	buildErr := r.build(ctx, picked)
	rows := map[string]Row{}
	for _, c := range picked {
		rows[c.ID] = r.runCriterion(ctx, c, buildErr)
	}
	if !r.Opts.KeepUp && r.composeUp {
		r.logf("compose down -v")
		if _, downErr := r.compose(context.WithoutCancel(ctx), "down", "-v"); downErr != nil {
			r.logf("warning: %v", downErr)
		}
	}
	for _, c := range file.Criteria {
		if row, ok := rows[c.ID]; ok {
			rep.Rows = append(rep.Rows, row)
		}
	}

	nm, err := r.readNotMet()
	if err != nil {
		return rep, err
	}
	rep.NotMet = nm
	mismatches, err := CheckCriteria(r.accDir(), file.Criteria)
	if err != nil {
		return rep, err
	}
	skips, err := CheckSkips(r.accDir())
	if err != nil {
		return rep, err
	}
	rep.Problems = slices.Concat(mismatches, skips)

	if wErr := os.WriteFile(filepath.Join(r.accDir(), "report.md"), []byte(rep.Render()), 0o600); wErr != nil {
		return rep, fmt.Errorf("runner: write report: %w", wErr)
	}
	return rep, nil
}

func (r *Runner) readNotMet() ([]NotMet, error) {
	data, err := os.ReadFile(filepath.Join(r.accDir(), "not-met.yaml")) //nolint:gosec // repo file
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("runner: read not-met.yaml: %w", err)
	}
	return ParseNotMet(data)
}

// build compiles everything from scratch and starts the stack. A Go build
// failure is returned (every row fails); image and compose failures are kept in
// stackErr so only criteria needing the stack are Blocked.
func (r *Runner) build(ctx context.Context, picked []*harness.Entry) error {
	if !r.Opts.NoBuild {
		r.logf("go build -a ./...")
		if _, err := r.run(ctx, "go", "build", "-a", "./..."); err != nil {
			return err
		}
	}
	if slices.ContainsFunc(picked, usesStack) {
		r.startStack(ctx)
	}
	return nil
}

// startStack builds the image and brings the stack up, recording any failure
// in stackErr.
func (r *Runner) startStack(ctx context.Context) {
	if !r.Opts.NoBuild {
		r.logf("make docker")
		if _, err := r.run(ctx, "make", "docker"); err != nil {
			r.stackErr = err.Error()
			return
		}
	}
	r.up(ctx)
}

func (r *Runner) up(ctx context.Context) {
	r.logf("compose up -d --wait")
	r.composeUp = true
	if _, err := r.compose(ctx, "up", "-d", "--wait"); err != nil {
		r.stackErr = err.Error()
		return
	}
	r.stackErr = ""
	r.fresh = true
}

func (r *Runner) restart(ctx context.Context) {
	r.logf("compose down -v; up -d --wait")
	r.composeUp = true
	if _, err := r.compose(ctx, "down", "-v"); err != nil {
		r.stackErr = err.Error()
		return
	}
	r.up(ctx)
}

func (r *Runner) probe(ctx context.Context, req string) ProbeResult {
	if res, ok := r.probeCache[req]; ok {
		return res
	}
	res := r.Prober.Probe(ctx, req)
	r.probeCache[req] = res
	return res
}

func (r *Runner) runCriterion(ctx context.Context, c *harness.Entry, buildErr error) Row {
	row := Row{ID: c.ID}
	th, thErr := c.Thresholds()
	row.Threshold = FormatThresholds(th)
	if errors.Is(thErr, harness.ErrSkipped) {
		row.Status, row.Detail, row.Threshold = StatusSkipped, "ci: skip in criteria.yaml", "skip"
		return row
	}
	if buildErr != nil {
		row.Status, row.Detail = StatusFail, "go build -a ./... failed: "+buildErr.Error()
		return row
	}
	r.logf("== %s %s", c.ID, c.Test)

	for _, req := range c.Requires {
		if res := r.probe(ctx, req); !res.OK {
			return blocked(row, res.Detail)
		}
	}
	if usesStack(c) {
		if why := r.prepareStack(ctx, c); why != "" {
			return blocked(row, why)
		}
	}
	return r.execute(ctx, c, row)
}

// prepareStack makes the compose stack ready for c. It returns a non-empty
// reason when the criterion must be Blocked.
func (r *Runner) prepareStack(ctx context.Context, c *harness.Entry) string {
	if r.stackErr != "" {
		return "compose stack unavailable: " + r.stackErr
	}
	if !r.fresh {
		r.restart(ctx)
		if r.stackErr != "" {
			return "compose restart: " + r.stackErr
		}
	}
	// Performance criteria measure a Wake the test starts natively on the
	// host, so the compose Wake must not compete for the slot or the CPU.
	if c.Kind == KindPerformance {
		r.logf("compose stop wake")
		if _, err := r.compose(ctx, "stop", "wake"); err != nil {
			return "compose stop wake: " + err.Error()
		}
	}
	r.fresh = false
	return ""
}

func blocked(row Row, detail string) Row {
	row.Status, row.Detail = StatusBlocked, detail
	return row
}

// TestCmd builds the go test invocation for a criterion: no test cache, one
// iteration for benchmarks, JSON output for tests.
func TestCmd(root string, c *harness.Entry, env []string) Cmd {
	args := []string{"test", "-tags", "acceptance", "-count=1", "-timeout", "120m"}
	if strings.HasPrefix(c.Test, "Benchmark") {
		args = append(args, "-run", "^$", "-bench", "^"+c.Test+"$", "-benchtime", "1x")
	} else {
		args = append(args, "-run", "^"+c.Test+"$", "-json")
	}
	args = append(args, "./acceptance/...")
	return Cmd{Dir: root, Env: env, Name: "go", Args: args}
}

func (r *Runner) execute(ctx context.Context, c *harness.Entry, row Row) Row {
	jsonPath := harness.ResultPath(r.logsDir(), c.ID)
	_ = os.Remove(jsonPath)
	env := []string{
		harness.EnvCriteriaPath + "=" + filepath.Join(r.accDir(), "criteria.yaml"),
		harness.EnvLogsDir + "=" + r.logsDir(),
		"CI=" + map[string]string{harness.EnvCI: "1", harness.EnvMac: ""}[r.Opts.Env],
	}
	out, runErr := r.Exec.Run(ctx, TestCmd(r.Opts.Root, c, env))
	text := ReadableTestOutput(out.Stdout) + out.Stderr
	logPath := filepath.Join(r.logsDir(), c.ID+".txt")
	if err := os.WriteFile(logPath, []byte(text), 0o600); err != nil {
		return blocked(row, "write log: "+err.Error())
	}

	res, resErr := harness.ReadResult(r.logsDir(), c.ID)
	if resErr == nil {
		row.Measured = FormatMeasured(res.Measured)
	}
	switch {
	case resErr == nil && !res.Pass && strings.HasPrefix(res.Details, BlockedPrefix):
		return blocked(row, strings.TrimSpace(strings.TrimPrefix(res.Details, BlockedPrefix)))
	case runErr != nil:
		row.Status = StatusFail
		row.Detail = failDetail(res, resErr, runErr.Error())
	case resErr != nil:
		row.Status, row.Detail = StatusFail, "test exited 0 but wrote no result file: "+resErr.Error()
	case !res.Pass:
		row.Status, row.Detail = StatusFail, res.Details
	default:
		row.Status = StatusPass
	}
	return row
}

func failDetail(res harness.Result, resErr error, runErr string) string {
	if resErr == nil && res.Details != "" {
		return res.Details
	}
	return runErr
}

// FormatMeasured renders measurements as sorted `key=value` pairs.
func FormatMeasured(m map[string]any) string {
	return FormatThresholds(harness.Thresholds(m))
}

// ReadableTestOutput turns `go test -json` output into plain text; lines that
// are not JSON events are kept as they are.
func ReadableTestOutput(out string) string {
	var b strings.Builder
	for _, line := range strings.Split(out, "\n") {
		if line == "" {
			continue
		}
		var ev struct{ Output string }
		if json.Unmarshal([]byte(line), &ev) == nil && strings.HasPrefix(line, "{") {
			b.WriteString(ev.Output)
			continue
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}
