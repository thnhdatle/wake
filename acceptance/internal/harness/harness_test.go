// SPDX-License-Identifier: Apache-2.0

package harness_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

const sample = `
project: wake
machine: "m"
criteria:
  - id: X-01
    title: t
    test: TestAcceptance_X01_A
    kind: functional
    real_deps: [postgres]
    requires: [docker]
    pass:
      mac: { rows: 1000, ratio: 0.5, outage_s: 180, latency_p99_ms_max: 200, window: 3m }
      ci: { rows: 100, outage_s: 180 }
  - id: X-02
    title: t2
    test: TestAcceptance_X02_B
    kind: functional
    real_deps: []
    requires: []
    pass:
      mac: { a: 1 }
      ci: skip
`

func TestThresholds_Getters(t *testing.T) {
	t.Parallel()
	f, err := harness.Parse([]byte(sample), harness.EnvMac)
	require.NoError(t, err)
	c, err := f.Criterion("X-01")
	require.NoError(t, err)

	tests := []struct {
		name string
		get  func() (any, error)
		want any
	}{
		{"int", func() (any, error) { return c.Int("rows") }, int64(1000)},
		{"float", func() (any, error) { return c.Float("ratio") }, 0.5},
		{"duration seconds", func() (any, error) { return c.Duration("outage_s") }, 180 * time.Second},
		{
			"duration ms with max suffix",
			func() (any, error) { return c.Duration("latency_p99_ms_max") },
			200 * time.Millisecond,
		},
		{"duration string", func() (any, error) { return c.Duration("window") }, 3 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, gerr := tc.get()
			require.NoError(t, gerr)
			assert.Equal(t, tc.want, got)
		})
	}

	_, err = c.Int("ratio")
	require.Error(t, err)
	_, err = c.Int("missing")
	require.ErrorIs(t, err, harness.ErrNotFound)
	_, err = c.Duration("rows")
	require.Error(t, err)
	_, err = f.Criterion("nope")
	require.ErrorIs(t, err, harness.ErrNotFound)
}

func TestCriterion_EnvironmentSelection(t *testing.T) {
	t.Parallel()
	f, err := harness.Parse([]byte(sample), harness.EnvCI)
	require.NoError(t, err)
	c, err := f.Criterion("X-01")
	require.NoError(t, err)
	assert.Equal(t, harness.EnvCI, c.Env())
	rows, err := c.Int("rows")
	require.NoError(t, err)
	assert.Equal(t, int64(100), rows)

	skipped, err := f.Criterion("X-02")
	require.NoError(t, err)
	assert.True(t, skipped.Pass.CISkip)
	_, err = skipped.Int("a")
	require.ErrorIs(t, err, harness.ErrSkipped)
}

func TestParse_Errors(t *testing.T) {
	t.Parallel()
	_, err := harness.Parse([]byte("criteria: [ {"), harness.EnvMac)
	require.Error(t, err)
	bad := "criteria:\n  - id: A\n    pass:\n      ci: nope\n"
	_, err = harness.Parse([]byte(bad), harness.EnvMac)
	require.Error(t, err)
}

func TestEnvironment(t *testing.T) {
	for _, tc := range []struct{ val, want string }{
		{"", harness.EnvMac}, {"0", harness.EnvMac}, {"1", harness.EnvCI}, {"true", harness.EnvCI},
	} {
		t.Run("CI="+tc.val, func(t *testing.T) {
			t.Setenv("CI", tc.val)
			assert.Equal(t, tc.want, harness.Environment())
		})
	}
}

func TestResult_WriteRead(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "logs")
	in := harness.Result{ID: "X-01", Measured: map[string]any{"rows": 5.0}, Pass: true, Details: "ok"}
	require.NoError(t, in.Write(dir))
	out, err := harness.ReadResult(dir, "X-01")
	require.NoError(t, err)
	assert.Equal(t, in, out)

	require.NoError(t, harness.Result{ID: "X-02"}.Write(dir))
	raw, err := os.ReadFile(harness.ResultPath(dir, "X-02"))
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"measured": {}`)
}

func TestFindCriteria_And_Load(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "acceptance", "sub"), 0o750))
	crit := filepath.Join(root, "acceptance", "criteria.yaml")
	require.NoError(t, os.WriteFile(crit, []byte(sample), 0o600))
	t.Chdir(filepath.Join(root, "acceptance", "sub"))

	got, err := harness.FindCriteria()
	require.NoError(t, err)
	gotReal, _ := filepath.EvalSymlinks(got)
	wantReal, _ := filepath.EvalSymlinks(crit)
	assert.Equal(t, wantReal, gotReal)

	c, err := harness.Criterion("X-01")
	require.NoError(t, err)
	assert.Equal(t, "TestAcceptance_X01_A", c.Test)

	dir, err := harness.LogsDir()
	require.NoError(t, err)
	assert.Equal(t, "logs", filepath.Base(dir))

	t.Setenv(harness.EnvLogsDir, "/x")
	dir, err = harness.LogsDir()
	require.NoError(t, err)
	assert.Equal(t, "/x", dir)

	t.Setenv(harness.EnvCriteriaPath, filepath.Join(root, "nope.yaml"))
	_, err = harness.Criterion("X-01")
	require.Error(t, err)
}

type fakeTB struct {
	testing.TB
	fatal  string
	errors []string
}

func (f *fakeTB) Helper() {}
func (f *fakeTB) Fatal(_ ...any) {
	f.fatal = "called"
}
func (f *fakeTB) Errorf(format string, _ ...any) { f.errors = append(f.errors, format) }

func TestFail_WritesFailingResult(t *testing.T) {
	logs := t.TempDir()
	t.Setenv(harness.EnvLogsDir, logs)
	ftb := &fakeTB{}
	harness.Fail(ftb, "X-09", "boom")
	assert.Equal(t, "called", ftb.fatal)
	r, err := harness.ReadResult(logs, "X-09")
	require.NoError(t, err)
	assert.False(t, r.Pass)
	assert.Equal(t, "boom", r.Details)

	harness.Failf(ftb, "X-10", "n=%d", 3)
	r, err = harness.ReadResult(logs, "X-10")
	require.NoError(t, err)
	assert.Equal(t, "n=3", r.Details)
}

func TestFinish_KeepsMeasurementsAndFailsOnlyWhenNotPassing(t *testing.T) {
	logs := t.TempDir()
	t.Setenv(harness.EnvLogsDir, logs)
	tests := []struct {
		name      string
		pass      bool
		wantFatal string
	}{
		{name: "pass records and continues", pass: true, wantFatal: ""},
		{name: "fail records and stops", pass: false, wantFatal: "called"},
	}
	// Cases run in sequence (no t.Run): t.Setenv forbids parallel subtests
	// and every case writes the same result file.
	for _, tc := range tests {
		ftb := &fakeTB{}
		in := harness.Result{ID: "X-11", Measured: map[string]any{"lost": 2.0}, Pass: tc.pass, Details: tc.name}

		harness.Finish(ftb, in)

		assert.Equal(t, tc.wantFatal, ftb.fatal, tc.name)
		out, err := harness.ReadResult(logs, "X-11")
		require.NoError(t, err, tc.name)
		assert.Equal(t, in, out, "%s: measurements are kept in the result file", tc.name)
	}
}
