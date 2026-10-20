// SPDX-License-Identifier: Apache-2.0

package runner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/runner"
)

func TestReport_Render(t *testing.T) {
	t.Parallel()
	rep := runner.Report{
		Project: "wake", Version: "v0.1.0", Date: "2026-10-18", Env: "mac",
		Rows: []runner.Row{
			{ID: "WAK-01", Status: runner.StatusFail, Measured: "a=1", Threshold: "rows=5", Detail: "not implemented"},
			{ID: "WAK-02", Status: runner.StatusPass, Measured: "x|y", Threshold: "t"},
			{ID: "WAK-03", Status: runner.StatusSkipped, Detail: "ci: skip in criteria.yaml"},
			{ID: "WAK-04", Status: runner.StatusBlocked, Detail: "docker info: boom"},
			{ID: "WAK-05", Status: runner.StatusFail, Detail: "measured 4999"},
		},
		NotMet: []runner.NotMet{
			{ID: "WAK-02", Cause: "passing rows print no note"},
			{ID: "WAK-05", Cause: "offered load\n  equals the threshold"},
		},
	}
	want := `# Acceptance report — wake v0.1.0 — 2026-10-18
Environment: mac

| ID | Result | Measured | Threshold |
|---|---|---|---|
| WAK-01 | FAIL | a=1 | rows=5 |
| WAK-02 | PASS | x\|y | t |
| WAK-03 | SKIPPED |  |  |
| WAK-04 | BLOCKED |  |  |
| WAK-05 | FAIL |  |  |

## Not met
- WAK-01: not implemented
- WAK-05: offered load equals the threshold

## Skipped (environment)
- WAK-03: ci: skip in criteria.yaml

## Blocked (environment)
- WAK-04: docker info: boom
`
	assert.Equal(t, want, rep.Render())
	assert.True(t, rep.Failed())
}

func TestReport_EmptySectionsAndProblems(t *testing.T) {
	t.Parallel()
	rep := runner.Report{Project: "wake", Rows: []runner.Row{{ID: "A", Status: runner.StatusPass}}}
	out := rep.Render()
	assert.Contains(t, out, "## Not met\n- none\n")
	assert.Contains(t, out, "## Skipped (environment)\n- none\n")
	assert.NotContains(t, out, "External images")
	assert.NotContains(t, out, "Source checks")
	assert.False(t, rep.Failed())

	rep.Problems = []string{"a_test.go:3: forbidden \"t.Skip\""}
	assert.True(t, rep.Failed())
	assert.Contains(t, rep.Render(), "## Source checks (FAIL)\n- a_test.go:3")
}

func TestParseNotMet(t *testing.T) {
	t.Parallel()
	list, err := runner.ParseNotMet([]byte("[]\n"))
	require.NoError(t, err)
	assert.Empty(t, list)
	list, err = runner.ParseNotMet([]byte("- {id: A, cause: c}\n"))
	require.NoError(t, err)
	assert.Equal(t, []runner.NotMet{{ID: "A", Cause: "c"}}, list)
	_, err = runner.ParseNotMet([]byte("{"))
	require.Error(t, err)
}

func TestFormatThresholds(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "a=1 b=2.5", runner.FormatThresholds(harness.Thresholds{"b": 2.5, "a": 1}))
}

func TestReadableTestOutput(t *testing.T) {
	t.Parallel()
	in := `{"Action":"output","Output":"--- FAIL: X\n"}` + "\n" + "plain line\n" + `{"Action":"fail"}` + "\n"
	assert.Equal(t, "--- FAIL: X\nplain line\n", runner.ReadableTestOutput(in))
}
