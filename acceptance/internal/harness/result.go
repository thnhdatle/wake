// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// EnvLogsDir overrides the directory result files are written to.
const EnvLogsDir = "WAKE_ACCEPTANCE_LOGS"

// Result is the content of acceptance/logs/<ID>.json.
type Result struct {
	ID       string         `json:"id"`
	Measured map[string]any `json:"measured"`
	Pass     bool           `json:"pass"`
	Details  string         `json:"details"`
}

// LogsDir returns the directory for result files: the override variable, else
// `logs` next to criteria.yaml.
func LogsDir() (string, error) {
	if d := os.Getenv(EnvLogsDir); d != "" {
		return d, nil
	}
	p, err := FindCriteria()
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(p), "logs"), nil
}

// ResultPath returns the result file path for id inside dir.
func ResultPath(dir, id string) string { return filepath.Join(dir, id+".json") }

// Write stores the result as <dir>/<ID>.json, creating dir when needed.
func (r Result) Write(dir string) error {
	if r.Measured == nil {
		r.Measured = map[string]any{}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("harness: create logs dir: %w", err)
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("harness: encode result: %w", err)
	}
	if err := os.WriteFile(ResultPath(dir, r.ID), append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("harness: write result: %w", err)
	}
	return nil
}

// ReadResult loads <dir>/<id>.json.
func ReadResult(dir, id string) (Result, error) {
	var r Result
	data, err := os.ReadFile(ResultPath(dir, id)) //nolint:gosec // dir is the acceptance logs directory
	if err != nil {
		return r, fmt.Errorf("harness: read result: %w", err)
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, fmt.Errorf("harness: decode result: %w", err)
	}
	return r, nil
}

// Record writes the result into the default logs directory and logs a test
// error when that fails.
func Record(tb testing.TB, r Result) {
	tb.Helper()
	dir, err := LogsDir()
	if err == nil {
		err = r.Write(dir)
	}
	if err != nil {
		tb.Errorf("write result for %s: %v", r.ID, err)
	}
}

// Fail records a failing result for id with msg and stops the test.
func Fail(tb testing.TB, id, msg string) {
	tb.Helper()
	Record(tb, Result{ID: id, Pass: false, Details: msg})
	tb.Fatal(id + ": " + msg)
}

// Failf is Fail with a format string.
func Failf(tb testing.TB, id, format string, args ...any) {
	tb.Helper()
	Fail(tb, id, fmt.Sprintf(format, args...))
}

// Finish records the final result of a criterion, measurements included, and
// fails the test when r.Pass is false. Use it for the threshold verdict; Fail
// is for errors that prevent a measurement.
func Finish(tb testing.TB, r Result) {
	tb.Helper()
	Record(tb, r)
	if !r.Pass {
		tb.Fatal(r.ID + ": threshold not met: " + r.Details)
	}
}
