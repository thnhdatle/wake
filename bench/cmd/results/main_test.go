// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatNum(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{200, "200"},
		{52.698603515625, "52.70"},
		{4999.841666666666, "4999.84"},
		{-3, "-3"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, formatNum(tt.in))
		})
	}
}

func TestCompare(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		v     float64
		op    string
		limit float64
		want  bool
	}{
		{"lt true", 1, "<", 2, true},
		{"lt equal", 2, "<", 2, false},
		{"le equal", 2, "<=", 2, true},
		{"le above", 3, "<=", 2, false},
		{"ge equal", 5000, ">=", 5000, true},
		{"ge below", 4999.8, ">=", 5000, false},
		{"unknown op", 1, "!", 2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, compare(tt.v, tt.op, tt.limit))
		})
	}
}

func TestBuildRows(t *testing.T) {
	t.Parallel()
	measured := map[string]any{
		"events_per_s": 4999.84, "events_per_s_min_run": 4999.675, "events_per_s_max_run": 5000.02,
		"latency_p99_ms": 12.0, "latency_p99_ms_min_run": 12.0, "latency_p99_ms_max_run": 12.0,
		"latency_p50_ms": 6.0,
	}
	thr := map[string]float64{"events_per_s_min": 5000, "latency_p99_ms_max": 200}
	rows := buildRows(sections[0].rows, measured, thr)
	want := []row{
		{"events/s", "4999.84", "4999.68", "5000.02", ">= 5000", "FAIL"},
		{"latency p99 (ms)", "12", "12", "12", "< 200", "PASS"},
		{"latency p50 (ms)", "6", "-", "-", "-", "-"},
	}
	assert.Equal(t, want, rows)
}

func TestBuildRowsMissingValues(t *testing.T) {
	t.Parallel()
	rows := buildRows(sections[2].rows, map[string]any{"ns_per_op": 10.0}, map[string]float64{"ns_per_op_max": 200})
	require.Len(t, rows, 2)
	assert.Equal(t, row{"ns/op", "10", "-", "-", "< 200", "PASS"}, rows[0])
	assert.Equal(t, row{"allocs/op", "-", "-", "-", "-", "-"}, rows[1])
}

func TestRenderTable(t *testing.T) {
	t.Parallel()
	got := renderTable([]row{{"m", "1", "0", "2", "< 3", "PASS"}})
	want := "| Metric | Median | Min | Max | Threshold | Pass |\n|---|---|---|---|---|---|\n" +
		"| m | 1 | 0 | 2 | < 3 | PASS |\n"
	assert.Equal(t, want, got)
}

func TestThresholds(t *testing.T) {
	t.Parallel()
	var c criterion
	c.Pass.Mac = map[string]any{"a": 5, "b": 1.5, "c": "x"}
	c.Pass.CI = map[string]any{"a": 2}
	assert.Equal(t, map[string]float64{"a": 5, "b": 1.5}, thresholds(c, false))
	assert.Equal(t, map[string]float64{"a": 2}, thresholds(c, true))
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"short", "abc", 5, "abc"},
		{"exact", "abcde", 5, "abcde"},
		{"cut", "abcdefg", 5, "abcde..."},
		{"newlines", "a\n b\t c", 10, "a b c"},
		{"runes", "ééééé", 3, "ééé..."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, truncate(tt.in, tt.n))
		})
	}
}

func TestParseBenchAndSummarize(t *testing.T) {
	t.Parallel()
	in := `goos: darwin
pkg: example.com/a
BenchmarkX-10   100  30.0 ns/op  8 B/op  1 allocs/op
BenchmarkX-10   100  10.0 ns/op  4 B/op  0 allocs/op
BenchmarkX-10   100  20.0 ns/op  6 B/op  2 allocs/op
BenchmarkNoMem/sub-8   10  5.5 ns/op
pkg: example.com/b
BenchmarkX-10   100  1.0 ns/op  0 B/op  0 allocs/op
PASS
ok  example.com/b 1.2s
BenchmarkBroken-10 abc 1 ns/op
`
	runs := parseBench(strings.NewReader(in))
	require.Len(t, runs, 5)
	got := summarize(runs)
	want := []benchSummary{
		{pkg: "example.com/a", name: "BenchmarkX", median: 20, min: 10, max: 30, medianB: 6, medianAlls: 2},
		{pkg: "example.com/a", name: "BenchmarkNoMem/sub", median: 5.5, min: 5.5, max: 5.5},
		{pkg: "example.com/b", name: "BenchmarkX", median: 1, min: 1, max: 1},
	}
	assert.Equal(t, want, got)
}

func TestParseBenchLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		line string
		ok   bool
	}{
		{"valid", "BenchmarkA-4 1 2 ns/op", true},
		{"not a benchmark", "ok pkg 1s", false},
		{"no ns/op", "BenchmarkA-4 1 2 MB/s", false},
		{"too short", "BenchmarkA-4", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ok := parseBenchLine(tt.line)
			assert.Equal(t, tt.ok, ok)
		})
	}
}

func TestRenderUnitTable(t *testing.T) {
	t.Parallel()
	got := renderUnitTable([]benchSummary{{pkg: "p", name: "BenchmarkA", median: 2, min: 1, max: 3.5, medianB: 8}})
	assert.Contains(t, got, "| BenchmarkA | p | 2 | 1 | 3.50 | 8 | 0 |")
	assert.Contains(t, renderUnitTable(nil), "(no benchmarks)")
}

func TestParseDockerInfo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		cpu  int
		mem  int64
	}{
		{"ok", "8 10737418240", 8, 10 << 30},
		{"empty", "", 0, 0},
		{"bad cpu", "x 10", 0, 0},
		{"bad mem", "8 y", 0, 0},
		{"extra", "8 1 2", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cpu, mem := parseDockerInfo(tt.in)
			assert.Equal(t, tt.cpu, cpu)
			assert.Equal(t, tt.mem, mem)
		})
	}
}

func TestFormatMachine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		m    machine
		want string
	}{
		{
			"full",
			machine{
				model: "Mac15,6", cpu: "Apple M3", memBytes: 36 << 30, macOS: "26.1",
				dockerCPU: 8, dockerMem: 10 << 30, goVersion: "go1.27.2", version: "v0.1.0",
			},
			"Mac15,6, Apple M3, 36 GB, macOS 26.1, Docker VM 8 CPU / 10 GB, Go 1.27.2, Wake v0.1.0",
		},
		{
			"minimal",
			machine{model: "linux/amd64", goVersion: "go1.27.2"},
			"linux/amd64, Go 1.27.2",
		},
		{
			"fractional docker memory",
			machine{model: "m", dockerCPU: 4, dockerMem: 7*gib + gib/2, goVersion: "go1"},
			"m, Docker VM 4 CPU / 7.5 GB, Go 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, formatMachine(tt.m))
		})
	}
}

func TestNotes(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "One run of 120 s, as the criterion fixes.", sections[1].note(map[string]any{"duration_s": 120.02}))
	assert.Equal(t, "Median, min and max over 3 runs.", runsNote(map[string]any{"runs": 3.0}))
}
