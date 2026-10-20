// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// rowSpec says which keys of a result's "measured" map fill one table row and
// which threshold key (and comparison) decides its Pass column.
type rowSpec struct {
	metric string
	median string
	min    string
	max    string
	thr    string
	op     string // "<", "<=", ">=" or "" for no threshold
}

const (
	fieldP99    = "latency_p99_ms"
	fieldP50    = "latency_p50_ms"
	fieldMax    = "latency_max_ms"
	fieldAllocs = "allocs_per_op"
)

type section struct {
	id   string
	rows []rowSpec
	note func(measured map[string]any) string
}

var sections = []section{
	{
		id: "WAK-05",
		rows: []rowSpec{
			{"events/s", "events_per_s", "events_per_s_min_run", "events_per_s_max_run", "events_per_s_min", ">="},
			{
				"latency p99 (ms)", fieldP99, "latency_p99_ms_min_run", "latency_p99_ms_max_run",
				"latency_p99_ms_max", "<",
			},
			{"latency p50 (ms)", fieldP50, "", "", "", ""},
		},
		note: runsNote,
	},
	{
		id: "WAK-10",
		rows: []rowSpec{
			{"latency p99 (ms)", fieldP99, fieldP99, fieldP99, "latency_p99_ms_max", "<"},
			{"latency p50 (ms)", fieldP50, fieldP50, fieldP50, "", ""},
			{"latency max (ms)", fieldMax, fieldMax, fieldMax, "", ""},
		},
		note: func(m map[string]any) string {
			d, _ := toFloat(m["duration_s"])
			return fmt.Sprintf("One run of %.0f s, as the criterion fixes.", d)
		},
	},
	{
		id: "WAK-13",
		rows: []rowSpec{
			{"ns/op", "ns_per_op", "ns_per_op_min", "ns_per_op_max_run", "ns_per_op_max", "<"},
			{"allocs/op", fieldAllocs, fieldAllocs, fieldAllocs, "allocs_per_op_max", "<="},
		},
		note: runsNote,
	},
}

func runsNote(m map[string]any) string {
	n, _ := toFloat(m["runs"])
	return fmt.Sprintf("Median, min and max over %.0f runs.", n)
}

type row struct {
	metric, median, min, max, threshold, pass string
}

// buildRows fills one row per spec from the measured values and thresholds.
func buildRows(specs []rowSpec, measured map[string]any, thr map[string]float64) []row {
	rows := make([]row, 0, len(specs))
	for _, s := range specs {
		r := row{
			metric:    s.metric,
			median:    measuredCell(measured, s.median),
			min:       measuredCell(measured, s.min),
			max:       measuredCell(measured, s.max),
			threshold: "-",
			pass:      "-",
		}
		if s.op != "" {
			r.threshold, r.pass = judge(measured, s, thr)
		}
		rows = append(rows, r)
	}
	return rows
}

func measuredCell(measured map[string]any, key string) string {
	if key == "" {
		return "-"
	}
	v, ok := toFloat(measured[key])
	if !ok {
		return "-"
	}
	return formatNum(v)
}

// judge renders the threshold cell and compares the median against it.
func judge(measured map[string]any, s rowSpec, thr map[string]float64) (cell, pass string) {
	limit, ok := thr[s.thr]
	if !ok {
		return "-", "-"
	}
	cell = s.op + " " + formatNum(limit)
	v, ok := toFloat(measured[s.median])
	if !ok {
		return cell, "-"
	}
	if compare(v, s.op, limit) {
		return cell, "PASS"
	}
	return cell, "FAIL"
}

func compare(v float64, op string, limit float64) bool {
	switch op {
	case "<":
		return v < limit
	case "<=":
		return v <= limit
	case ">=":
		return v >= limit
	default:
		return false
	}
}

// formatNum prints whole numbers without decimals and other numbers with two.
func formatNum(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func renderTable(rows []row) string {
	var b strings.Builder
	b.WriteString("| Metric | Median | Min | Max | Threshold | Pass |\n|---|---|---|---|---|---|\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n", r.metric, r.median, r.min, r.max, r.threshold, r.pass)
	}
	return b.String()
}
