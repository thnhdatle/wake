// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// benchRun is one result line of `go test -bench -benchmem`.
type benchRun struct {
	pkg    string
	name   string
	nsOp   float64
	bOp    float64
	allocs float64
}

type benchSummary struct {
	pkg, name           string
	median, min, max    float64
	medianB, medianAlls float64
}

var gomaxprocsSuffix = regexp.MustCompile(`-\d+$`)

// parseBench reads go test -bench output; "pkg:" lines set the package of the lines after them.
func parseBench(r io.Reader) []benchRun {
	var runs []benchRun
	pkg := ""
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if rest, ok := strings.CutPrefix(line, "pkg:"); ok {
			pkg = strings.TrimSpace(rest)
			continue
		}
		if br, ok := parseBenchLine(line); ok {
			br.pkg = pkg
			runs = append(runs, br)
		}
	}
	return runs
}

// parseBenchLine parses "BenchmarkX-10  1000  12.3 ns/op  8 B/op  1 allocs/op".
func parseBenchLine(line string) (benchRun, bool) {
	f := strings.Fields(line)
	if len(f) < 4 || !strings.HasPrefix(f[0], "Benchmark") {
		return benchRun{}, false
	}
	if _, err := strconv.ParseInt(f[1], 10, 64); err != nil {
		return benchRun{}, false
	}
	br := benchRun{name: gomaxprocsSuffix.ReplaceAllString(f[0], "")}
	seen := false
	for i := 2; i+1 < len(f); i += 2 {
		v, err := strconv.ParseFloat(f[i], 64)
		if err != nil {
			continue
		}
		switch f[i+1] {
		case "ns/op":
			br.nsOp, seen = v, true
		case "B/op":
			br.bOp = v
		case "allocs/op":
			br.allocs = v
		}
	}
	return br, seen
}

// summarize groups runs by (package, name) in order of first appearance; the median is the
// lower median run by ns/op, and B/op and allocs/op are those of that run.
func summarize(runs []benchRun) []benchSummary {
	type key struct{ pkg, name string }
	groups := map[key][]benchRun{}
	var order []key
	for _, r := range runs {
		k := key{r.pkg, r.name}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	out := make([]benchSummary, 0, len(order))
	for _, k := range order {
		g := append([]benchRun(nil), groups[k]...)
		sort.SliceStable(g, func(i, j int) bool { return g[i].nsOp < g[j].nsOp })
		med := g[(len(g)-1)/2]
		out = append(out, benchSummary{
			pkg: k.pkg, name: k.name,
			median: med.nsOp, min: g[0].nsOp, max: g[len(g)-1].nsOp,
			medianB: med.bOp, medianAlls: med.allocs,
		})
	}
	return out
}

func renderUnitTable(sums []benchSummary) string {
	var b strings.Builder
	b.WriteString("| Benchmark | Package | Median ns/op | Min | Max | B/op | allocs/op |\n|---|---|---|---|---|---|---|\n")
	for _, s := range sums {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n", s.name, s.pkg,
			formatNum(s.median), formatNum(s.min), formatNum(s.max), formatNum(s.medianB), formatNum(s.medianAlls))
	}
	if len(sums) == 0 {
		b.WriteString("| (no benchmarks) | | | | | | |\n")
	}
	return b.String()
}
