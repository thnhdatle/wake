// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// Row is one criterion line of the report.
type Row struct {
	ID        string
	Status    Status
	Measured  string
	Threshold string
	Detail    string // skip probe result, blocking command or failure cause
}

// NotMet is an entry of acceptance/not-met.yaml: the explanation printed
// under "Not met" when the criterion fails.
type NotMet struct {
	ID    string `yaml:"id"`
	Cause string `yaml:"cause"`
}

// Report is everything acceptance/report.md renders.
type Report struct {
	Project  string
	Version  string
	Date     string
	Env      string
	Rows     []Row
	NotMet   []NotMet
	Problems []string // criteria check and skip check failures
}

// ParseNotMet decodes not-met.yaml; empty input is an empty list.
func ParseNotMet(data []byte) ([]NotMet, error) {
	var list []NotMet
	if err := yaml.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("runner: parse not-met.yaml: %w", err)
	}
	return list, nil
}

// Failed reports whether the run must exit non-zero.
func (r Report) Failed() bool {
	if len(r.Problems) > 0 {
		return true
	}
	for _, row := range r.Rows {
		if row.Status == StatusFail {
			return true
		}
	}
	return false
}

// FormatThresholds renders thresholds as sorted `key=value` pairs.
func FormatThresholds(t harness.Thresholds) string {
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, t[k]))
	}
	return strings.Join(parts, " ")
}

func cell(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	return strings.ReplaceAll(s, "\n", " ")
}

// Render produces the report markdown described in acceptance/README.md.
func (r Report) Render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Acceptance report — %s %s — %s\n", r.Project, r.Version, r.Date)
	fmt.Fprintf(&b, "Environment: %s\n\n", r.Env)
	b.WriteString("| ID | Result | Measured | Threshold |\n|---|---|---|---|\n")
	for _, row := range r.Rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n",
			row.ID, row.Status, cell(row.Measured), cell(row.Threshold))
	}

	b.WriteString("\n## Not met\n")
	causes := map[string]string{}
	for _, n := range r.NotMet {
		causes[n.ID] = strings.Join(strings.Fields(n.Cause), " ")
	}
	none := true
	for _, row := range r.Rows {
		if row.Status != StatusFail {
			continue
		}
		none = false
		if cause, ok := causes[row.ID]; ok {
			fmt.Fprintf(&b, "- %s: %s\n", row.ID, cause)
		} else {
			fmt.Fprintf(&b, "- %s: %s\n", row.ID, row.Detail)
		}
	}
	if none {
		b.WriteString("- none\n")
	}

	r.section(&b, "Skipped (environment)", StatusSkipped)
	r.section(&b, "Blocked (environment)", StatusBlocked)

	if len(r.Problems) > 0 {
		b.WriteString("\n## Source checks (FAIL)\n")
		for _, p := range r.Problems {
			fmt.Fprintf(&b, "- %s\n", p)
		}
	}
	return b.String()
}

func (r Report) section(b *strings.Builder, title string, st Status) {
	fmt.Fprintf(b, "\n## %s\n", title)
	n := 0
	for _, row := range r.Rows {
		if row.Status == st {
			fmt.Fprintf(b, "- %s: %s\n", row.ID, row.Detail)
			n++
		}
	}
	if n == 0 {
		b.WriteString("- none\n")
	}
}
