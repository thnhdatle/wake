// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// forbiddenPatterns are skip constructs that acceptance sources never use: a
// criterion whose environment is missing is reported as Skipped or Blocked by
// the runner, never skipped by the test itself.
var forbiddenPatterns = []string{
	"t.Skip", "b.Skip", ".SkipNow", "testing.Short", "pytest.mark.skip", "pytest.skip", "xfail",
}

var acceptanceFunc = regexp.MustCompile(`(?m)^func ((?:Test|Benchmark)Acceptance_\w+)\(`)

// sourceFiles lists the Go files directly inside the acceptance directory.
func sourceFiles(acceptanceDir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(acceptanceDir, "*.go"))
	if err != nil {
		return nil, fmt.Errorf("runner: glob acceptance sources: %w", err)
	}
	sort.Strings(files)
	return files, nil
}

// CheckCriteria verifies that every criterion has exactly one test function
// with its test name, that the name carries the id, and that no acceptance
// test exists without a criterion. It returns one message per problem.
func CheckCriteria(acceptanceDir string, criteria []*harness.Entry) ([]string, error) {
	files, err := sourceFiles(acceptanceDir)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // repo source files
		if err != nil {
			return nil, fmt.Errorf("runner: read %s: %w", f, err)
		}
		for _, m := range acceptanceFunc.FindAllStringSubmatch(string(data), -1) {
			counts[m[1]]++
		}
	}
	var problems []string
	known := map[string]bool{}
	for _, c := range criteria {
		known[c.Test] = true
		if !strings.Contains(c.Test, strings.ReplaceAll(c.ID, "-", "")) {
			problems = append(problems, fmt.Sprintf("%s: test name %s does not contain the id", c.ID, c.Test))
		}
		if n := counts[c.Test]; n != 1 {
			problems = append(problems, fmt.Sprintf("%s: expected exactly one test named %s, found %d", c.ID, c.Test, n))
		}
	}
	var orphans []string
	for name := range counts {
		if !known[name] {
			orphans = append(orphans, name)
		}
	}
	sort.Strings(orphans)
	for _, name := range orphans {
		problems = append(problems, name+" has no criterion in criteria.yaml")
	}
	return problems, nil
}

// CheckSkips greps the acceptance sources for skip constructs; every hit is a
// problem.
func CheckSkips(acceptanceDir string) ([]string, error) {
	files, err := sourceFiles(acceptanceDir)
	if err != nil {
		return nil, err
	}
	var problems []string
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // repo source files
		if err != nil {
			return nil, fmt.Errorf("runner: read %s: %w", f, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			for _, pat := range forbiddenPatterns {
				if strings.Contains(line, pat) {
					problems = append(problems, fmt.Sprintf("%s:%d: forbidden %q", filepath.Base(f), i+1, pat))
					break
				}
			}
		}
	}
	return problems, nil
}
