// SPDX-License-Identifier: Apache-2.0

// Command results writes bench/RESULTS.md from the acceptance
// benchmark logs (acceptance/logs/WAK-NN.json), the unit benchmark output
// (out/bench/unit.txt) and the thresholds in acceptance/criteria.yaml. It is run by
// scripts/bench.sh, which `make bench` calls.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type config struct {
	out      string
	criteria string
	notMet   string
	logs     string
	unit     string
	version  string
	ci       bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "results:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config{ci: os.Getenv("CI") != ""}
	flag.StringVar(&cfg.out, "out", "bench/RESULTS.md", "output Markdown file")
	flag.StringVar(&cfg.criteria, "criteria", "acceptance/criteria.yaml", "criteria file")
	flag.StringVar(&cfg.notMet, "not-met", "acceptance/not-met.yaml", "not-met file (optional)")
	flag.StringVar(&cfg.logs, "logs", "acceptance/logs", "directory with WAK-NN.json files")
	flag.StringVar(&cfg.unit, "unit", "out/bench/unit.txt", "go test -bench output of the unit benchmarks")
	flag.StringVar(&cfg.version, "version", "dev", "Wake version shown on every Machine line")
	flag.Parse()

	var buf bytes.Buffer
	if err := generate(&buf, cfg, collectMachine(cfg.version)); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.out), 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.WriteFile(cfg.out, buf.Bytes(), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", cfg.out, err)
	}
	fmt.Println("results: wrote", cfg.out)
	return nil
}

const header = `# Benchmark results

Reproduce with ` + "`make bench`" + ` (then ` + "`make down`" + ` to remove the compose dependencies). For the
performance criteria Wake runs natively on the host against the compose dependencies; the unit
benchmarks run with ` + "`go test -bench . -benchmem -count 3 ./...`" + `. The WAK-05, WAK-10 and WAK-13
measurements are defined in ` + "`acceptance/criteria.yaml`" + ` (M6 and M7 for the load shape and the warm-up);
thresholds below are the ` + "%s" + ` ones.
`

func generate(w *bytes.Buffer, cfg config, m machine) error {
	crits, err := loadCriteria(cfg.criteria)
	if err != nil {
		return err
	}
	notMet, err := loadNotMet(cfg.notMet)
	if err != nil {
		return err
	}
	profile := "Mac"
	if cfg.ci {
		profile = "CI"
	}
	fmt.Fprintf(w, header, profile)
	machineLine := formatMachine(m)
	for _, sec := range sections {
		if err := writeCriterion(w, cfg, sec, crits, notMet, machineLine); err != nil {
			return err
		}
	}
	return writeUnit(w, cfg.unit, machineLine)
}

func writeCriterion(w *bytes.Buffer, cfg config, sec section, crits map[string]criterion,
	notMet map[string]string, machineLine string,
) error {
	c, ok := crits[sec.id]
	if !ok {
		return fmt.Errorf("criterion %s not found in %s", sec.id, cfg.criteria)
	}
	res, date, found, err := loadResult(filepath.Join(cfg.logs, sec.id+".json"))
	if err != nil {
		return err
	}
	if !found {
		fmt.Fprintf(w, "\n## %s %s\n\nNot run: %s has no result file.\n", sec.id, c.Title,
			filepath.Join(cfg.logs, sec.id+".json"))
		return nil
	}
	fmt.Fprintf(w, "\n## %s %s — %s\nMachine: %s\n", sec.id, c.Title, date, machineLine)
	rows := buildRows(sec.rows, res.Measured, thresholds(c, cfg.ci))
	w.WriteString("\n" + renderTable(rows))
	if sec.note != nil {
		fmt.Fprintf(w, "\n%s\n", sec.note(res.Measured))
	}
	fmt.Fprintf(w, "\nResult: %s\n", verdict(res.Pass))
	if cause, ok := notMet[sec.id]; ok && !res.Pass {
		fmt.Fprintf(w, "\nNot met: %s\n", cause)
	}
	if res.Details != "" {
		fmt.Fprintf(w, "\nDetails: %s\n", truncate(res.Details, detailsMax))
	}
	return nil
}

func verdict(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}

func writeUnit(w *bytes.Buffer, path, machineLine string) error {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from the -unit flag of this tool
	if os.IsNotExist(err) {
		fmt.Fprintf(w, "\n## Unit benchmarks\n\nNot run: %s does not exist.\n", path)
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	date := time.Now().Format(dateLayout)
	if st, serr := os.Stat(path); serr == nil {
		date = st.ModTime().Format(dateLayout)
	}
	sums := summarize(parseBench(bytes.NewReader(data)))
	fmt.Fprintf(w, "\n## Unit benchmarks — %s\nMachine: %s\n\n", date, machineLine)
	w.WriteString(renderUnitTable(sums))
	return nil
}
