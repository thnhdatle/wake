// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
)

// errCheckFailed reports that `wake check` found a failing check.
var errCheckFailed = errors.New("pre-flight checks failed")

func newCheckCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Run pre-flight checks against the source",
		Long: "Run the pre-flight checks of `wake run` against the source without changing anything:\n" +
			"server version, wal_level, replication privilege (IDENTIFY_SYSTEM), free slots and WAL senders,\n" +
			"publications and CREATE/ownership (managed) or publish_via_partition_root (existing), keyless\n" +
			"tables, identifiers and topic lengths, WAL retention and status_interval.\n" +
			"Exits 1 when a check fails.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(*cfgPath, false)
			if err != nil {
				return err
			}
			dsn, err := sourceDSN(&cfg)
			if err != nil {
				return err
			}
			r, err := newResolver(&cfg)
			if err != nil {
				return &exitError{code: exitConfig, err: err}
			}
			src, err := openSource(cmd.Context(), &cfg, dsn, newCLILogger())
			if err != nil {
				return err
			}
			defer func() { _ = src.close(cmd.Context()) }()
			report, err := runChecks(cmd.Context(), src, &cfg, r)
			if err != nil {
				return err
			}
			printReport(cmd.OutOrStdout(), &report)
			if report.Failed() {
				return &exitError{code: exitFailure, err: &findingsError{failed: failures(&report)}}
			}
			return nil
		},
	}
}

// printReport writes one line per finding, then its tables indented.
func printReport(w io.Writer, r *pgsql.Report) {
	for _, f := range r.Findings {
		code := ""
		if f.Code != "" {
			code = " [" + f.Code + "]"
		}
		fmt.Fprintf(w, "%-4s  %-22s %s%s\n", strings.ToUpper(string(f.Status)), f.Check, f.Message, code)
		for _, t := range f.Tables {
			fmt.Fprintf(w, "      %-22s - %s\n", "", t)
		}
	}
	if r.Failed() {
		fmt.Fprintln(w, "result: FAILED (remediation per code: docs/guides/errors.md)")
		return
	}
	fmt.Fprintln(w, "result: OK")
}

func failures(r *pgsql.Report) []pgsql.Finding {
	var out []pgsql.Finding
	for _, f := range r.Findings {
		if f.Status == pgsql.StatusFail {
			out = append(out, f)
		}
	}
	if len(out) == 0 {
		out = append(out, pgsql.Finding{Check: "check", Code: "", Message: errCheckFailed.Error()})
	}
	return out
}
