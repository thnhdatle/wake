// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// Main parses args and runs the protocol. It returns the process exit code:
// 0 all rows pass, skipped or blocked; 1 any FAIL or failed source check;
// 2 usage or infrastructure errors.
func Main(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer, ex Exec) int {
	fs := flag.NewFlagSet("runner", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: runner [--only ID,ID] [--version V] [--keep-up] [--no-build] [--root DIR]")
		fmt.Fprintln(stderr, "Runs the acceptance criteria of acceptance/criteria.yaml and writes acceptance/report.md.")
		fmt.Fprintln(stderr, "Environment: ONLY=ID,ID selects criteria; CI=1 selects CI thresholds.")
		fs.PrintDefaults()
	}
	only := fs.String("only", getenv("ONLY"), "comma-separated criterion ids to run (default: env ONLY, else all)")
	version := fs.String("version", "dev", "version shown in the report heading")
	keepUp := fs.Bool("keep-up", false, "leave the compose stack running at the end (iteration)")
	noBuild := fs.Bool("no-build", false, "skip the from-scratch go build and image build (iteration)")
	root := fs.String("root", ".", "repository root")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintln(stderr, "runner:", err)
		return 2
	}
	*root = absRoot

	env := harness.EnvironmentOf(getenv("CI"))
	opts := Options{Root: *root, Version: *version, KeepUp: *keepUp, NoBuild: *noBuild, Env: env, Out: stdout}
	for _, id := range strings.Split(*only, ",") {
		if id = strings.TrimSpace(id); id != "" {
			opts.Only = append(opts.Only, id)
		}
	}
	prober := &Prober{Exec: ex, Root: *root, Env: env, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH}
	rep, err := New(opts, ex, prober).Run(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "runner:", err)
		return 2
	}
	fmt.Fprint(stdout, rep.Render())
	if rep.Failed() {
		return 1
	}
	return 0
}
