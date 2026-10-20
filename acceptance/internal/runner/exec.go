// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Cmd is one external command.
type Cmd struct {
	Dir  string
	Env  []string // extra KEY=VALUE entries, appended to the process environment
	Name string
	Args []string
}

// String renders the command for logs and reports.
func (c Cmd) String() string {
	return strings.TrimSpace(c.Name + " " + strings.Join(c.Args, " "))
}

// Output is what a command wrote and how it ended.
type Output struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// Exec runs external commands. A non-zero exit is reported as a non-nil error
// together with the captured Output.
type Exec interface {
	Run(ctx context.Context, c Cmd) (Output, error)
}

// OSExec runs commands with os/exec.
type OSExec struct{}

// Run implements Exec.
func (OSExec) Run(ctx context.Context, c Cmd) (Output, error) {
	cmd := exec.CommandContext(ctx, c.Name, c.Args...) //nolint:gosec // commands are fixed by the runner protocol
	cmd.Dir = c.Dir
	cmd.Env = append(os.Environ(), c.Env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	out := Output{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			out.ExitCode = ee.ExitCode()
		} else {
			out.ExitCode = -1
		}
		return out, fmt.Errorf("runner: %s: %w", c, err)
	}
	return out, nil
}
