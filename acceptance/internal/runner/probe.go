// SPDX-License-Identifier: Apache-2.0

package runner

import (
	"context"
	"fmt"
	"strings"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// Status is the outcome of a criterion row.
type Status string

// Row outcomes.
const (
	StatusPass    Status = "PASS"
	StatusFail    Status = "FAIL"
	StatusSkipped Status = "SKIPPED"
	StatusBlocked Status = "BLOCKED"
)

// ProbeResult is the outcome of probing one `requires` entry; a requirement
// that is not met blocks the criterion.
type ProbeResult struct {
	OK     bool
	Detail string
}

// Prober checks environment capabilities.
type Prober struct {
	Exec   Exec
	Root   string
	Env    string // harness.EnvMac or harness.EnvCI
	GOOS   string
	GOARCH string
}

func ok(detail string) ProbeResult { return ProbeResult{OK: true, Detail: detail} }

func missing(format string, args ...any) ProbeResult {
	return ProbeResult{Detail: fmt.Sprintf(format, args...)}
}

// Probe checks a single `requires` entry.
func (p *Prober) Probe(ctx context.Context, req string) ProbeResult {
	switch req {
	case "docker":
		return p.tool(ctx, "docker info", "docker", "info")
	case "mac":
		return p.mac()
	default:
		return missing("unknown requirement %q", req)
	}
}

func (p *Prober) tool(ctx context.Context, label, name string, args ...string) ProbeResult {
	out, err := p.Exec.Run(ctx, Cmd{Dir: p.Root, Name: name, Args: args})
	if err != nil {
		return missing("%s failed: %v", label, err)
	}
	return ok(label + " ok: " + firstLine(out.Stdout))
}

func (p *Prober) mac() ProbeResult {
	if p.Env == harness.EnvCI {
		return ok("CI thresholds apply, reference machine not required")
	}
	if p.GOOS != "darwin" || p.GOARCH != "arm64" {
		return missing("host is %s/%s, reference machine is darwin/arm64", p.GOOS, p.GOARCH)
	}
	return ok("darwin/arm64")
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
