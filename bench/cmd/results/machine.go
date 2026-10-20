// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const gib = 1 << 30

// machine describes the host a benchmark ran on; zero fields are omitted from the line.
type machine struct {
	model     string
	cpu       string
	memBytes  int64
	macOS     string
	dockerCPU int
	dockerMem int64
	goVersion string
	version   string // Wake version under test
}

// collectMachine gathers the machine facts by shelling out; every failure only drops a field.
func collectMachine(version string) machine {
	m := machine{
		model:     sh("sysctl", "-n", "hw.model"),
		cpu:       sh("sysctl", "-n", "machdep.cpu.brand_string"),
		macOS:     sh("sw_vers", "-productVersion"),
		goVersion: runtime.Version(),
		version:   version,
	}
	if m.model == "" {
		m.model = runtime.GOOS + "/" + runtime.GOARCH
	}
	m.memBytes, _ = strconv.ParseInt(sh("sysctl", "-n", "hw.memsize"), 10, 64)
	m.dockerCPU, m.dockerMem = parseDockerInfo(sh("docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"))
	return m
}

// sh runs a fixed command and returns its trimmed stdout, or "" on any failure.
func sh(name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).Output() //nolint:gosec // callers pass fixed commands
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// parseDockerInfo parses "<NCPU> <MemTotal bytes>"; it returns zeros when the input is malformed.
func parseDockerInfo(s string) (cpu int, mem int64) {
	f := strings.Fields(s)
	if len(f) != 2 {
		return 0, 0
	}
	c, err := strconv.Atoi(f[0])
	if err != nil {
		return 0, 0
	}
	b, err := strconv.ParseInt(f[1], 10, 64)
	if err != nil {
		return 0, 0
	}
	return c, b
}

// formatMachine renders the "Machine:" line printed under every result.
func formatMachine(m machine) string {
	var parts []string
	add := func(s string) {
		if s != "" {
			parts = append(parts, s)
		}
	}
	add(m.model)
	add(m.cpu)
	if m.memBytes > 0 {
		add(gbString(m.memBytes) + " GB")
	}
	if m.macOS != "" {
		add("macOS " + m.macOS)
	}
	if m.dockerCPU > 0 && m.dockerMem > 0 {
		add(fmt.Sprintf("Docker VM %d CPU / %s GB", m.dockerCPU, gbString(m.dockerMem)))
	}
	add("Go " + strings.TrimPrefix(m.goVersion, "go"))
	if m.version != "" {
		add("Wake " + m.version)
	}
	return strings.Join(parts, ", ")
}

// gbString renders bytes as GiB, with one decimal only when it is not a whole number.
func gbString(b int64) string {
	g := float64(b) / gib
	if r := float64(int64(g + 0.5)); g > r-0.05 && g < r+0.05 {
		return strconv.FormatInt(int64(r), 10)
	}
	return strconv.FormatFloat(g, 'f', 1, 64)
}
