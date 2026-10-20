// SPDX-License-Identifier: Apache-2.0

package runner_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
	"github.com/thnhdatle/wake/acceptance/internal/runner"
)

type probeSetup struct {
	goos, goarch string
	env          string
	handler      func(c runner.Cmd) (runner.Output, error)
}

func (s probeSetup) prober() *runner.Prober {
	if s.goos == "" {
		s.goos, s.goarch = "darwin", "arm64"
	}
	if s.env == "" {
		s.env = harness.EnvMac
	}
	return &runner.Prober{Exec: &fakeExec{handler: s.handler}, Root: "/r", Env: s.env, GOOS: s.goos, GOARCH: s.goarch}
}

func TestProber_Probe(t *testing.T) {
	t.Parallel()
	fail := func(runner.Cmd) (runner.Output, error) { return runner.Output{}, errBoom }
	tests := []struct {
		name   string
		s      probeSetup
		req    string
		ok     bool
		detail string
	}{
		{"docker ok", probeSetup{}, "docker", true, "docker info ok"},
		{"docker missing", probeSetup{handler: fail}, "docker", false, "docker info failed"},
		{"mac ok", probeSetup{}, "mac", true, "darwin/arm64"},
		{"mac wrong host", probeSetup{goos: "linux", goarch: "amd64"}, "mac", false, "linux/amd64"},
		{
			"mac not needed on ci",
			probeSetup{goos: "linux", goarch: "amd64", env: harness.EnvCI},
			"mac",
			true,
			"CI thresholds",
		},
		{"unknown", probeSetup{}, "bogus", false, "unknown requirement"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := tc.s.prober().Probe(context.Background(), tc.req)
			assert.Equal(t, tc.ok, res.OK, res.Detail)
			assert.Contains(t, res.Detail, tc.detail)
		})
	}
}
