// SPDX-License-Identifier: Apache-2.0

package buildinfo_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/wake/internal/platform/buildinfo"
)

func TestGetDefaults(t *testing.T) {
	t.Parallel()

	got := buildinfo.Get()

	assert.Equal(t, "dev", got.Version)
	assert.Equal(t, "none", got.Commit)
	assert.Equal(t, "unknown", got.BuildDate)
	assert.Equal(t, runtime.Version(), got.GoVersion)
}

func TestInfoString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		info buildinfo.Info
		want string
	}{
		{
			"defaults",
			buildinfo.Info{Version: "dev", Commit: "none", BuildDate: "unknown", GoVersion: "go1"},
			"wake dev (commit none, built unknown, go1)",
		},
		{
			"release",
			buildinfo.Info{Version: "0.1.0", Commit: "abc", BuildDate: "2026-01-01", GoVersion: "go2"},
			"wake 0.1.0 (commit abc, built 2026-01-01, go2)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.info.String())
		})
	}
}
