// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
)

const (
	examplePath = "../../deploy/config.example.yaml"
	composePath = "../../deploy/config.compose.yaml"

	// minimalYAML is the smallest valid configuration.
	minimalYAML = `
source: {name: app}
tables: {include: ["public.*"]}
sinks:
  - {name: out, type: stdout}
`
)

// Indexes of the sinks in the example file.
const (
	sinkKafka = iota
	sinkNATS
	sinkWebhook
	sinkStdout
)

func parse(t *testing.T, yaml string, opts config.Options) (config.Config, error) {
	t.Helper()
	return config.Parse([]byte(yaml), opts)
}

func mustParse(t *testing.T, yaml string) config.Config {
	t.Helper()
	cfg, err := parse(t, yaml, config.Options{})
	require.NoError(t, err)
	return cfg
}

// loadExample returns a fresh copy of the example configuration; tests
// mutate it, so each test needs its own.
func loadExample(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.LoadWith(config.Options{Path: examplePath})
	require.NoError(t, err)
	return cfg
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}
