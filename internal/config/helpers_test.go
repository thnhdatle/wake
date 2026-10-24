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

	minimalYAML = `
source: {name: app}
tables: {include: ["public.*"]}
sinks:
  - {name: out, type: stdout}
`
)

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
