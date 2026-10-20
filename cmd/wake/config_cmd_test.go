// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
)

const (
	exampleConfig = "../../deploy/config.example.yaml"
	composeConfig = "../../deploy/config.compose.yaml"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "wake.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string // used as is when content is empty
		content  string // written to a temp file when set
		wantErr  []string
		wantCode int
	}{
		{name: "example", path: exampleConfig},
		{name: "compose", path: composeConfig},
		{
			name: "every problem reported", wantCode: 2,
			content: "source: {name: Bad}\nsnapshot: {chunk_rows: 0}\nadmin: {listen: nope}\n",
			wantErr: []string{"source.name", "snapshot.chunk_rows", "admin.listen", "sinks"},
		},
		{name: "missing file", wantCode: 2, path: "/nonexistent/wake.yaml", wantErr: []string{"read config"}},
		{name: "unknown key", wantCode: 2, content: "bogus: 1\n", wantErr: []string{"bogus"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := tc.path
			if tc.content != "" {
				path = writeConfig(t, tc.content)
			}

			out, err := execute(t, "config", "validate", "--config", path)

			if tc.wantCode == 0 {
				require.NoError(t, err)
				assert.Contains(t, out, "valid")
				return
			}
			var ce *exitError
			require.ErrorAs(t, err, &ce)
			assert.Equal(t, tc.wantCode, ce.code)
			for _, w := range tc.wantErr {
				assert.ErrorContains(t, err, w)
			}
		})
	}
}

func TestConfigPrint(t *testing.T) {
	t.Parallel()

	secretURL := "url: https://user:hunter2@hooks.example/x"
	cfg := writeConfig(t, `
source: {name: app}
tables: {include: ["public.*"]}
sinks:
  - name: hooks
    type: webhook
    webhook:
      `+secretURL+`
      secret_env: WAKE_WEBHOOK_SECRET
`)
	tests := []struct {
		name        string
		args        []string
		wantContain []string
		wantAbsent  []string
	}{
		{
			name:        "effective config with defaults",
			args:        []string{"config", "print", "--config", exampleConfig},
			wantContain: []string{"name: app", "status_interval: 10s", "max_tx_bytes: 256MiB", "listen: 127.0.0.1:9093"},
		},
		{
			name:        "redact keeps env names, masks url passwords",
			args:        []string{"config", "print", "--redact", "--config", cfg},
			wantContain: []string{"secret_env: WAKE_WEBHOOK_SECRET", "xxxxx"},
			wantAbsent:  []string{"hunter2"},
		},
		{
			name:        "without redact the file value is shown",
			args:        []string{"config", "print", "--config", cfg},
			wantContain: []string{"hunter2"},
		},
		{
			name:        "invalid config still prints",
			args:        []string{"config", "print", "--config", writeConfig(t, "snapshot: {chunk_rows: 0}\n")},
			wantContain: []string{"chunk_rows: 0"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := execute(t, tc.args...)

			require.NoError(t, err)
			for _, w := range tc.wantContain {
				assert.Contains(t, out, w)
			}
			for _, w := range tc.wantAbsent {
				assert.NotContains(t, out, w)
			}
		})
	}
}

func TestConfigPrintBadFileExitsTwo(t *testing.T) {
	t.Parallel()

	_, err := execute(t, "config", "print", "--config", writeConfig(t, "admin: [\n"))

	var ce *exitError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, 2, ce.code)
}

// t.Setenv mutates the process environment, so this test cannot run in parallel.
func TestConfigPrintAppliesEnvButNeverPrintsSecrets(t *testing.T) { //nolint:paralleltest // uses t.Setenv
	t.Setenv("WAKE_ADMIN_LISTEN", "0.0.0.0:9999")
	t.Setenv("WAKE_SOURCE_DSN", "postgres://user:dsn-secret@h/db")

	out, err := execute(t, "config", "print", "--redact", "--config", composeConfig)

	require.NoError(t, err)
	assert.Contains(t, out, "listen: 0.0.0.0:9999")
	assert.Contains(t, out, "dsn_env: WAKE_SOURCE_DSN")
	assert.NotContains(t, out, "dsn-secret")
}

// An invalidator section with validate_dsn_env naming an unset variable: the
// database check is skipped, so validation passes without a database.
func TestConfigValidateSkipsTemplateCheckWithoutDSN(t *testing.T) {
	t.Parallel()
	cfg := writeConfig(t, minimalConfig+`cache_invalidator:
  source: kafka
  kafka:
    brokers: ["127.0.0.1:9"]
    topic_prefix: cdc
  redis:
    addrs: ["127.0.0.1:9"]
  namespace: users
  validate_dsn_env: WAKE_TEST_VALIDATE_DSN_NEVER_SET
  templates:
    public.users: ["user:{id}", "user:email:{email}"]
`)

	out, err := execute(t, "config", "validate", "--config", cfg)

	require.NoError(t, err)
	assert.Contains(t, out, "configuration is valid")
	assert.Contains(t, out, "replica identity is checked at runtime only")
}

func TestConfigValidateRejectsInvalidTemplates(t *testing.T) {
	t.Parallel()
	cfg := writeConfig(t, minimalConfig+`cache_invalidator:
  source: kafka
  kafka:
    brokers: ["127.0.0.1:9"]
    topic_prefix: cdc
  redis:
    addrs: ["127.0.0.1:9"]
  namespace: users
  validate_dsn_env: WAKE_TEST_VALIDATE_DSN_NEVER_SET
  templates:
    public.users: ["user:{id"]
`)

	_, err := execute(t, "config", "validate", "--config", cfg)

	var ce *exitError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, 2, ce.code)
	assert.ErrorContains(t, err, "cache_invalidator.templates")
}

func TestValidateInvalidatorTemplatesNotConfigured(t *testing.T) {
	t.Parallel()
	cfg := config.Default()
	env := invalEnv{getenv: func(string) string { return "postgres://never-used" }, log: slog.New(slog.DiscardHandler)}

	require.NoError(t, validateInvalidatorTemplates(t.Context(), &cfg, env), "section absent: no database access")
}
