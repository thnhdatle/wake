// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(t.Context())
	return out.String(), err
}

const minimalConfig = `source: {name: app, dsn_env: WAKE_TEST_DSN_NEVER_SET}
tables: {include: ["public.*"]}
sinks: [{name: out, type: stdout}]
`

func TestDestructiveCommandsRequireYes(t *testing.T) {
	t.Parallel()
	cfg := writeConfig(t, minimalConfig)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"slot drop", []string{"slot", "drop", "--config", cfg}, "drop replication slot wake_app"},
		{"publication drop", []string{"publication", "drop", "--config", cfg}, "wake_app_insert"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := execute(t, tc.args...)

			require.ErrorIs(t, err, errNotConfirmed)
			require.ErrorContains(t, err, tc.want)
			assert.Equal(t, exitConfig, exitCode(err))
			assert.Equal(t, codeConfirmationRequired, errorCode(err))
		})
	}
}

func TestCommandsNeedTheSourceDSN(t *testing.T) {
	t.Parallel()
	cfg := writeConfig(t, minimalConfig)
	tests := []struct {
		name string
		args []string
	}{
		{"check", []string{"check", "--config", cfg}},
		{"snapshot table", []string{"snapshot", "table", "public.users", "--config", cfg}},
		{"slot drop", []string{"slot", "drop", "--yes", "--config", cfg}},
		{"publication drop", []string{"publication", "drop", "--yes", "--config", cfg}},
		{"run", []string{"run", "--config", cfg}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := execute(t, tc.args...)

			require.ErrorContains(t, err, "WAKE_TEST_DSN_NEVER_SET is not set")
			assert.Equal(t, exitConfig, exitCode(err))
			assert.Equal(t, codeConfigInvalid, errorCode(err))
		})
	}
}

func TestSnapshotTableRejectsBadNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"users", ".users", "public.", ""} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := execute(t, "snapshot", "table", name, "--config", writeConfig(t, minimalConfig))

			require.ErrorContains(t, err, "must have the form <schema>.<table>")
			assert.Equal(t, exitConfig, exitCode(err))
		})
	}
}

const stateFixture = `{"format_version":1,"slot_name":"wake_app","system_id":"7",
"consistent_point":"0/16B3748","run_counter":2,"schemas":{"public.users":{"version":3,"replica_identity":"d",
"columns":[]}},"snapshot":{"runs":[{"id":2,"kind":"table","chunk_rows":100,"next_chunk":4,
"tables":[{"table":"public.users","position":[41],"rows":300,"done":false}]}],"queue":["public.orders"]}}`

func TestStatusFallsBackToTheStateFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	state := filepath.Join(dir, "state.json")
	require.NoError(t, os.WriteFile(state, []byte(stateFixture), 0o600))
	cfg := writeConfig(t, minimalConfig+"state: {path: "+state+"}\n")

	cmd := newRootCmd()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"status", "--config", cfg, "--addr", "127.0.0.1:1"})
	require.NoError(t, cmd.ExecuteContext(t.Context()))

	assert.Contains(t, errOut.String(), "unreachable")
	assert.Contains(t, errOut.String(), "reading the state file")
	var doc statusDoc
	require.NoError(t, json.Unmarshal(out.Bytes(), &doc))
	assert.Equal(t, "state_file", doc.Origin)
	assert.Equal(t, "wake_app", doc.Slot.Name)
	assert.Contains(t, doc.Slot.Error, "WAKE_TEST_DSN_NEVER_SET")
	assert.Equal(t, "0/16B3748", doc.Snapshot.ConsistentPoint)
	assert.Equal(t, []string{"public.orders"}, doc.Snapshot.Queue)
	require.Len(t, doc.Snapshot.Runs, 1)
	assert.Equal(t, []progressDoc{{Table: "public.users", Rows: 300}}, doc.Snapshot.Runs[0].Tables)
	assert.Equal(t, map[string]uint64{"public.users": 3}, doc.Schemas)
	assert.NotContains(t, out.String(), "position", "chunk positions hold key values")
}

func TestVersion(t *testing.T) {
	t.Parallel()

	out, err := execute(t, "version")

	require.NoError(t, err)
	assert.Contains(t, out, "wake dev (commit none")
}

func TestSnapshotTableNeedsArgument(t *testing.T) {
	t.Parallel()

	_, err := execute(t, "snapshot", "table")

	require.ErrorContains(t, err, "accepts 1 arg")
}

func TestHealthcheckUnreachable(t *testing.T) {
	t.Parallel()

	_, err := execute(t, "healthcheck", "--addr", "127.0.0.1:1")

	require.ErrorContains(t, err, "healthcheck")
}

func TestRunInvalidConfigExitsTwo(t *testing.T) {
	t.Parallel()

	_, err := execute(t, "run", "--config", "/nonexistent/wake.yaml")

	var ce *exitError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, 2, ce.code)
}
