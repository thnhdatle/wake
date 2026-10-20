// SPDX-License-Identifier: Apache-2.0

package pgtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImageFor_DefaultsAndValidates(t *testing.T) {
	t.Parallel()
	img, err := imageFor("")
	require.NoError(t, err)
	assert.Equal(t, "postgres:"+DefaultVersion, img)
	img, err = imageFor("14")
	require.NoError(t, err)
	assert.Equal(t, "postgres:14", img)
	for _, bad := range []string{"17/evil", "evil:latest", "a b", "x@sha256"} {
		_, err := imageFor(bad)
		require.ErrorIs(t, err, ErrBadVersion, bad)
	}
}

func TestImage_ReadsEnvironment(t *testing.T) { //nolint:paralleltest // t.Setenv forbids t.Parallel
	t.Setenv(EnvVersion, "16")
	img, err := Image()
	require.NoError(t, err)
	assert.Equal(t, "postgres:16", img)
}

func TestCommand_EnablesLogicalDecoding(t *testing.T) {
	t.Parallel()
	cmd := Command()
	assert.Equal(t, "postgres", cmd[0])
	assert.Contains(t, cmd, "wal_level=logical")
	assert.Contains(t, cmd, "max_wal_senders=20")
	assert.Contains(t, cmd, "max_replication_slots=20")
}

func TestDSN_EscapesCredentials(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "postgres://r%40x:wake@127.0.0.1:5432/db?sslmode=disable", dsn("127.0.0.1:5432", "db", "r@x"))
	s := &Server{hostPort: "h:1"}
	assert.Equal(t, "postgres://u:wake@h:1/d?sslmode=disable", s.DSN("d", "u"))
	assert.Equal(t, "db_1", s.unique("db"))
	assert.Equal(t, "db_2", s.unique("db"))
}
