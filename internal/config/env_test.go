// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
)

const withPublicationName = `
source: {name: app, publication: {name: from-file}}
tables: {include: ["public.*"]}
sinks:
  - {name: out, type: stdout}
`

func lookup(env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}
}

func TestEnvName(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "WAKE_ADMIN_LISTEN", config.EnvName("admin.listen"))
	assert.Equal(t, "WAKE_STREAM_STATUS_INTERVAL", config.EnvName("stream.status_interval"))
	assert.Equal(t, "WAKE_CACHE_INVALIDATOR_KAFKA_TLS_CA_FILE", config.EnvName("cache_invalidator.kafka.tls.ca_file"))
}

func TestParse_EnvOverrides(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  map[string]string
		get  func(c config.Config) any
		want any
	}{
		{
			"string",
			map[string]string{"WAKE_ADMIN_LISTEN": "0.0.0.0:1"},
			func(c config.Config) any { return c.Admin.Listen }, "0.0.0.0:1",
		},
		{
			"duration with d suffix",
			map[string]string{"WAKE_STREAM_STATUS_INTERVAL": "1d"},
			func(c config.Config) any { return c.Stream.StatusInterval }, config.Duration(24 * time.Hour),
		},
		{
			"size",
			map[string]string{"WAKE_STREAM_MAX_TX_BYTES": "1GiB"},
			func(c config.Config) any { return c.Stream.MaxTxBytes }, config.ByteSize(1 << 30),
		},
		{
			"int",
			map[string]string{"WAKE_SNAPSHOT_CHUNK_ROWS": "500"},
			func(c config.Config) any { return c.Snapshot.ChunkRows }, 500,
		},
		{
			"bool",
			map[string]string{"WAKE_EVENTS_EMIT_TRUNCATE": "true"},
			func(c config.Config) any { return c.Events.EmitTruncate }, true,
		},
		{
			"typed string enum",
			map[string]string{"WAKE_SNAPSHOT_MODE": "never"},
			func(c config.Config) any { return c.Snapshot.Mode }, config.SnapshotNever,
		},
		{
			"string list",
			map[string]string{"WAKE_TABLES_INCLUDE": "public.a, public.b,,"},
			func(c config.Config) any { return c.Tables.Include },
			[]string{"public.a", "public.b"},
		},
		{
			"nested section",
			map[string]string{"WAKE_CACHE_INVALIDATOR_REDIS_MASTER_NAME": "mymaster"},
			func(c config.Config) any { return c.CacheInvalidator.Redis.MasterName }, "mymaster",
		},
		{
			"empty value clears",
			map[string]string{"WAKE_SOURCE_PUBLICATION_NAME": ""},
			func(c config.Config) any { return c.Source.Publication.Name }, "",
		},
		{
			"unrelated variables ignored",
			map[string]string{"WAKE_CONFIG": "x", "WAKE_SOURCE_DSN": "postgres://u:p@h/db"},
			func(c config.Config) any { return c.Source.DSNEnv }, "WAKE_SOURCE_DSN",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := parse(t, withPublicationName,
				config.Options{Env: lookup(tc.env), SkipValidate: true})

			require.NoError(t, err)
			assert.Equal(t, tc.want, tc.get(cfg))
		})
	}
}

func TestParse_Precedence(t *testing.T) {
	t.Parallel()

	file := minimalYAML + "admin: {listen: '1.1.1.1:1'}\nlag: {interval: 20s}\n"
	tests := []struct {
		name      string
		env       map[string]string
		overrides map[string]string
		want      string
	}{
		{"file beats default", nil, nil, "1.1.1.1:1"},
		{"env beats file", map[string]string{"WAKE_ADMIN_LISTEN": "2.2.2.2:2"}, nil, "2.2.2.2:2"},
		{
			"flag beats env",
			map[string]string{"WAKE_ADMIN_LISTEN": "2.2.2.2:2"},
			map[string]string{"admin.listen": "3.3.3.3:3"},
			"3.3.3.3:3",
		},
		{"flag beats file", nil, map[string]string{"admin.listen": "3.3.3.3:3"}, "3.3.3.3:3"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := parse(t, file, config.Options{Env: lookup(tc.env), Overrides: tc.overrides})

			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.Admin.Listen)
			assert.Equal(t, config.Duration(20*time.Second), cfg.Lag.Interval, "untouched file value kept")
			assert.Equal(t, config.Duration(time.Minute), cfg.Heartbeat.Interval, "untouched default kept")
		})
	}
}

func TestParse_EnvAndOverrideErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts config.Options
		want []string
	}{
		{
			"bad duration",
			config.Options{Env: lookup(map[string]string{"WAKE_STREAM_STATUS_INTERVAL": "x"})},
			[]string{"WAKE_STREAM_STATUS_INTERVAL", "stream.status_interval"},
		},
		{
			"bad bool",
			config.Options{Env: lookup(map[string]string{"WAKE_EVENTS_EMIT_TRUNCATE": "maybe"})},
			[]string{"WAKE_EVENTS_EMIT_TRUNCATE", "boolean"},
		},
		{
			"bad int",
			config.Options{Env: lookup(map[string]string{"WAKE_SNAPSHOT_CHUNK_ROWS": "1.5"})},
			[]string{"WAKE_SNAPSHOT_CHUNK_ROWS", "integer"},
		},
		{
			"unknown override key",
			config.Options{Overrides: map[string]string{"nope.key": "1"}},
			[]string{"override nope.key"},
		},
		{
			"list key is not overridable",
			config.Options{Overrides: map[string]string{"sinks": "x"}},
			[]string{"override sinks"},
		},
		{
			"bad override value",
			config.Options{Overrides: map[string]string{"snapshot.chunk_rows": "x"}},
			[]string{"override snapshot.chunk_rows"},
		},
		{
			"env error and validation error together",
			config.Options{Env: lookup(map[string]string{"WAKE_SNAPSHOT_CHUNK_ROWS": "x", "WAKE_SNAPSHOT_MODE": "bogus"})},
			[]string{"WAKE_SNAPSHOT_CHUNK_ROWS", "snapshot.mode"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parse(t, minimalYAML, tc.opts)

			require.Error(t, err)
			for _, w := range tc.want {
				assert.ErrorContains(t, err, w)
			}
		})
	}
}
