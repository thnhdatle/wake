// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
)

func TestDefault_MatchesSpecSection5(t *testing.T) {
	t.Parallel()

	d := config.Default()

	assert.Equal(t, config.Source{
		DSNEnv:      "WAKE_SOURCE_DSN",
		Publication: config.Publication{Mode: config.PublicationManaged},
	}, d.Source)
	assert.Equal(t, config.Duration(time.Minute), d.Tables.RefreshInterval)
	assert.False(t, d.Tables.SkipUnkeyed)
	assert.False(t, d.Tables.EmitUnkeyed)
	assert.Empty(t, d.Tables.NameMap)
	assert.Equal(t, config.Events{}, d.Events)
	assert.Equal(t, config.Snapshot{
		Mode:           config.SnapshotInitial,
		AddedTables:    true,
		ChunkRows:      10000,
		ParallelTables: 4,
	}, d.Snapshot)
	assert.Equal(t, config.Stream{
		MaxTxBytes:      256 << 20,
		SpillDir:        "./spill",
		StatusInterval:  config.Duration(10 * time.Second),
		MinConfirmBytes: 1 << 20,
	}, d.Stream)
	assert.Equal(t, "./wake-state.json", d.State.Path)
	assert.Equal(t, config.Lag{
		Interval:      config.Duration(15 * time.Second),
		WarnBytes:     1 << 30,
		CriticalBytes: 10 << 30,
		MaxAckAge:     config.Duration(5 * time.Minute),
	}, d.Lag)
	assert.Equal(t, config.Duration(time.Minute), d.Heartbeat.Interval)
	assert.Equal(t, "127.0.0.1:9093", d.Admin.Listen)
	assert.Equal(t, "127.0.0.1:9093", config.DefaultAdminListen)

	ci := d.CacheInvalidator
	assert.Equal(t, "127.0.0.1:9096", ci.AdminListen)
	assert.Equal(t, config.SourceKafka, ci.Source)
	assert.Equal(t, "wake-cache-invalidator", ci.Kafka.Group)
	assert.Equal(t, "wake-cache-invalidator", ci.NATS.Durable)
	assert.False(t, ci.InvalidateOnSnapshot)
	assert.Equal(t, "WAKE_SOURCE_DSN", ci.ValidateDSNEnv)
	assert.False(t, ci.Configured())
}

func TestParse_SinkDefaults(t *testing.T) {
	t.Parallel()

	cfg := mustParse(t, `
source: {name: app}
tables: {include: ["*.*"]}
sinks:
  - {name: k, type: kafka, kafka: {brokers: ["b:9092"], topic_prefix: cdc}}
  - {name: n, type: nats, nats: {url: "nats://n:4222", subject_prefix: cdc}}
  - {name: w, type: webhook, webhook: {url: "https://h/x", secret_env: S}}
  - {name: s, type: stdout}
`)

	for _, s := range cfg.Sinks {
		assert.Equal(t, []string{"*"}, s.Tables, s.Name)
		assert.Equal(t, 1000, s.MaxEvents, s.Name)
		assert.Equal(t, config.ByteSize(1<<20), s.MaxBytes, s.Name)
		assert.Equal(t, config.Duration(10*time.Millisecond), s.Linger, s.Name)
	}
	assert.Equal(t, config.ByteSize(1<<20), cfg.Sinks[sinkKafka].MaxEventBytes)
	assert.Equal(t, config.ByteSize(0), cfg.Sinks[sinkNATS].MaxEventBytes, "0 = server max_payload")
	assert.Equal(t, config.ByteSize(10<<20), cfg.Sinks[sinkWebhook].MaxEventBytes)
	assert.Equal(t, config.ByteSize(0), cfg.Sinks[sinkStdout].MaxEventBytes)

	k := cfg.Sinks[sinkKafka].Kafka
	assert.True(t, k.Tombstones)
	assert.False(t, k.AutoCreateTopics)
	assert.Equal(t, -1, k.Partitions)
	assert.Equal(t, -1, k.ReplicationFactor)

	n := cfg.Sinks[sinkNATS].NATS
	assert.True(t, n.CreateStream)
	assert.Equal(t, "wake_app", n.Stream)
	assert.Equal(t, config.Duration(2*time.Minute), n.DuplicateWindow)

	w := cfg.Sinks[sinkWebhook].Webhook
	assert.Equal(t, config.Duration(10*time.Second), w.Timeout)
	assert.Equal(t, 8, w.Lanes)
	assert.Equal(t, 10, w.MaxAttempts)
	assert.Empty(t, w.DeadLetterDir)
}

func TestParse_ExplicitValuesOverrideSinkDefaults(t *testing.T) {
	t.Parallel()

	cfg := mustParse(t, `
source: {name: app}
tables: {include: ["*.*"]}
sinks:
  - name: k
    type: kafka
    tables: []
    max_event_bytes: 2MiB
    kafka: {brokers: ["b:9092"], topic_prefix: cdc, tombstones: false}
`)

	s := cfg.Sinks[0]
	assert.Empty(t, s.Tables, "[] means no tables")
	assert.NotNil(t, s.Tables)
	assert.Equal(t, config.ByteSize(2<<20), s.MaxEventBytes)
	assert.False(t, s.Kafka.Tombstones)
}

func TestParse_Derived(t *testing.T) {
	t.Parallel()

	cfg := mustParse(t, minimalYAML+`
cache_invalidator:
  source: kafka
  namespace: users
  kafka: {brokers: ["b:9092"], topic_prefix: cdc}
  redis: {addrs: ["r:6379"]}
  templates: {"public.users": ["user:{id}"]}
`)

	assert.Equal(t, "wake:inval:users", cfg.CacheInvalidator.BusChannel)
	assert.Equal(t, "wake_app", cfg.CacheInvalidator.NATS.Stream)
	assert.Equal(t, "wake:inval:x", config.DefaultBusChannel("x"))
	assert.Equal(t, "wake_x", config.DefaultNATSStream("x"))
}

func TestParse_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"bad yaml", "admin: [\n", "parse config"},
		{"unknown top-level key", minimalYAML + "bogus: 1\n", "field bogus not found"},
		{"unknown nested key", minimalYAML + "admin: {listen: 'a:1', nope: 1}\n", "field nope not found"},
		{"unknown sink key", "sinks:\n  - {name: a, type: stdout, extra: 1}\n", "field extra not found"},
		{"unknown kafka key", "sinks:\n  - {name: a, type: kafka, kafka: {bogus: 1}}\n", "field bogus not found"},
		{"bad duration", minimalYAML + "heartbeat: {interval: soon}\n", "duration"},
		{"wrong type", minimalYAML + "snapshot: {chunk_rows: many}\n", "parse config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := parse(t, tc.yaml, config.Options{})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestParse_EmptyDocumentUsesDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := parse(t, "", config.Options{SkipValidate: true})

	require.NoError(t, err)
	assert.Equal(t, config.DefaultAdminListen, cfg.Admin.Listen)
}

func TestLoadWith(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string // written to a temp file when set
		missing bool   // point at a file that does not exist
		noFile  bool
		wantErr string
	}{
		{name: "valid file", content: minimalYAML},
		{name: "missing file", missing: true, wantErr: "read config"},
		{name: "invalid file", content: "admin: {listen: nope}\n", wantErr: "admin.listen"},
		{name: "no file is invalid without a source", noFile: true, wantErr: "source.name"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := ""
			switch {
			case tc.missing:
				path = filepath.Join(t.TempDir(), "absent.yaml")
			case !tc.noFile:
				path = writeFile(t, tc.content)
			}

			_, err := config.LoadWith(config.Options{Path: path})

			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestLoadWith_SkipValidate(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadWith(config.Options{SkipValidate: true})

	require.NoError(t, err)
	assert.Equal(t, config.DefaultAdminListen, cfg.Admin.Listen)
}

// t.Setenv mutates the process environment, so this test cannot run in parallel.
func TestLoad_ReadsProcessEnv(t *testing.T) { //nolint:paralleltest // uses t.Setenv
	t.Setenv("WAKE_ADMIN_LISTEN", "0.0.0.0:9999")
	path := writeFile(t, minimalYAML)

	cfg, err := config.Load(path)

	require.NoError(t, err)
	assert.Equal(t, "0.0.0.0:9999", cfg.Admin.Listen)
}

func TestYAML_RoundTrip(t *testing.T) {
	t.Parallel()

	for _, path := range []string{examplePath, composePath} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			cfg, err := config.LoadWith(config.Options{Path: path})
			require.NoError(t, err)

			first, err := cfg.YAML()
			require.NoError(t, err)
			again, err := config.Parse(first, config.Options{})
			require.NoError(t, err)
			second, err := again.YAML()
			require.NoError(t, err)

			assert.Equal(t, string(first), string(second))
			assert.NotContains(t, string(first), "null")
		})
	}
}

func TestCompose_HasKafkaSinkForQuickstart(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadWith(config.Options{Path: composePath})

	require.NoError(t, err)
	require.Len(t, cfg.Sinks, 1)
	assert.Equal(t, "kafka-main", cfg.Sinks[0].Name)
	assert.Equal(t, []string{"kafka:29092"}, cfg.Sinks[0].Kafka.Brokers)
	assert.Equal(t, "cdc", cfg.Sinks[0].Kafka.TopicPrefix)
	assert.True(t, cfg.Sinks[0].Kafka.AutoCreateTopics)
	assert.Equal(t, []string{"public.*"}, cfg.Tables.Include)
	assert.Equal(t, "0.0.0.0:9093", cfg.Admin.Listen)
}
