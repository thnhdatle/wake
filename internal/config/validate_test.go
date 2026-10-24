// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
)

type conf = config.Config

func kafkaSink(c *conf) *config.KafkaSink    { return c.Sinks[sinkKafka].Kafka }
func natsSink(c *conf) *config.NATSSink      { return c.Sinks[sinkNATS].NATS }
func hookSink(c *conf) *config.WebhookSink   { return c.Sinks[sinkWebhook].Webhook }
func inval(c *conf) *config.CacheInvalidator { return &c.CacheInvalidator }

func TestValidate_ExampleIsValid(t *testing.T) {
	t.Parallel()

	require.NoError(t, loadExample(t).Validate())
}

func TestValidate_Accepts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(c *conf)
	}{
		{"source name of 58 characters", func(c *conf) { c.Source.Name = strings.Repeat("a", 58) }},
		{"51 characters with managed emit_unkeyed", func(c *conf) {
			c.Source.Name = strings.Repeat("a", 51)
			c.Tables.EmitUnkeyed = true
		}},
		{"52 characters without emit_unkeyed", func(c *conf) { c.Source.Name = strings.Repeat("a", 52) }},
		{"52 characters with existing publication", func(c *conf) {
			c.Source.Name = strings.Repeat("a", 52)
			c.Tables.EmitUnkeyed = true
			c.Source.Publication = config.Publication{Mode: config.PublicationExisting, Name: "mypub"}
		}},
		{"existing mode needs no include", func(c *conf) {
			c.Source.Publication = config.Publication{Mode: config.PublicationExisting, Name: "mypub"}
			c.Tables.Include = nil
		}},
		{"snapshot never", func(c *conf) { c.Snapshot.Mode = config.SnapshotNever }},
		{"valid name map", func(c *conf) {
			c.Tables.NameMap = map[string]string{"public.User Data": "public.user_data", "my schema.t": "my_schema.t"}
		}},
		{"unconfigured cache invalidator", func(c *conf) { c.CacheInvalidator = config.Default().CacheInvalidator }},
		{"nats source", func(c *conf) { inval(c).Source = config.SourceNATS }},
		{"sasl scram 512", func(c *conf) {
			kafkaSink(c).SASL = config.SASL{Mechanism: config.SASLScramSHA512, UsernameEnv: "U", PasswordEnv: "P"}
		}},
		{"tls cert and key", func(c *conf) {
			kafkaSink(c).TLS = config.TLS{Enabled: true, CertFile: "c.pem", KeyFile: "k.pem"}
		}},
		{"nats url list", func(c *conf) { natsSink(c).URL = "nats://a:4222, tls://b:4222" }},
		{"kafka default partitions", func(c *conf) {
			kafkaSink(c).Partitions = -1
			kafkaSink(c).ReplicationFactor = -1
		}},
		{"ipv6 listen", func(c *conf) { c.Admin.Listen = "[::1]:9093" }},
		{"nats duplicate window ignored without create_stream", func(c *conf) {
			natsSink(c).CreateStream = false
			natsSink(c).DuplicateWindow = 0
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := loadExample(t)
			tc.mutate(&cfg)

			require.NoError(t, cfg.Validate())
		})
	}
}

func TestValidate_Rejects(t *testing.T) {
	t.Parallel()

	longTable := strings.Repeat("t", 250)
	tests := []struct {
		name   string
		mutate func(c *conf)
		want   string
	}{
		{"source name empty", func(c *conf) { c.Source.Name = "" }, "source.name: required"},
		{"source name upper case", func(c *conf) { c.Source.Name = "App" }, "source.name: \"App\" must match"},
		{"source name with dash", func(c *conf) { c.Source.Name = "my-app" }, "source.name"},
		{"source name too long", func(c *conf) { c.Source.Name = strings.Repeat("a", 59) }, "source.name"},
		{"source name 52 with managed emit_unkeyed", func(c *conf) {
			c.Source.Name = strings.Repeat("a", 52)
			c.Tables.EmitUnkeyed = true
		}, "source.name: \"" + strings.Repeat("a", 52) + "\" has 52 characters; at most 51"},
		{"dsn_env empty", func(c *conf) { c.Source.DSNEnv = "" }, "source.dsn_env: required"},
		{"dsn_env holds a dsn", func(c *conf) { c.Source.DSNEnv = "postgres://u:p@h/db" }, "source.dsn_env"},
		{"publication mode unknown", func(c *conf) { c.Source.Publication.Mode = "auto" }, "source.publication.mode"},
		{"existing without name", func(c *conf) {
			c.Source.Publication = config.Publication{Mode: config.PublicationExisting}
		}, "source.publication.name: required"},

		{"managed without include", func(c *conf) { c.Tables.Include = nil }, "tables.include: at least one glob"},
		{"include glob malformed", func(c *conf) { c.Tables.Include = []string{"public.[x"} }, "tables.include[0]"},
		{"exclude glob empty", func(c *conf) { c.Tables.Exclude = []string{""} }, "tables.exclude[0]"},
		{"refresh_interval zero", func(c *conf) { c.Tables.RefreshInterval = 0 }, "tables.refresh_interval"},
		{"name_map key without dot", func(c *conf) {
			c.Tables.NameMap = map[string]string{"users": "public.users"}
		}, `tables.name_map["users"]: key must have the form`},
		{"name_map target without dot", func(c *conf) {
			c.Tables.NameMap = map[string]string{"public.a b": "users"}
		}, `tables.name_map["public.a b"]: target "users" must have the form`},
		{"name_map target schema invalid", func(c *conf) {
			c.Tables.NameMap = map[string]string{"public.a b": "my schema.users"}
		}, `tables.name_map["public.a b"]: target "my schema.users"`},
		{"name_map target table invalid", func(c *conf) {
			c.Tables.NameMap = map[string]string{"public.a b": "public.us ers"}
		}, "must match [A-Za-z0-9_-]+"},
		{"name_map collision", func(c *conf) {
			c.Tables.NameMap = map[string]string{"public.a b": "public.x", "public.c d": "public.x"}
		}, `tables.name_map["public.c d"]: target "public.x" is also the target of "public.a b"`},
		{"name_map topic too long", func(c *conf) {
			c.Tables.NameMap = map[string]string{"public.a b": "public." + longTable}
		}, `tables.name_map["public.a b"]: target "public.` + longTable[:10]},

		{"snapshot mode unknown", func(c *conf) { c.Snapshot.Mode = "always" }, "snapshot.mode"},
		{"chunk_rows zero", func(c *conf) { c.Snapshot.ChunkRows = 0 }, "snapshot.chunk_rows"},
		{"chunk_rows negative", func(c *conf) { c.Snapshot.ChunkRows = -5 }, "snapshot.chunk_rows"},
		{"parallel_tables zero", func(c *conf) { c.Snapshot.ParallelTables = 0 }, "snapshot.parallel_tables"},
		{"max_tx_bytes zero", func(c *conf) { c.Stream.MaxTxBytes = 0 }, "stream.max_tx_bytes"},
		{"spill_dir empty", func(c *conf) { c.Stream.SpillDir = "" }, "stream.spill_dir"},
		{"status_interval zero", func(c *conf) { c.Stream.StatusInterval = 0 }, "stream.status_interval"},
		{"min_confirm_bytes negative", func(c *conf) { c.Stream.MinConfirmBytes = -1 }, "stream.min_confirm_bytes"},
		{"state path empty", func(c *conf) { c.State.Path = "" }, "state.path"},

		{"lag interval zero", func(c *conf) { c.Lag.Interval = 0 }, "lag.interval"},
		{"warn equals critical", func(c *conf) { c.Lag.WarnBytes = c.Lag.CriticalBytes }, "lag.warn_bytes"},
		{"warn above critical", func(c *conf) { c.Lag.WarnBytes = c.Lag.CriticalBytes + 1 }, "lag.warn_bytes"},
		{"warn zero", func(c *conf) { c.Lag.WarnBytes = 0 }, "lag.warn_bytes"},
		{"critical zero", func(c *conf) { c.Lag.CriticalBytes = 0 }, "lag.critical_bytes"},
		{"max_ack_age zero", func(c *conf) { c.Lag.MaxAckAge = 0 }, "lag.max_ack_age"},
		{"alert_webhook scheme", func(c *conf) { c.Lag.AlertWebhook = "ftp://x/y" }, "lag.alert_webhook"},
		{"alert_webhook password", func(c *conf) { c.Lag.AlertWebhook = "https://u:p@x/y" }, "lag.alert_webhook"},
		{"heartbeat zero", func(c *conf) { c.Heartbeat.Interval = 0 }, "heartbeat.interval"},
		{"admin listen no port", func(c *conf) { c.Admin.Listen = "localhost" }, "admin.listen"},
		{"admin listen bad port", func(c *conf) { c.Admin.Listen = "127.0.0.1:99999" }, "admin.listen"},
		{"admin listen port not numeric", func(c *conf) { c.Admin.Listen = "127.0.0.1:http" }, "admin.listen"},

		{"no sinks", func(c *conf) { c.Sinks = nil }, "sinks: at least one sink"},
		{"sink name empty", func(c *conf) { c.Sinks[sinkNATS].Name = "" }, "sinks[1].name: must not be empty"},
		{
			"sink name duplicate",
			func(c *conf) { c.Sinks[sinkNATS].Name = "kafka-main" },
			`sinks[1].name: "kafka-main" duplicates sinks[0]`,
		},
		{"sink type unknown", func(c *conf) { c.Sinks[sinkStdout].Type = "sqs" }, "sinks[3].type"},
		{"sink tables glob", func(c *conf) { c.Sinks[sinkStdout].Tables = []string{"[bad"} }, "sinks[3].tables[0]"},
		{"max_events zero", func(c *conf) { c.Sinks[sinkStdout].MaxEvents = 0 }, "sinks[3].max_events"},
		{"max_bytes zero", func(c *conf) { c.Sinks[sinkStdout].MaxBytes = 0 }, "sinks[3].max_bytes"},
		{"linger negative", func(c *conf) { c.Sinks[sinkStdout].Linger = -1 }, "sinks[3].linger"},
		{
			"max_event_bytes negative",
			func(c *conf) { c.Sinks[sinkStdout].MaxEventBytes = -1 },
			"sinks[3].max_event_bytes",
		},
		{"kafka section on stdout", func(c *conf) {
			c.Sinks[sinkStdout].Kafka = kafkaSink(c)
		}, "sinks[3].kafka: not allowed for a sink of type stdout"},

		{"kafka section missing", func(c *conf) { c.Sinks[sinkKafka].Kafka = nil }, "sinks[0].kafka: section required"},
		{"kafka brokers empty", func(c *conf) { kafkaSink(c).Brokers = nil }, "sinks[0].kafka.brokers"},
		{
			"kafka broker without port",
			func(c *conf) { kafkaSink(c).Brokers = []string{"a:9092", "b"} },
			"sinks[0].kafka.brokers[1]",
		},
		{
			"kafka broker without host",
			func(c *conf) { kafkaSink(c).Brokers = []string{":9092"} },
			"sinks[0].kafka.brokers[0]",
		},
		{
			"kafka prefix empty",
			func(c *conf) { kafkaSink(c).TopicPrefix = "" },
			"sinks[0].kafka.topic_prefix: required",
		},
		{"kafka prefix invalid", func(c *conf) { kafkaSink(c).TopicPrefix = "a.b" }, "sinks[0].kafka.topic_prefix"},
		{"kafka partitions zero", func(c *conf) { kafkaSink(c).Partitions = 0 }, "sinks[0].kafka.partitions"},
		{"kafka partitions below -1", func(c *conf) { kafkaSink(c).Partitions = -2 }, "sinks[0].kafka.partitions"},
		{
			"kafka replication zero",
			func(c *conf) { kafkaSink(c).ReplicationFactor = 0 },
			"sinks[0].kafka.replication_factor",
		},
		{
			"sasl mechanism unknown",
			func(c *conf) { kafkaSink(c).SASL.Mechanism = "PLAIN" },
			"sinks[0].kafka.sasl.mechanism",
		},
		{"sasl without username_env", func(c *conf) {
			kafkaSink(c).SASL = config.SASL{Mechanism: config.SASLScramSHA256, PasswordEnv: "P"}
		}, "sinks[0].kafka.sasl.username_env"},
		{"sasl without password_env", func(c *conf) {
			kafkaSink(c).SASL = config.SASL{Mechanism: config.SASLScramSHA256, UsernameEnv: "U"}
		}, "sinks[0].kafka.sasl.password_env"},
		{"sasl password value instead of name", func(c *conf) {
			kafkaSink(c).SASL = config.SASL{Mechanism: config.SASLScramSHA256, UsernameEnv: "U", PasswordEnv: "p@ss word"}
		}, "sinks[0].kafka.sasl.password_env"},
		{"kafka tls cert without key", func(c *conf) {
			kafkaSink(c).TLS = config.TLS{CertFile: "c.pem"}
		}, "sinks[0].kafka.tls.cert_file"},
		{"kafka tls key without cert", func(c *conf) {
			kafkaSink(c).TLS = config.TLS{KeyFile: "k.pem"}
		}, "sinks[0].kafka.tls.cert_file"},

		{"nats section missing", func(c *conf) { c.Sinks[sinkNATS].NATS = nil }, "sinks[1].nats: section required"},
		{"nats url empty", func(c *conf) { natsSink(c).URL = "" }, "sinks[1].nats.url: required"},
		{"nats url scheme", func(c *conf) { natsSink(c).URL = "http://h:4222" }, "sinks[1].nats.url"},
		{"nats url no host", func(c *conf) { natsSink(c).URL = "nats://" }, "sinks[1].nats.url"},
		{"nats url unparsable", func(c *conf) { natsSink(c).URL = "nats://h:99x" }, "sinks[1].nats.url"},
		{"nats url password", func(c *conf) { natsSink(c).URL = "nats://u:p@h:4222" }, "sinks[1].nats.url"},
		{"nats prefix invalid", func(c *conf) { natsSink(c).SubjectPrefix = "a b" }, "sinks[1].nats.subject_prefix"},
		{
			"nats duplicate window zero",
			func(c *conf) { natsSink(c).DuplicateWindow = 0 },
			"sinks[1].nats.duplicate_window",
		},
		{"nats tls half", func(c *conf) { natsSink(c).TLS.KeyFile = "k.pem" }, "sinks[1].nats.tls.cert_file"},

		{
			"webhook section missing",
			func(c *conf) { c.Sinks[sinkWebhook].Webhook = nil },
			"sinks[2].webhook: section required",
		},
		{"webhook url empty", func(c *conf) { hookSink(c).URL = "" }, "sinks[2].webhook.url: required"},
		{"webhook url scheme", func(c *conf) { hookSink(c).URL = "ftp://h/x" }, "sinks[2].webhook.url"},
		{"webhook url no host", func(c *conf) { hookSink(c).URL = "https:///x" }, "sinks[2].webhook.url"},
		{"webhook url unparsable", func(c *conf) { hookSink(c).URL = "https://h:x/" }, "sinks[2].webhook.url"},
		{"webhook url password", func(c *conf) { hookSink(c).URL = "https://u:p@h/x" }, "sinks[2].webhook.url"},
		{
			"webhook secret_env empty",
			func(c *conf) { hookSink(c).SecretEnv = "" },
			"sinks[2].webhook.secret_env: required",
		},
		{
			"webhook secret_env is a value",
			func(c *conf) { hookSink(c).SecretEnv = "s3-cr3t!" },
			"sinks[2].webhook.secret_env",
		},
		{"webhook timeout zero", func(c *conf) { hookSink(c).Timeout = 0 }, "sinks[2].webhook.timeout"},
		{"webhook lanes zero", func(c *conf) { hookSink(c).Lanes = 0 }, "sinks[2].webhook.lanes"},
		{"webhook max_attempts zero", func(c *conf) { hookSink(c).MaxAttempts = 0 }, "sinks[2].webhook.max_attempts"},

		{"invalidator admin_listen", func(c *conf) { inval(c).AdminListen = "nope" }, "cache_invalidator.admin_listen"},
		{"invalidator source", func(c *conf) { inval(c).Source = "sqs" }, "cache_invalidator.source"},
		{"invalidator namespace", func(c *conf) { inval(c).Namespace = "a b" }, "cache_invalidator.namespace"},
		{"invalidator bus_channel empty", func(c *conf) { inval(c).BusChannel = "" }, "cache_invalidator.bus_channel"},
		{
			"invalidator validate_dsn_env",
			func(c *conf) { inval(c).ValidateDSNEnv = "postgres://x" },
			"cache_invalidator.validate_dsn_env",
		},
		{
			"invalidator redis addr",
			func(c *conf) { inval(c).Redis.Addrs = []string{"redis"} },
			"cache_invalidator.redis.addrs[0]",
		},
		{
			"invalidator redis addrs empty",
			func(c *conf) { inval(c).Redis.Addrs = nil },
			"cache_invalidator.redis.addrs",
		},
		{
			"invalidator redis password_env",
			func(c *conf) { inval(c).Redis.PasswordEnv = "hunter 2" },
			"cache_invalidator.redis.password_env",
		},
		{
			"invalidator redis tls",
			func(c *conf) { inval(c).Redis.TLS.CertFile = "c.pem" },
			"cache_invalidator.redis.tls.cert_file",
		},
		{
			"invalidator kafka brokers",
			func(c *conf) { inval(c).Kafka.Brokers = nil },
			"cache_invalidator.kafka.brokers",
		},
		{
			"invalidator kafka prefix",
			func(c *conf) { inval(c).Kafka.TopicPrefix = "a.b" },
			"cache_invalidator.kafka.topic_prefix",
		},
		{"invalidator kafka group", func(c *conf) { inval(c).Kafka.Group = "" }, "cache_invalidator.kafka.group"},
		{
			"invalidator kafka sasl",
			func(c *conf) { inval(c).Kafka.SASL.Mechanism = "PLAIN" },
			"cache_invalidator.kafka.sasl.mechanism",
		},
		{
			"invalidator kafka tls",
			func(c *conf) { inval(c).Kafka.TLS.KeyFile = "k" },
			"cache_invalidator.kafka.tls.cert_file",
		},
		{"invalidator nats url", func(c *conf) {
			inval(c).Source = config.SourceNATS
			inval(c).NATS.URL = ""
		}, "cache_invalidator.nats.url"},
		{"invalidator nats prefix", func(c *conf) {
			inval(c).Source = config.SourceNATS
			inval(c).NATS.SubjectPrefix = "a b"
		}, "cache_invalidator.nats.subject_prefix"},
		{"invalidator nats durable", func(c *conf) {
			inval(c).Source = config.SourceNATS
			inval(c).NATS.Durable = ""
		}, "cache_invalidator.nats.durable"},
		{"invalidator nats stream", func(c *conf) {
			inval(c).Source = config.SourceNATS
			inval(c).NATS.Stream = ""
		}, "cache_invalidator.nats.stream"},
		{"invalidator nats tls", func(c *conf) {
			inval(c).Source = config.SourceNATS
			inval(c).NATS.TLS.CertFile = "c"
		}, "cache_invalidator.nats.tls.cert_file"},
		{
			"invalidator namespace missing",
			func(c *conf) { inval(c).Namespace = "" },
			"cache_invalidator.namespace: required",
		},
		{
			"invalidator templates missing",
			func(c *conf) { inval(c).Templates = nil },
			"cache_invalidator.templates: at least one",
		},
		{"template key without dot", func(c *conf) {
			inval(c).Templates = map[string][]string{"users": {"user:{id}"}}
		}, `cache_invalidator.templates["users"]: key must have the form`},
		{"template list empty", func(c *conf) {
			inval(c).Templates = map[string][]string{"public.users": {}}
		}, `cache_invalidator.templates["public.users"]: at least one key template`},
		{"template blank", func(c *conf) {
			inval(c).Templates = map[string][]string{"public.users": {" "}}
		}, `cache_invalidator.templates["public.users"][0]: template must not be empty`},
		{"template unbalanced", func(c *conf) {
			inval(c).Templates = map[string][]string{"public.users": {"user:{id"}}
		}, `cache_invalidator.templates["public.users"][0]`},
		{"template empty placeholder", func(c *conf) {
			inval(c).Templates = map[string][]string{"public.users": {"user:{}"}}
		}, "unbalanced or empty {placeholder}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := loadExample(t)
			tc.mutate(&cfg)

			err := cfg.Validate()

			require.Error(t, err)
			require.ErrorIs(t, err, config.ErrInvalid)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestValidate_ReportsAllErrorsAtOnce(t *testing.T) {
	t.Parallel()
	cfg := loadExample(t)
	cfg.Source.Name = ""
	cfg.Snapshot.ChunkRows = 0
	cfg.Lag.WarnBytes = cfg.Lag.CriticalBytes
	cfg.Admin.Listen = "nope"
	cfg.Sinks[sinkKafka].Kafka.Brokers = nil
	cfg.Sinks[sinkWebhook].Webhook.Lanes = 0
	cfg.CacheInvalidator.Templates = map[string][]string{"users": {"x"}}

	err := cfg.Validate()

	require.Error(t, err)
	for _, key := range []string{
		"source.name", "snapshot.chunk_rows", "lag.warn_bytes", "admin.listen",
		"sinks[0].kafka.brokers", "sinks[2].webhook.lanes", "cache_invalidator.templates",
	} {
		require.ErrorContains(t, err, key)
	}
	var joined interface{ Unwrap() []error }
	require.ErrorAs(t, err, &joined)
	assert.Len(t, joined.Unwrap(), 7)
	for _, e := range joined.Unwrap() {
		var ve *config.ValidationError
		require.ErrorAs(t, e, &ve)
		assert.NotEmpty(t, ve.Path)
		assert.NotEmpty(t, ve.Message)
		assert.Equal(t, ve.Path+": "+ve.Message, e.Error())
	}
}

func TestParse_ReportsAllValidationErrorsAtOnce(t *testing.T) {
	t.Parallel()

	_, err := parse(t, "snapshot: {chunk_rows: 0, parallel_tables: 0}\nadmin: {listen: nope}\n", config.Options{})

	require.Error(t, err)
	keys := []string{"source.name", "snapshot.chunk_rows", "snapshot.parallel_tables", "admin.listen", "sinks"}
	for _, key := range keys {
		require.ErrorContains(t, err, key)
	}
}

func TestValidateCacheInvalidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(c *conf)
		want   []string
	}{
		{"unconfigured kafka source", func(*conf) {}, []string{
			"cache_invalidator.namespace", "cache_invalidator.redis.addrs", "cache_invalidator.templates",
			"cache_invalidator.kafka.brokers", "cache_invalidator.kafka.topic_prefix",
		}},
		{"unconfigured nats source", func(c *conf) { inval(c).Source = config.SourceNATS }, []string{
			"cache_invalidator.nats.url", "cache_invalidator.nats.subject_prefix", "cache_invalidator.nats.stream",
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := config.Default()
			tc.mutate(&cfg)

			err := cfg.ValidateCacheInvalidator()

			require.Error(t, err)
			for _, w := range tc.want {
				require.ErrorContains(t, err, w)
			}
			assert.NotContains(t, err.Error(), "source.name: ", "other sections are ignored")
		})
	}

	require.NoError(t, loadExample(t).ValidateCacheInvalidator())
}

func TestValidationError_Is(t *testing.T) {
	t.Parallel()

	err := error(&config.ValidationError{Path: "a.b", Message: "bad"})

	require.ErrorIs(t, err, config.ErrInvalid)
	require.NotErrorIs(t, err, errors.New("other"))
	assert.Equal(t, "a.b: bad", err.Error())
}
