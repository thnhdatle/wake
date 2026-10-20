// SPDX-License-Identifier: Apache-2.0

package config

import "time"

// Defaults, as documented in docs/guides/configuration.md.
const (
	DefaultAdminListen      = "127.0.0.1:9093"
	DefaultInvalidatorAdmin = "127.0.0.1:9096"
	DefaultDSNEnv           = "WAKE_SOURCE_DSN"
	DefaultInvalidatorGroup = "wake-cache-invalidator"
	DefaultBusChannelPrefix = "wake:inval:"

	defaultSinkMaxEvents      = 1000
	defaultSinkMaxBytes       = 1 * MiB
	defaultSinkLinger         = 10 * time.Millisecond
	defaultKafkaMaxEventBytes = 1 * MiB
	defaultWebhookMaxEvent    = 10 * MiB
	defaultNATSDupWindow      = 2 * time.Minute
	defaultWebhookTimeout     = 10 * time.Second
	defaultWebhookLanes       = 8
	defaultWebhookAttempts    = 10
	brokerDefault             = -1
)

// Default returns the configuration with every default applied. It is not
// valid on its own: source.name, tables.include (managed mode) and at least
// one sink must be configured.
func Default() Config {
	return Config{
		Source: Source{
			DSNEnv:      DefaultDSNEnv,
			Publication: Publication{Mode: PublicationManaged},
		},
		Tables: Tables{RefreshInterval: Duration(time.Minute)},
		Snapshot: Snapshot{
			Mode:           SnapshotInitial,
			AddedTables:    true,
			ChunkRows:      10000,
			ParallelTables: 4,
		},
		Stream: Stream{
			MaxTxBytes:      ByteSize(256 * MiB),
			SpillDir:        "./spill",
			StatusInterval:  Duration(10 * time.Second),
			MinConfirmBytes: ByteSize(1 * MiB),
		},
		State: State{Path: "./wake-state.json"},
		Lag: Lag{
			Interval:      Duration(15 * time.Second),
			WarnBytes:     ByteSize(1 * GiB),
			CriticalBytes: ByteSize(10 * GiB),
			MaxAckAge:     Duration(5 * time.Minute),
		},
		Heartbeat: Heartbeat{Interval: Duration(time.Minute)},
		Admin:     Admin{Listen: DefaultAdminListen},
		CacheInvalidator: CacheInvalidator{
			AdminListen:    DefaultInvalidatorAdmin,
			Source:         SourceKafka,
			Kafka:          InvalidatorKafka{Group: DefaultInvalidatorGroup},
			NATS:           InvalidatorNATS{Durable: DefaultInvalidatorGroup},
			ValidateDSNEnv: DefaultDSNEnv,
		},
	}
}

// DefaultBusChannel returns the default cache bus channel for a namespace.
func DefaultBusChannel(namespace string) string { return DefaultBusChannelPrefix + namespace }

// DefaultNATSStream returns the default JetStream stream name for a source.
func DefaultNATSStream(sourceName string) string { return "wake_" + sourceName }

// UnmarshalYAML presets the per-sink defaults, then reads the entry over them.
// It uses the callback form so the decoder's strict (unknown key) mode applies.
func (s *Sink) UnmarshalYAML(unmarshal func(any) error) error {
	type plain Sink
	p := plain{
		Tables:    []string{"*"},
		MaxEvents: defaultSinkMaxEvents,
		MaxBytes:  ByteSize(defaultSinkMaxBytes),
		Linger:    Duration(defaultSinkLinger),
	}
	if err := unmarshal(&p); err != nil {
		return err
	}
	*s = Sink(p)
	return nil
}

// UnmarshalYAML presets the kafka.* defaults, then reads the section over them.
func (k *KafkaSink) UnmarshalYAML(unmarshal func(any) error) error {
	type plain KafkaSink
	p := plain{Tombstones: true, Partitions: brokerDefault, ReplicationFactor: brokerDefault}
	if err := unmarshal(&p); err != nil {
		return err
	}
	*k = KafkaSink(p)
	return nil
}

// UnmarshalYAML presets the nats.* defaults, then reads the section over them.
func (n *NATSSink) UnmarshalYAML(unmarshal func(any) error) error {
	type plain NATSSink
	p := plain{CreateStream: true, DuplicateWindow: Duration(defaultNATSDupWindow)}
	if err := unmarshal(&p); err != nil {
		return err
	}
	*n = NATSSink(p)
	return nil
}

// UnmarshalYAML presets the webhook.* defaults, then reads the section over them.
func (w *WebhookSink) UnmarshalYAML(unmarshal func(any) error) error {
	type plain WebhookSink
	p := plain{
		Timeout:     Duration(defaultWebhookTimeout),
		Lanes:       defaultWebhookLanes,
		MaxAttempts: defaultWebhookAttempts,
	}
	if err := unmarshal(&p); err != nil {
		return err
	}
	*w = WebhookSink(p)
	return nil
}

// finalize fills the defaults that depend on other values: per-type sink
// event limits, the NATS stream names and the cache bus channel. It runs
// after files, env vars and flag overrides are merged.
func (c *Config) finalize() {
	for i := range c.Sinks {
		s := &c.Sinks[i]
		if s.MaxEventBytes == 0 {
			switch s.Type {
			case SinkKafka:
				s.MaxEventBytes = ByteSize(defaultKafkaMaxEventBytes)
			case SinkWebhook:
				s.MaxEventBytes = ByteSize(defaultWebhookMaxEvent)
			case SinkNATS, SinkStdout:
				// NATS: 0 = the server's max_payload; stdout has no limit.
			}
		}
		if s.NATS != nil && s.NATS.Stream == "" && c.Source.Name != "" {
			s.NATS.Stream = DefaultNATSStream(c.Source.Name)
		}
	}
	ci := &c.CacheInvalidator
	if ci.BusChannel == "" && ci.Namespace != "" {
		ci.BusChannel = DefaultBusChannel(ci.Namespace)
	}
	if ci.NATS.Stream == "" && c.Source.Name != "" {
		ci.NATS.Stream = DefaultNATSStream(c.Source.Name)
	}
}
