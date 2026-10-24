// SPDX-License-Identifier: Apache-2.0

package config

import "time"

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

// Default returns the configuration with every default applied.
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

func DefaultBusChannel(namespace string) string { return DefaultBusChannelPrefix + namespace }

func DefaultNATSStream(sourceName string) string { return "wake_" + sourceName }

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

func (k *KafkaSink) UnmarshalYAML(unmarshal func(any) error) error {
	type plain KafkaSink
	p := plain{Tombstones: true, Partitions: brokerDefault, ReplicationFactor: brokerDefault}
	if err := unmarshal(&p); err != nil {
		return err
	}
	*k = KafkaSink(p)
	return nil
}

func (n *NATSSink) UnmarshalYAML(unmarshal func(any) error) error {
	type plain NATSSink
	p := plain{CreateStream: true, DuplicateWindow: Duration(defaultNATSDupWindow)}
	if err := unmarshal(&p); err != nil {
		return err
	}
	*n = NATSSink(p)
	return nil
}

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
