// SPDX-License-Identifier: Apache-2.0

package config

// Config is the complete Wake configuration (docs/guides/configuration.md). It is built by
// Load or Parse and must be treated as immutable afterwards.
type Config struct {
	Source           Source           `yaml:"source"`
	Tables           Tables           `yaml:"tables"`
	Events           Events           `yaml:"events"`
	Snapshot         Snapshot         `yaml:"snapshot"`
	Stream           Stream           `yaml:"stream"`
	State            State            `yaml:"state"`
	Sinks            []Sink           `yaml:"sinks"`
	Lag              Lag              `yaml:"lag"`
	Heartbeat        Heartbeat        `yaml:"heartbeat"`
	Admin            Admin            `yaml:"admin"`
	CacheInvalidator CacheInvalidator `yaml:"cache_invalidator"`
}

// PublicationMode selects who owns the source publication.
type PublicationMode string

// Publication modes.
const (
	PublicationManaged  PublicationMode = "managed"
	PublicationExisting PublicationMode = "existing"
)

// Source configures the source Postgres.
type Source struct {
	// Name is the source name: slot and managed publication wake_<name>.
	Name string `yaml:"name"`
	// DSNEnv names the environment variable that holds the DSN.
	DSNEnv      string      `yaml:"dsn_env"`
	Publication Publication `yaml:"publication"`
}

// Publication configures the publication Wake reads from.
type Publication struct {
	Mode PublicationMode `yaml:"mode"`
	// Name is the publication used in existing mode.
	Name string `yaml:"name"`
}

// Tables selects the captured tables.
type Tables struct {
	Include         []string `yaml:"include"`
	Exclude         []string `yaml:"exclude"`
	RefreshInterval Duration `yaml:"refresh_interval"`
	SkipUnkeyed     bool     `yaml:"skip_unkeyed"`
	EmitUnkeyed     bool     `yaml:"emit_unkeyed"`
	// NameMap maps "<schema>.<table>" to the "<schema>.<table>" used in events and topics.
	NameMap map[string]string `yaml:"name_map"`
}

// Events toggles optional event kinds.
type Events struct {
	EmitTruncate bool `yaml:"emit_truncate"`
	EmitMessages bool `yaml:"emit_messages"`
}

// SnapshotMode selects the initial snapshot behavior.
type SnapshotMode string

// Snapshot modes.
const (
	SnapshotInitial SnapshotMode = "initial"
	SnapshotNever   SnapshotMode = "never"
)

// Snapshot configures the initial snapshot and snapshots of added tables.
type Snapshot struct {
	Mode           SnapshotMode `yaml:"mode"`
	AddedTables    bool         `yaml:"added_tables"`
	ChunkRows      int          `yaml:"chunk_rows"`
	ParallelTables int          `yaml:"parallel_tables"`
}

// Stream configures transaction assembly and acknowledgements.
type Stream struct {
	MaxTxBytes      ByteSize `yaml:"max_tx_bytes"`
	SpillDir        string   `yaml:"spill_dir"`
	StatusInterval  Duration `yaml:"status_interval"`
	MinConfirmBytes ByteSize `yaml:"min_confirm_bytes"`
}

// State configures the state file.
type State struct {
	Path string `yaml:"path"`
}

// SinkType names a sink implementation.
type SinkType string

// Sink types.
const (
	SinkKafka   SinkType = "kafka"
	SinkNATS    SinkType = "nats"
	SinkWebhook SinkType = "webhook"
	SinkStdout  SinkType = "stdout"
)

// Sink is one destination. Exactly the sub-section matching Type is set.
type Sink struct {
	Name   string   `yaml:"name"`
	Type   SinkType `yaml:"type"`
	Tables []string `yaml:"tables"`

	MaxEvents int      `yaml:"max_events"`
	MaxBytes  ByteSize `yaml:"max_bytes"`
	Linger    Duration `yaml:"linger"`
	// MaxEventBytes is the per-event limit; 0 on a NATS sink means "use the server's max_payload".
	MaxEventBytes ByteSize `yaml:"max_event_bytes"`

	Kafka   *KafkaSink   `yaml:"kafka,omitempty"`
	NATS    *NATSSink    `yaml:"nats,omitempty"`
	Webhook *WebhookSink `yaml:"webhook,omitempty"`
}

// KafkaSink holds the kafka.* options of a sink.
type KafkaSink struct {
	Brokers          []string `yaml:"brokers"`
	TopicPrefix      string   `yaml:"topic_prefix"`
	Tombstones       bool     `yaml:"tombstones"`
	AutoCreateTopics bool     `yaml:"auto_create_topics"`
	// Partitions and ReplicationFactor of auto-created topics; -1 = broker default.
	Partitions        int  `yaml:"partitions"`
	ReplicationFactor int  `yaml:"replication_factor"`
	TLS               TLS  `yaml:"tls"`
	SASL              SASL `yaml:"sasl"`
}

// NATSSink holds the nats.* options of a sink.
type NATSSink struct {
	URL           string `yaml:"url"`
	SubjectPrefix string `yaml:"subject_prefix"`
	CreateStream  bool   `yaml:"create_stream"`
	// Stream defaults to wake_<source.name>.
	Stream          string   `yaml:"stream"`
	DuplicateWindow Duration `yaml:"duplicate_window"`
	CredentialsFile string   `yaml:"credentials_file"`
	TLS             TLS      `yaml:"tls"`
}

// WebhookSink holds the webhook.* options of a sink.
type WebhookSink struct {
	URL string `yaml:"url"`
	// SecretEnv names the environment variable that holds the HMAC secret.
	SecretEnv   string   `yaml:"secret_env"`
	Timeout     Duration `yaml:"timeout"`
	Lanes       int      `yaml:"lanes"`
	MaxAttempts int      `yaml:"max_attempts"`
	// DeadLetterDir is opt-in; empty makes a poison batch block the sink.
	DeadLetterDir string `yaml:"dead_letter_dir"`
}

// TLS configures a TLS client connection.
type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// SASLMechanism names a SASL/SCRAM mechanism.
type SASLMechanism string

// SASL mechanisms; the empty mechanism disables SASL.
const (
	SASLNone        SASLMechanism = ""
	SASLScramSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLScramSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// SASL configures SASL/SCRAM authentication; credentials come from env vars.
type SASL struct {
	Mechanism   SASLMechanism `yaml:"mechanism"`
	UsernameEnv string        `yaml:"username_env"`
	PasswordEnv string        `yaml:"password_env"`
}

// Lag configures lag monitoring and alerts.
type Lag struct {
	Interval      Duration `yaml:"interval"`
	WarnBytes     ByteSize `yaml:"warn_bytes"`
	CriticalBytes ByteSize `yaml:"critical_bytes"`
	MaxAckAge     Duration `yaml:"max_ack_age"`
	AlertWebhook  string   `yaml:"alert_webhook"`
}

// Heartbeat configures the idle-source heartbeat.
type Heartbeat struct {
	Interval Duration `yaml:"interval"`
}

// Admin configures the admin HTTP listener.
type Admin struct {
	Listen string `yaml:"listen"`
}

// InvalidatorSource selects where the cache invalidator reads events from.
type InvalidatorSource string

// Invalidator sources.
const (
	SourceKafka InvalidatorSource = "kafka"
	SourceNATS  InvalidatorSource = "nats"
)

// CacheInvalidator configures `wake cache-invalidator`.
type CacheInvalidator struct {
	AdminListen string            `yaml:"admin_listen"`
	Source      InvalidatorSource `yaml:"source"`
	Kafka       InvalidatorKafka  `yaml:"kafka"`
	NATS        InvalidatorNATS   `yaml:"nats"`
	Redis       Redis             `yaml:"redis"`
	Namespace   string            `yaml:"namespace"`
	// BusChannel defaults to wake:inval:<namespace>.
	BusChannel           string `yaml:"bus_channel"`
	InvalidateOnSnapshot bool   `yaml:"invalidate_on_snapshot"`
	// ValidateDSNEnv names the env var of the DSN used to check replica identity; "" = runtime check only.
	ValidateDSNEnv string `yaml:"validate_dsn_env"`
	// Templates maps "<schema>.<table>" to key templates such as "user:{id}".
	Templates map[string][]string `yaml:"templates"`
}

// InvalidatorKafka is the Kafka consumer of the cache invalidator.
type InvalidatorKafka struct {
	Brokers     []string `yaml:"brokers"`
	TopicPrefix string   `yaml:"topic_prefix"`
	Group       string   `yaml:"group"`
	TLS         TLS      `yaml:"tls"`
	SASL        SASL     `yaml:"sasl"`
}

// InvalidatorNATS is the NATS consumer of the cache invalidator.
type InvalidatorNATS struct {
	URL           string `yaml:"url"`
	SubjectPrefix string `yaml:"subject_prefix"`
	// Stream defaults to wake_<source.name>.
	Stream          string `yaml:"stream"`
	Durable         string `yaml:"durable"`
	CredentialsFile string `yaml:"credentials_file"`
	TLS             TLS    `yaml:"tls"`
}

// Redis is the cache backend the invalidator deletes from.
type Redis struct {
	// Addrs: several addresses select Cluster; MasterName set selects Sentinel.
	Addrs       []string `yaml:"addrs"`
	MasterName  string   `yaml:"master_name"`
	PasswordEnv string   `yaml:"password_env"`
	TLS         TLS      `yaml:"tls"`
}

// Configured reports whether the cache_invalidator section was filled in
// (namespace, templates or Redis addresses set). Validate checks the section
// strictly only then; ValidateCacheInvalidator always does.
func (c CacheInvalidator) Configured() bool {
	return c.Namespace != "" || len(c.Templates) > 0 || len(c.Redis.Addrs) > 0
}
