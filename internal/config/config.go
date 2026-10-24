// SPDX-License-Identifier: Apache-2.0

package config

// Config is the complete Wake configuration.
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

type PublicationMode string

const (
	PublicationManaged  PublicationMode = "managed"
	PublicationExisting PublicationMode = "existing"
)

type Source struct {
	Name        string      `yaml:"name"`
	DSNEnv      string      `yaml:"dsn_env"`
	Publication Publication `yaml:"publication"`
}

type Publication struct {
	Mode PublicationMode `yaml:"mode"`
	Name string          `yaml:"name"`
}

type Tables struct {
	Include         []string          `yaml:"include"`
	Exclude         []string          `yaml:"exclude"`
	RefreshInterval Duration          `yaml:"refresh_interval"`
	SkipUnkeyed     bool              `yaml:"skip_unkeyed"`
	EmitUnkeyed     bool              `yaml:"emit_unkeyed"`
	NameMap         map[string]string `yaml:"name_map"`
}

type Events struct {
	EmitTruncate bool `yaml:"emit_truncate"`
	EmitMessages bool `yaml:"emit_messages"`
}

type SnapshotMode string

const (
	SnapshotInitial SnapshotMode = "initial"
	SnapshotNever   SnapshotMode = "never"
)

type Snapshot struct {
	Mode           SnapshotMode `yaml:"mode"`
	AddedTables    bool         `yaml:"added_tables"`
	ChunkRows      int          `yaml:"chunk_rows"`
	ParallelTables int          `yaml:"parallel_tables"`
}

type Stream struct {
	MaxTxBytes      ByteSize `yaml:"max_tx_bytes"`
	SpillDir        string   `yaml:"spill_dir"`
	StatusInterval  Duration `yaml:"status_interval"`
	MinConfirmBytes ByteSize `yaml:"min_confirm_bytes"`
}

type State struct {
	Path string `yaml:"path"`
}

type SinkType string

const (
	SinkKafka   SinkType = "kafka"
	SinkNATS    SinkType = "nats"
	SinkWebhook SinkType = "webhook"
	SinkStdout  SinkType = "stdout"
)

type Sink struct {
	Name   string   `yaml:"name"`
	Type   SinkType `yaml:"type"`
	Tables []string `yaml:"tables"`

	MaxEvents     int      `yaml:"max_events"`
	MaxBytes      ByteSize `yaml:"max_bytes"`
	Linger        Duration `yaml:"linger"`
	MaxEventBytes ByteSize `yaml:"max_event_bytes"`

	Kafka   *KafkaSink   `yaml:"kafka,omitempty"`
	NATS    *NATSSink    `yaml:"nats,omitempty"`
	Webhook *WebhookSink `yaml:"webhook,omitempty"`
}

type KafkaSink struct {
	Brokers           []string `yaml:"brokers"`
	TopicPrefix       string   `yaml:"topic_prefix"`
	Tombstones        bool     `yaml:"tombstones"`
	AutoCreateTopics  bool     `yaml:"auto_create_topics"`
	Partitions        int      `yaml:"partitions"`
	ReplicationFactor int      `yaml:"replication_factor"`
	TLS               TLS      `yaml:"tls"`
	SASL              SASL     `yaml:"sasl"`
}

type NATSSink struct {
	URL             string   `yaml:"url"`
	SubjectPrefix   string   `yaml:"subject_prefix"`
	CreateStream    bool     `yaml:"create_stream"`
	Stream          string   `yaml:"stream"`
	DuplicateWindow Duration `yaml:"duplicate_window"`
	CredentialsFile string   `yaml:"credentials_file"`
	TLS             TLS      `yaml:"tls"`
}

type WebhookSink struct {
	URL           string   `yaml:"url"`
	SecretEnv     string   `yaml:"secret_env"`
	Timeout       Duration `yaml:"timeout"`
	Lanes         int      `yaml:"lanes"`
	MaxAttempts   int      `yaml:"max_attempts"`
	DeadLetterDir string   `yaml:"dead_letter_dir"`
}

type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type SASLMechanism string

const (
	SASLNone        SASLMechanism = ""
	SASLScramSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLScramSHA512 SASLMechanism = "SCRAM-SHA-512"
)

type SASL struct {
	Mechanism   SASLMechanism `yaml:"mechanism"`
	UsernameEnv string        `yaml:"username_env"`
	PasswordEnv string        `yaml:"password_env"`
}

type Lag struct {
	Interval      Duration `yaml:"interval"`
	WarnBytes     ByteSize `yaml:"warn_bytes"`
	CriticalBytes ByteSize `yaml:"critical_bytes"`
	MaxAckAge     Duration `yaml:"max_ack_age"`
	AlertWebhook  string   `yaml:"alert_webhook"`
}

type Heartbeat struct {
	Interval Duration `yaml:"interval"`
}

type Admin struct {
	Listen string `yaml:"listen"`
}

type InvalidatorSource string

const (
	SourceKafka InvalidatorSource = "kafka"
	SourceNATS  InvalidatorSource = "nats"
)

type CacheInvalidator struct {
	AdminListen          string              `yaml:"admin_listen"`
	Source               InvalidatorSource   `yaml:"source"`
	Kafka                InvalidatorKafka    `yaml:"kafka"`
	NATS                 InvalidatorNATS     `yaml:"nats"`
	Redis                Redis               `yaml:"redis"`
	Namespace            string              `yaml:"namespace"`
	BusChannel           string              `yaml:"bus_channel"`
	InvalidateOnSnapshot bool                `yaml:"invalidate_on_snapshot"`
	ValidateDSNEnv       string              `yaml:"validate_dsn_env"`
	Templates            map[string][]string `yaml:"templates"`
}

type InvalidatorKafka struct {
	Brokers     []string `yaml:"brokers"`
	TopicPrefix string   `yaml:"topic_prefix"`
	Group       string   `yaml:"group"`
	TLS         TLS      `yaml:"tls"`
	SASL        SASL     `yaml:"sasl"`
}

type InvalidatorNATS struct {
	URL             string `yaml:"url"`
	SubjectPrefix   string `yaml:"subject_prefix"`
	Stream          string `yaml:"stream"`
	Durable         string `yaml:"durable"`
	CredentialsFile string `yaml:"credentials_file"`
	TLS             TLS    `yaml:"tls"`
}

type Redis struct {
	Addrs       []string `yaml:"addrs"`
	MasterName  string   `yaml:"master_name"`
	PasswordEnv string   `yaml:"password_env"`
	TLS         TLS      `yaml:"tls"`
}

func (c CacheInvalidator) Configured() bool {
	return c.Namespace != "" || len(c.Templates) > 0 || len(c.Redis.Addrs) > 0
}
