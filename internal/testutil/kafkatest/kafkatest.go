// SPDX-License-Identifier: Apache-2.0

package kafkatest

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/thnhdatle/wake/internal/testutil/tcpproxy"
	"github.com/thnhdatle/wake/internal/testutil/tlsgen"
)

// DefaultImage is the Kafka image of deploy/docker-compose.yml; the
// WAKE_TEST_KAFKA_IMAGE environment variable overrides it.
const DefaultImage = "apache/kafka:3.9.0"

const (
	clientPort   = "9093/tcp"
	clusterID    = "MkU3OEVBNTcwNTJENDM2Qk"
	startScript  = "/tmp/wake/start.sh"
	propsFile    = "/tmp/wake/server.properties"
	keystoreFile = "/tmp/wake/keystore.pem"
	caFile       = "/tmp/wake/ca.pem"
	startTimeout = 3 * time.Minute
	memoryLimit  = 1 << 30
	stopTimeout  = 20 * time.Second
	readyTimeout = 2 * time.Minute
)

// Kafka is a single-node KRaft broker in a container, reached through a
// loopback proxy so it survives restarts on a new host port.
type Kafka struct {
	ctr   *testcontainers.DockerContainer
	proxy *tcpproxy.Proxy
	// Brokers is the seed list ("127.0.0.1:<port>").
	Brokers []string
	// TLS holds the certificates when the broker listens with TLS.
	TLS *tlsgen.Bundle
	// Users are the SCRAM users of a SASL_SSL listener; test clients
	// authenticate as the first.
	Users []User
}

// User is a SCRAM user name and password (letters and digits only).
type User struct {
	Name     string
	Password string
}

// Option configures Start.
type Option func(*options)

type options struct {
	tls   bool
	users []User
}

// WithTLSAndSCRAM makes the client listener SASL_SSL: certificates from
// tlsgen, client certificates requested but optional, and SCRAM-SHA-256 and
// SCRAM-SHA-512 credentials for every user (bootstrapped at format time).
func WithTLSAndSCRAM(users ...User) Option {
	return func(o *options) { o.tls, o.users = true, users }
}

// Start runs a broker for the test and terminates it at the end. Topics are
// never auto-created; the heap is 512 MiB.
func Start(tb testing.TB, opts ...Option) *Kafka {
	tb.Helper()
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	k := &Kafka{proxy: tcpproxy.Start(tb, ""), Users: o.users}
	k.Brokers = []string{k.proxy.Addr()}
	files := []testcontainers.ContainerFile{
		textFile(propsFile, k.properties(o), 0o644), textFile(startScript, startCommand(o.users), 0o755),
	}
	if o.tls {
		k.TLS = tlsgen.Generate(tb, "127.0.0.1", "localhost")
		keystore := string(k.TLS.KeyPEM) + string(k.TLS.CertPEM)
		files = append(files, textFile(keystoreFile, keystore, 0o644), textFile(caFile, string(k.TLS.CAPEM), 0o644))
	}
	ctx, cancel := context.WithTimeout(tb.Context(), startTimeout)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, image(),
		testcontainers.WithExposedPorts(clientPort),
		testcontainers.WithEnv(map[string]string{"KAFKA_HEAP_OPTS": "-Xms512m -Xmx512m"}),
		testcontainers.WithEntrypoint("/bin/bash", startScript),
		testcontainers.WithFiles(files...),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) { hc.Memory = memoryLimit }),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Server started").WithStartupTimeout(startTimeout)),
	)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("kafkatest: start %s: %v", image(), err)
	}
	k.ctr = ctr
	k.retarget(ctx, tb)
	k.waitReady(tb)
	return k
}

// listeners binds the client and inter-broker listeners on every
// interface; the controller listener stays inside the container.
const listeners = "EXTERNAL://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://localhost:9094"

func textFile(path, content string, mode int64) testcontainers.ContainerFile {
	return testcontainers.ContainerFile{Reader: strings.NewReader(content), ContainerFilePath: path, FileMode: mode}
}

func image() string {
	if img := os.Getenv("WAKE_TEST_KAFKA_IMAGE"); img != "" {
		return img
	}
	return DefaultImage
}

// properties renders server.properties: a combined broker and controller
// whose client listener EXTERNAL advertises the proxy.
func (k *Kafka) properties(o options) string {
	protocol := "PLAINTEXT"
	if o.tls {
		protocol = "SASL_SSL"
	}
	props := map[string]string{
		"process.roles":                            "broker,controller",
		"node.id":                                  "1",
		"controller.quorum.voters":                 "1@localhost:9094",
		"controller.listener.names":                "CONTROLLER",
		"listeners":                                listeners,
		"advertised.listeners":                     "EXTERNAL://" + k.proxy.Addr() + ",BROKER://localhost:9092",
		"listener.security.protocol.map":           "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,EXTERNAL:" + protocol,
		"inter.broker.listener.name":               "BROKER",
		"log.dirs":                                 "/tmp/kraft-logs",
		"num.partitions":                           "1",
		"auto.create.topics.enable":                "false",
		"offsets.topic.replication.factor":         "1",
		"transaction.state.log.replication.factor": "1",
		"transaction.state.log.min.isr":            "1",
		"group.initial.rebalance.delay.ms":         "0",
		"log.flush.interval.messages":              "9223372036854775807",
	}
	if o.tls {
		jaas := "org.apache.kafka.common.security.scram.ScramLoginModule required;"
		props["sasl.enabled.mechanisms"] = "SCRAM-SHA-256,SCRAM-SHA-512"
		props["listener.name.external.scram-sha-256.sasl.jaas.config"] = jaas
		props["listener.name.external.scram-sha-512.sasl.jaas.config"] = jaas
		props["ssl.keystore.type"] = "PEM"
		props["ssl.keystore.location"] = keystoreFile
		props["ssl.truststore.type"] = "PEM"
		props["ssl.truststore.location"] = caFile
		props["ssl.client.auth"] = "requested"
	}
	var b strings.Builder
	for _, key := range slices.Sorted(maps.Keys(props)) {
		fmt.Fprintf(&b, "%s=%s\n", key, props[key])
	}
	return b.String()
}

// startCommand formats the storage (idempotent across restarts, adding the
// SCRAM credentials the first time) and starts the server.
func startCommand(users []User) string {
	var b strings.Builder
	b.WriteString("set -e\n")
	fmt.Fprintf(&b, "/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t %s -c %s", clusterID, propsFile)
	for _, u := range users {
		for _, mech := range []string{"SCRAM-SHA-256", "SCRAM-SHA-512"} {
			fmt.Fprintf(&b, " --add-scram '%s=[name=%s,password=%s]'", mech, u.Name, u.Password)
		}
	}
	fmt.Fprintf(&b, "\nexec /opt/kafka/bin/kafka-server-start.sh %s\n", propsFile)
	return b.String()
}

// retarget points the proxy at the container's current host port.
func (k *Kafka) retarget(ctx context.Context, tb testing.TB) {
	tb.Helper()
	addr, err := k.ctr.PortEndpoint(ctx, clientPort, "")
	if err != nil {
		tb.Fatalf("kafkatest: mapped port: %v", err)
	}
	k.proxy.SetTarget(addr)
}

// waitReady polls the broker until it answers metadata requests.
func (k *Kafka) waitReady(tb testing.TB) {
	tb.Helper()
	cl := k.Client(tb)
	adm := kadm.NewClient(cl)
	deadline := time.Now().Add(readyTimeout)
	for {
		ctx, cancel := context.WithTimeout(tb.Context(), 5*time.Second)
		_, err := adm.ListBrokers(ctx)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("kafkatest: broker not ready: %v", err)
		}
		select {
		case <-tb.Context().Done():
			tb.Fatalf("kafkatest: %v", tb.Context().Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ClientOpts returns the franz-go options that reach the broker as the
// first SCRAM user (SCRAM-SHA-512) over TLS, or in plain text.
func (k *Kafka) ClientOpts(tb testing.TB) []kgo.Opt {
	tb.Helper()
	opts := []kgo.Opt{kgo.SeedBrokers(k.Brokers...)}
	if k.TLS == nil {
		return opts
	}
	opts = append(opts, kgo.DialTLSConfig(k.TLS.ClientTLS(tb)))
	if len(k.Users) > 0 {
		u := k.Users[0]
		opts = append(opts, kgo.SASL(scram.Auth{User: u.Name, Pass: u.Password}.AsSha512Mechanism()))
	}
	return opts
}

// Client returns a franz-go client closed at the end of the test.
func (k *Kafka) Client(tb testing.TB, extra ...kgo.Opt) *kgo.Client {
	tb.Helper()
	cl, err := kgo.NewClient(append(k.ClientOpts(tb), extra...)...)
	if err != nil {
		tb.Fatalf("kafkatest: client: %v", err)
	}
	tb.Cleanup(cl.Close)
	return cl
}

// Admin returns an admin client closed at the end of the test.
func (k *Kafka) Admin(tb testing.TB) *kadm.Client {
	tb.Helper()
	return kadm.NewClient(k.Client(tb))
}

// CreateTopic creates a topic with partitions partitions, replication
// factor 1 and the given configs.
func (k *Kafka) CreateTopic(tb testing.TB, topic string, partitions int32, configs map[string]*string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 30*time.Second)
	defer cancel()
	if _, err := k.Admin(tb).CreateTopic(ctx, partitions, 1, configs, topic); err != nil {
		tb.Fatalf("kafkatest: create topic %s: %v", topic, err)
	}
}

// Consume reads topic from the beginning until it has at least n records
// or timeout passes, and returns what it read.
func (k *Kafka) Consume(tb testing.TB, topic string, n int, timeout time.Duration) []*kgo.Record {
	tb.Helper()
	cl := k.Client(tb, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(tb.Context(), timeout)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// Outage drops every connection to the broker until Restore.
func (k *Kafka) Outage() { k.proxy.Down() }

// Restore ends an Outage.
func (k *Kafka) Restore() { k.proxy.Up() }

// Restart stops the container (the broker shuts down cleanly), starts it
// again on its data and waits until it serves requests.
func (k *Kafka) Restart(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), startTimeout)
	defer cancel()
	timeout := stopTimeout
	if err := k.ctr.Stop(ctx, &timeout); err != nil {
		tb.Fatalf("kafkatest: stop: %v", err)
	}
	if err := k.ctr.Start(ctx); err != nil {
		tb.Fatalf("kafkatest: start: %v", err)
	}
	k.retarget(ctx, tb)
	k.waitReady(tb)
}
