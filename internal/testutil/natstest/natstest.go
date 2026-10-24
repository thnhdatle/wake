// SPDX-License-Identifier: Apache-2.0

package natstest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/thnhdatle/wake/internal/testutil/tcpproxy"
	"github.com/thnhdatle/wake/internal/testutil/tlsgen"
)

const DefaultImage = "nats:2-alpine"

const (
	clientPort   = "4222/tcp"
	confFile     = "/etc/wake/nats.conf"
	certDir      = "/etc/wake"
	startTimeout = 2 * time.Minute
	stopTimeout  = 10 * time.Second
	readyTimeout = time.Minute
	memoryLimit  = 256 << 20
)

// NATS is a JetStream server in a container.
type NATS struct {
	ctr       *testcontainers.DockerContainer
	proxy     *tcpproxy.Proxy
	trust     *trust
	dir       string
	URL       string
	TLS       *tlsgen.Bundle
	CredsFile string
}

type Option func(*options)

type options struct {
	tls        bool
	creds      bool
	maxPayload int
}

func WithTLS() Option { return func(o *options) { o.tls = true } }

func WithCredentials() Option { return func(o *options) { o.creds = true } }

func WithMaxPayload(n int) Option { return func(o *options) { o.maxPayload = n } }

// Start runs a JetStream server for the test and terminates it at the end.
func Start(tb testing.TB, opts ...Option) *NATS {
	tb.Helper()
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	n := &NATS{proxy: tcpproxy.Start(tb, ""), dir: tb.TempDir()}
	n.URL = "nats://" + n.proxy.Addr()
	var files []testcontainers.ContainerFile
	if o.tls {
		n.TLS = tlsgen.Generate(tb, "127.0.0.1", "localhost")
		for name, data := range map[string][]byte{
			"server.pem": n.TLS.CertPEM, "server-key.pem": n.TLS.KeyPEM, "ca.pem": n.TLS.CAPEM,
		} {
			files = append(files, testcontainers.ContainerFile{
				Reader: strings.NewReader(string(data)), ContainerFilePath: certDir + "/" + name, FileMode: 0o644,
			})
		}
	}
	if o.creds {
		n.trust = newTrust(tb)
		n.CredsFile = n.trust.userCreds(tb, n.dir, "wake", n.trust.account)
	}
	files = append(files, testcontainers.ContainerFile{
		Reader: strings.NewReader(n.config(o)), ContainerFilePath: confFile, FileMode: 0o644,
	})
	ctx, cancel := context.WithTimeout(tb.Context(), startTimeout)
	defer cancel()
	ctr, err := testcontainers.Run(ctx, image(),
		testcontainers.WithExposedPorts(clientPort),
		testcontainers.WithCmd("nats-server", "-c", confFile),
		testcontainers.WithFiles(files...),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) { hc.Memory = memoryLimit }),
		testcontainers.WithWaitStrategy(wait.ForLog("Server is ready").WithStartupTimeout(startTimeout)),
	)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("natstest: start %s: %v", image(), err)
	}
	n.ctr = ctr
	n.retarget(ctx, tb)
	n.waitReady(tb)
	return n
}

func image() string {
	if img := os.Getenv("WAKE_TEST_NATS_IMAGE"); img != "" {
		return img
	}
	return DefaultImage
}

func (n *NATS) config(o options) string {
	var b strings.Builder
	b.WriteString("listen: 0.0.0.0:4222\njetstream {\n  store_dir: \"/tmp/nats-js\"\n}\n")
	if o.maxPayload > 0 {
		fmt.Fprintf(&b, "max_payload: %d\n", o.maxPayload)
	}
	if o.tls {
		fmt.Fprintf(&b, "tls {\n  cert_file: %q\n  key_file: %q\n  ca_file: %q\n}\n",
			certDir+"/server.pem", certDir+"/server-key.pem", certDir+"/ca.pem")
	}
	if n.trust != nil {
		b.WriteString(n.trust.serverConfig())
	}
	return b.String()
}

func (n *NATS) retarget(ctx context.Context, tb testing.TB) {
	tb.Helper()
	addr, err := n.ctr.PortEndpoint(ctx, clientPort, "")
	if err != nil {
		tb.Fatalf("natstest: mapped port: %v", err)
	}
	n.proxy.SetTarget(addr)
}

func (n *NATS) waitReady(tb testing.TB) {
	tb.Helper()
	deadline := time.Now().Add(readyTimeout)
	for {
		err := n.ping(tb)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("natstest: server not ready: %v", err)
		}
		select {
		case <-tb.Context().Done():
			tb.Fatalf("natstest: %v", tb.Context().Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (n *NATS) ping(tb testing.TB) error {
	tb.Helper()
	nc, err := nats.Connect(n.URL, n.ClientOpts(tb)...)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	ctx, cancel := context.WithTimeout(tb.Context(), 5*time.Second)
	defer cancel()
	if _, err := js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("account info: %w", err)
	}
	return nil
}

func (n *NATS) ClientOpts(tb testing.TB) []nats.Option {
	tb.Helper()
	opts := []nats.Option{nats.Name("wake-test"), nats.Timeout(5 * time.Second)}
	if n.TLS != nil {
		opts = append(opts, nats.Secure(n.TLS.ClientTLS(tb)))
	}
	if n.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(n.CredsFile))
	}
	return opts
}

func (n *NATS) Connect(tb testing.TB) *nats.Conn {
	tb.Helper()
	nc, err := nats.Connect(n.URL, append(n.ClientOpts(tb), nats.MaxReconnects(-1))...)
	if err != nil {
		tb.Fatalf("natstest: connect: %v", err)
	}
	tb.Cleanup(nc.Close)
	return nc
}

func (n *NATS) JetStream(tb testing.TB) jetstream.JetStream {
	tb.Helper()
	js, err := jetstream.New(n.Connect(tb))
	if err != nil {
		tb.Fatalf("natstest: jetstream: %v", err)
	}
	return js
}

func (n *NATS) NewCredsFile(tb testing.TB, name string) string {
	tb.Helper()
	if n.trust == nil {
		tb.Fatalf("natstest: NewCredsFile needs WithCredentials")
	}
	return n.trust.userCreds(tb, n.dir, name, n.trust.account)
}

func (n *NATS) ForeignCredsFile(tb testing.TB, name string) string {
	tb.Helper()
	if n.trust == nil {
		tb.Fatalf("natstest: ForeignCredsFile needs WithCredentials")
	}
	return n.trust.userCreds(tb, n.dir, name, mustKey(tb, nkeys.CreateAccount))
}

func (n *NATS) Outage() { n.proxy.Down() }

func (n *NATS) Restore() { n.proxy.Up() }

func (n *NATS) Restart(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), startTimeout)
	defer cancel()
	timeout := stopTimeout
	if err := n.ctr.Stop(ctx, &timeout); err != nil {
		tb.Fatalf("natstest: stop: %v", err)
	}
	if err := n.ctr.Start(ctx); err != nil {
		tb.Fatalf("natstest: start: %v", err)
	}
	n.retarget(ctx, tb)
	n.waitReady(tb)
}
