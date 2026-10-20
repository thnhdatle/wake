// SPDX-License-Identifier: Apache-2.0

package redistest

import (
	"bytes"
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/thnhdatle/wake/internal/testutil/tlsgen"
)

// TLS is a Redis node that accepts only TLS connections and requires a
// client certificate signed by the test CA.
type TLS struct {
	addr   string
	bundle *tlsgen.Bundle
}

// StartTLS starts the TLS node; it is terminated when tb ends.
func StartTLS(tb testing.TB) *TLS {
	tb.Helper()
	b := tlsgen.Generate(tb, "localhost", "127.0.0.1")
	file := func(pem []byte, name string) testcontainers.ContainerFile {
		return testcontainers.ContainerFile{Reader: bytes.NewReader(pem), ContainerFilePath: "/tls/" + name, FileMode: 0o644}
	}
	ctr := startContainer(tb,
		testcontainers.WithExposedPorts(singlePort),
		testcontainers.WithFiles(file(b.CAPEM, "ca.pem"), file(b.CertPEM, "server.pem"), file(b.KeyPEM, "server-key.pem")),
		testcontainers.WithCmd("redis-server", "--port", "0", "--tls-port", "6379",
			"--tls-cert-file", "/tls/server.pem", "--tls-key-file", "/tls/server-key.pem",
			"--tls-ca-cert-file", "/tls/ca.pem", "--tls-auth-clients", "yes", "--save", "", "--appendonly", "no"),
		testcontainers.WithWaitStrategy(wait.ForLog("Ready to accept connections").WithStartupTimeout(startTimeout)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	addr, err := endpoint(ctx, ctr, singlePort)
	if err != nil {
		tb.Fatalf("redistest: tls endpoint: %v", err)
	}
	return &TLS{addr: addr, bundle: b}
}

// Addr returns the host:port of the TLS listener.
func (s *TLS) Addr() string { return s.addr }

// Bundle returns the CA, server and client certificates.
func (s *TLS) Bundle() *tlsgen.Bundle { return s.bundle }

// Options returns client options (short timeouts, no retries) that verify
// the server against the test CA and present the client certificate.
func (s *TLS) Options(tb testing.TB) *redis.Options {
	tb.Helper()
	opt := clientOptions(nil)
	opt.Addr = s.addr
	opt.TLSConfig = s.bundle.ClientTLS(tb)
	return opt
}

// Client returns a TLS client built from Options; it is closed when tb ends.
func (s *TLS) Client(tb testing.TB) *redis.Client {
	tb.Helper()
	rdb := newClient(tb, s.Options(tb))
	waitPing(tb, rdb)
	return rdb
}
