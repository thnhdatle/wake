// SPDX-License-Identifier: Apache-2.0

package tlsgen_test

import (
	"crypto/tls"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/testutil/tlsgen"
)

func TestGenerate_MutualTLSHandshake(t *testing.T) {
	t.Parallel()
	b := tlsgen.Generate(t, "localhost", "127.0.0.1")
	for _, f := range []string{b.CAFile, b.CertFile, b.KeyFile, b.ClientCertFile, b.ClientKeyFile} {
		_, err := os.Stat(f)
		require.NoError(t, err)
	}
	srvCfg := b.ServerTLS(t)
	srvCfg.ClientAuth = tls.RequireAndVerifyClientCert
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	require.NoError(t, err)
	defer ln.Close()
	done := make(chan error, 1)
	ctx := t.Context()
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			done <- aerr
			return
		}
		defer c.Close()
		done <- c.(*tls.Conn).HandshakeContext(ctx) //nolint:forcetypeassert // tls.Listen returns *tls.Conn
	}()
	cliCfg := b.ClientTLS(t)
	cliCfg.ServerName = "localhost"
	d := tls.Dialer{Config: cliCfg}
	nc, err := d.DialContext(ctx, "tcp", ln.Addr().String())
	require.NoError(t, err)
	conn := nc.(*tls.Conn) //nolint:forcetypeassert // tls.Dialer returns *tls.Conn
	require.NoError(t, conn.HandshakeContext(ctx))
	require.NoError(t, conn.Close())
	require.NoError(t, <-done)
}

func TestGenerate_IPAndDNSNames(t *testing.T) {
	t.Parallel()
	b := tlsgen.Generate(t, "kafka", "10.0.0.1")
	cert, err := tls.X509KeyPair(b.CertPEM, b.KeyPEM)
	require.NoError(t, err)
	leaf := cert.Leaf
	require.NotNil(t, leaf)
	assert.Equal(t, []string{"kafka"}, leaf.DNSNames)
	assert.True(t, leaf.IPAddresses[0].Equal(net.ParseIP("10.0.0.1")))
}
