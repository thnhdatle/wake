// SPDX-License-Identifier: Apache-2.0

package tlsgen

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Bundle holds the generated PEM files and their contents.
type Bundle struct {
	Dir            string
	CAFile         string
	CertFile       string // server certificate
	KeyFile        string // server key
	ClientCertFile string
	ClientKeyFile  string
	CAPEM          []byte
	CertPEM        []byte
	KeyPEM         []byte
	ClientCertPEM  []byte
	ClientKeyPEM   []byte
}

// Generate creates a CA, a server certificate valid for hosts (DNS names or
// IP addresses) and a client certificate, all valid for 24 hours. It fails
// the test on any error.
func Generate(tb testing.TB, hosts ...string) *Bundle {
	tb.Helper()
	dir := tb.TempDir()
	now := time.Now()
	caKey := newKey(tb)
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(tb),
		Subject:               pkix.Name{CommonName: "wake test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		tb.Fatalf("tlsgen: create CA: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		tb.Fatalf("tlsgen: parse CA: %v", err)
	}
	b := &Bundle{Dir: dir, CAPEM: encodeCert(caDER)}
	b.CertPEM, b.KeyPEM = leaf(tb, caCert, caKey, "wake test server", hosts, x509.ExtKeyUsageServerAuth)
	b.ClientCertPEM, b.ClientKeyPEM = leaf(tb, caCert, caKey, "wake test client", nil, x509.ExtKeyUsageClientAuth)
	b.CAFile = write(tb, dir, "ca.pem", b.CAPEM)
	b.CertFile = write(tb, dir, "server.pem", b.CertPEM)
	b.KeyFile = write(tb, dir, "server-key.pem", b.KeyPEM)
	b.ClientCertFile = write(tb, dir, "client.pem", b.ClientCertPEM)
	b.ClientKeyFile = write(tb, dir, "client-key.pem", b.ClientKeyPEM)
	return b
}

// CertPool returns a pool containing only the CA.
func (b *Bundle) CertPool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AppendCertsFromPEM(b.CAPEM)
	return p
}

// ServerTLS returns a server configuration that requires client
// certificates signed by the CA.
func (b *Bundle) ServerTLS(tb testing.TB) *tls.Config {
	tb.Helper()
	cert, err := tls.X509KeyPair(b.CertPEM, b.KeyPEM)
	if err != nil {
		tb.Fatalf("tlsgen: server key pair: %v", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    b.CertPool(),
		ClientAuth:   tls.VerifyClientCertIfGiven,
		MinVersion:   tls.VersionTLS12,
	}
}

// ClientTLS returns a client configuration trusting the CA and presenting
// the client certificate.
func (b *Bundle) ClientTLS(tb testing.TB) *tls.Config {
	tb.Helper()
	cert, err := tls.X509KeyPair(b.ClientCertPEM, b.ClientKeyPEM)
	if err != nil {
		tb.Fatalf("tlsgen: client key pair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: b.CertPool(), MinVersion: tls.VersionTLS12}
}

func leaf(tb testing.TB, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, hosts []string,
	usage x509.ExtKeyUsage,
) (certPEM, keyPEM []byte) {
	tb.Helper()
	key := newKey(tb)
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial(tb),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		tb.Fatalf("tlsgen: create %s: %v", cn, err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		tb.Fatalf("tlsgen: marshal key: %v", err)
	}
	return encodeCert(der), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func newKey(tb testing.TB) *ecdsa.PrivateKey {
	tb.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatalf("tlsgen: key: %v", err)
	}
	return k
}

func serial(tb testing.TB) *big.Int {
	tb.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		tb.Fatalf("tlsgen: serial: %v", err)
	}
	return n
}

func encodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func write(tb testing.TB, dir, name string, data []byte) string {
	tb.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		tb.Fatalf("tlsgen: write %s: %v", name, err)
	}
	return p
}
