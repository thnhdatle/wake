// SPDX-License-Identifier: Apache-2.0

// Package tlsgen generates a throw-away certificate authority with a server
// and a client certificate for TLS integration tests (Kafka, NATS, Redis,
// Postgres). Files are written to a test temp directory as PEM.
//
//	b := tlsgen.Generate(t, "localhost", "127.0.0.1", "kafka")
//	cfg := b.ClientTLS() // trusts the CA and presents the client certificate
package tlsgen
