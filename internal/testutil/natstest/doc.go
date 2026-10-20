// SPDX-License-Identifier: Apache-2.0

// Package natstest starts a NATS JetStream server (nats:2-alpine as in
// deploy/docker-compose.yml) in a container for integration tests: plain,
// with TLS (tlsgen certificates), with decentralized authentication
// (operator mode with a memory resolver, credentials files), and with a
// custom max_payload. The operator, account and user JWTs are generated
// with github.com/nats-io/nkeys alone, following the jwt/v2 claims, because
// github.com/nats-io/jwt is not a dependency. Clients reach the server
// through a tcpproxy on a fixed loopback port, so the test can simulate an
// outage (Outage, Restore) or restart the container on a new host port
// (Restart) without changing the clients' configuration. The container is
// terminated when the test ends.
//
//	n := natstest.Start(t, natstest.WithTLS(), natstest.WithCredentials())
//	cfg.NATS.URL, cfg.NATS.CredentialsFile = n.URL, n.CredsFile
//	js := n.JetStream(t)
package natstest
