// SPDX-License-Identifier: Apache-2.0

// Package kafkatest starts a single-node Kafka (KRaft, apache/kafka as in
// deploy/docker-compose.yml, 512 MiB heap) in a container for integration
// tests, plain or with a SASL_SSL listener (tlsgen certificates, SCRAM-SHA-256
// and SCRAM-SHA-512 users created at format time). Clients reach it through
// a tcpproxy on a fixed loopback port that the broker advertises, so the
// test can simulate an outage (Outage, Restore) or restart the container on
// a new host port (Restart) without changing the clients' configuration.
// Topics are never auto-created by the broker. The container is terminated
// when the test ends.
//
//	k := kafkatest.Start(t, kafkatest.WithTLSAndSCRAM(kafkatest.User{Name: "wake", Password: "secret"}))
//	k.CreateTopic(t, "cdc.public.users", 3, nil)
//	recs := k.Consume(t, "cdc.public.users", 10, time.Minute)
package kafkatest
