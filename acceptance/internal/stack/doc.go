// SPDX-License-Identifier: Apache-2.0

// Package stack drives the compose stack of deploy/docker-compose.yml for the
// acceptance tests: it checks that the dependencies are reachable (EnsureUp),
// runs the Wake container with a per-test configuration (ConfigureWake, Kill,
// Start, WaitReady, admin client, logs), and gives tests real clients and
// measurement helpers:
//
//   - Postgres (pgx): DDL and DML, pg_current_wal_lsn, slot confirmed LSN and
//     lag, WaitConfirmed (the "Wake confirmed the final LSN" step of M2).
//   - Kafka (franz-go): ReadTopics consumes topics from the beginning up to
//     the end offsets taken at the call, DecodeRecords classifies payloads
//     with pkg/event.
//   - Receiver: an HTTP endpoint the Wake container reaches through
//     host.docker.internal; it verifies webhook signatures and injects
//     retryable failures and slow responses.
//   - Load: an open-model (constant arrival rate) transaction generator over
//     N dedicated connections that records every committed change (M1 commit
//     time, table, key, ver).
//   - Checks: M3 lost and duplicates, M4 order violations; M2 final state is
//     internal/testutil/m2.
//
// Every operation is bounded by a context or timeout. Generated files live in
// the test's temporary directory; Wake container logs are saved to
// acceptance/logs/<ID>-wake.log at cleanup.
package stack
