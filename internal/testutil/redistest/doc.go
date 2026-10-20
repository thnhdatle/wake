// SPDX-License-Identifier: Apache-2.0

// Package redistest starts Redis 7 containers for integration tests with
// Testcontainers: a single node (StartSingle) that can be stopped,
// restarted and frozen, a 3-master Cluster (StartCluster) and a TLS-only
// node (StartTLS) using certificates from internal/testutil/tlsgen. Every
// container is terminated when the test that started it ends. Only tests
// import it.
//
// Main types: Single, Cluster, TLS.
//
// Addresses. Published ports are random and change when a container is
// restarted, so clients get a Dialer that resolves the current host port at
// dial time instead of a fixed address.
//
// Cluster on Docker Desktop. Cluster nodes tell clients where slots live
// (CLUSTER SLOTS, MOVED); container addresses are not reachable from a macOS
// host. The three nodes therefore run in one container on ports 7000-7002
// with cluster-announce-ip 127.0.0.1 (they reach each other, and their bus
// ports, over the container's loopback), and the client's Dialer maps every
// announced 127.0.0.1:700x to the host port Docker published for 700x. No
// fixed host ports or host networking are needed, and the same works on any
// Linux Docker.
//
//	srv := redistest.StartSingle(t)
//	rdb := srv.Client(t) // closed when t ends
//	srv.Stop(t)          // SIGTERM: Redis saves its dataset, then exits
//	srv.Start(t)         // dataset reloaded, script cache empty
package redistest
