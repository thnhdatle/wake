// SPDX-License-Identifier: Apache-2.0

// Package l2fake is an in-memory model of the Redis L2 used by unit tests of
// pkg/cache. A Server models one Redis deployment: a keyspace of strings with
// millisecond expiry driven by an injected clock, a monotonic microsecond
// TIME, the per-key scripts of the l2 contract executed atomically under one
// lock, and pub/sub channels with bounded per-subscriber buffers (an overflow
// drops the subscriber's connection, as Redis' client-output-buffer-limit
// does). A Client is one cache instance's connection: it implements
// l2.Backend and offers failure injection (unreachable, bus disconnect,
// paused delivery, per-operation hooks).
//
//	srv := l2fake.NewServer(clk)
//	cli := srv.Client(l2.Config{Namespace: "users", Channel: "wake:inval:users"})
//	cli.SetDown(true) // every command fails, the bus reports a disconnect
package l2fake
