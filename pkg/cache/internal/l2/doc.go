// SPDX-License-Identifier: Apache-2.0

// Package l2 defines the contract between the cache core (pkg/cache) and an
// L2 implementation: the Redis Lua scripts in pkg/cache/redisl2 and the
// in-memory fake in pkg/cache/internal/l2fake used by unit tests.
//
// Main types: Backend (Store, Locker, EpochStore and Bus), the request and
// result structs of each operation, Event (the decoded bus message) and the
// key, value and message encodings both implementations must share.
//
// Key layout: <ns>:e<epoch>:{<key>}:v (value), :g (generation),
// :l (lock); the namespace epoch lives in <ns>:epoch. Every per-key operation
// touches only keys carrying the same {<key>} hash tag, so Redis Cluster works.
//
//	v, g, l := l2.KeyNames("users", 7, "user:42")
//	// v == "users:e7:{user:42}:v"
package l2
