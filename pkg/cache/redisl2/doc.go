// SPDX-License-Identifier: Apache-2.0

// Package redisl2 is the Redis implementation of the pkg/cache L2 contract
// (pkg/cache/internal/l2): generation-checked values, the per-key load lock,
// the namespace epoch and the pub/sub invalidation bus, over a go-redis
// UniversalClient (single node, Sentinel or Cluster). Applications use it
// through cache.WithL2; its request types live in an internal package, so it
// is not meant to be called directly.
//
// Main type: Backend, created by New.
//
// Scripts. Every per-key operation (Read, Fill, Set, Invalidate, Unlock) is
// one Lua script whose KEYS all carry the same {<key>} hash tag, so each runs
// atomically on one Cluster shard; Lock is a plain SET NX PX. The epoch key
// <ns>:epoch is only ever touched by its own scripts (Epoch, BumpEpoch). Each
// script reads Redis TIME once, in microseconds; a missing generation is
// initialized to it so a re-created generation never repeats an earlier value,
// and every script that touches a generation extends its TTL to at least
// GenTTL without shortening it. Scripts run with EVALSHA and fall back to EVAL
// when the server's script cache is empty (restart, failover, SCRIPT FLUSH).
// Set and Invalidate pipeline a GET of the epoch key with the script.
//
// Bus. New starts one goroutine that owns a SUBSCRIBE connection to the
// configured channel (on Cluster a regular PUBLISH reaches every node, so any
// node serves the subscription) and hands each event to Receive without
// queueing; Close stops it. Every "subscribe" confirmation yields
// EventConnected, preceded by EventDisconnected when it was not the first of
// a connection the backend already reported. When the link stays silent for
// one health-check interval (l2.Config.BusHealthCheck, default 1 s) the
// goroutine sends PING; no reply within a second interval marks the link dead,
// so a silently broken connection is reported within two intervals. A lost
// link is closed and re-established with jittered exponential backoff.
//
// Errors. Every failed command is returned wrapped with ErrUnavailable (and
// the go-redis cause); a reply that does not match the script's shape is
// l2.ErrMalformed. The cache core counts both as L2 failures for its circuit
// breaker. go-redis command retries (Options.MaxRetries) may run a command
// twice after a timeout: Set and Invalidate then increment the generation
// twice and Fill is rejected or idempotent, which the protocol tolerates; a
// retried SET NX may report the backend's own lock as held, and the caller
// then waits up to lock_timeout and loads by itself (the bounded fallback).
// go-redis also retries failed dials (Options.DialerRetries, default 5 with
// 100 ms pauses), which delays each failure while Redis is down.
//
// Cluster: the empty key "" is not supported; its names carry the empty hash
// tag "{}", which Redis ignores, so they map to different slots (CROSSSLOT).
//
//	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"localhost:6379"}})
//	users, err := cache.New[User]("users", cache.JSONCodec[User](), cache.WithL2(rdb))
package redisl2
