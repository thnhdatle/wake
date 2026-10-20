// SPDX-License-Identifier: Apache-2.0

// Package cache is Wake's two-tier read-through cache library: an
// in-process L1 (pkg/cache/l1) in front of a shared Redis L2, kept
// consistent by per-key generations in L2, a namespace epoch in every L2 key
// name and an invalidation guard in L1. Invalidations are
// driven by Delete, typically called by pkg/cache/cdcsource for every
// change captured from Postgres.
//
// Main types: Cache (Get, GetOrLoad, Set, Delete, Close), LoadFunc, Codec
// (JSONCodec, MsgpackCodec, ProtobufCodec), Option, SetOption and Clock.
//
// Consistency rules in short:
//   - an L2 value is valid only while its stored generation equals the key's
//     current generation; Delete and Set increment the generation;
//   - a loader's result is written to L2 only if the generation it observed
//     before loading is still current, so a load racing a Delete is
//     discarded;
//   - L1 is populated only if no invalidation touched the key's L1 shard
//     between the L2 read and the insert;
//   - while L2 or the invalidation bus is unreachable L1 ages are capped to
//     WithDegradedL1TTL and Delete returns ErrDeferred.
//
// Loaders must read from the primary database: a lagging replica can
// repopulate a stale value under a fresh generation, kept until its TTL.
// Values are shared between callers: treat V as immutable.
//
//	users, err := cache.New[User]("users", cache.JSONCodec[User](),
//		cache.WithTTL(10*time.Minute, time.Minute))
//	if err != nil { ... }
//	defer users.Close()
//	u, err := users.GetOrLoad(ctx, "user:42", func(ctx context.Context) (User, error) {
//		return db.LoadUser(ctx, 42)
//	})
package cache
