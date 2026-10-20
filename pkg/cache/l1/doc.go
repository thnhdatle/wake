// SPDX-License-Identifier: Apache-2.0

// Package l1 is the in-process tier of pkg/cache: a sharded LRU bounded by
// entry count and accounted bytes, with per-entry TTL (optionally jittered by
// a seeded source) and a per-shard invalidation counter.
//
// Main types: Cache (the sharded LRU), Entry (what Set stores), Token (a
// captured shard invalidation counter) and Clock.
//
// The invalidation counter closes the "populate after invalidation" race of
// a read-through cache: a reader captures a Token before it reads the lower
// tier and inserts with SetIfUnchanged, which refuses the insert when any
// invalidation (Set, Invalidate, Delete, Flush) touched the shard since.
//
// Values are stored and returned by value; when V holds pointers, slices or
// maps the referenced data is shared between callers, so treat V as
// immutable. A hit on a string key does not allocate.
//
//	c, _ := l1.New[string](10_000, 64<<20)
//	tok := c.Token("user:1")
//	v := loadFromBelow()
//	c.SetIfUnchanged(tok, "user:1", l1.Entry[string]{Value: v, Size: len(v), TTL: time.Minute})
//	got, ok := c.Get("user:1")
package l1
