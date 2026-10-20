// SPDX-License-Identifier: Apache-2.0

package l1

import (
	"fmt"
	"hash/maphash"
	"math/bits"
	"math/rand/v2"
	"time"
)

const (
	defaultShards      = 64
	minEntriesPerShard = 8
)

// Entry is what Set and SetIfUnchanged store.
type Entry[V any] struct {
	// Value is stored and returned by value (shared, treat as immutable).
	Value V
	// Size is the accounted size in bytes (the caller's encoded size).
	Size int
	// TTL is the lifetime; zero or negative never expires.
	TTL time.Duration
	// Version orders writes of the same key: an entry is never replaced by
	// one with a lower version, and Invalidate keeps an entry whose version
	// is at least the invalidated one.
	Version uint64
}

// Token is a captured shard invalidation counter (see Cache.Token).
type Token struct {
	shard uint32
	n     uint64
}

// Cache is a sharded LRU bounded by entries and bytes. It is safe for
// concurrent use.
type Cache[V any] struct {
	shards []shard[V]
	mask   uint64
	seed   maphash.Seed
	clock  Clock
	jitter time.Duration
}

// New returns a cache holding at most maxEntries entries and maxBytes
// accounted bytes; each shard holds an equal share of both bounds.
func New[V any](maxEntries int, maxBytes int64, opts ...Option) (*Cache[V], error) {
	if maxEntries <= 0 || maxBytes <= 0 {
		return nil, fmt.Errorf("bounds %d entries, %d bytes: %w", maxEntries, maxBytes, ErrInvalidConfig)
	}
	cfg := config{clock: systemClock{}}
	for _, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, err
		}
	}
	n := cfg.shards
	if n == 0 {
		n = defaultShards
		for n > 1 && maxEntries/n < minEntriesPerShard {
			n /= 2
		}
	}
	n = 1 << bits.Len(uint(n-1)) // round up to a power of two
	c := &Cache[V]{
		shards: make([]shard[V], n),
		mask:   uint64(n - 1), //nolint:gosec // G115: n is a positive power of two
		seed:   maphash.MakeSeed(),
		clock:  cfg.clock,
		jitter: cfg.jitter,
	}
	perEntries := (maxEntries + n - 1) / n
	perBytes := (maxBytes + int64(n) - 1) / int64(n)
	for i := range c.shards {
		rng := rand.New(rand.NewPCG(cfg.jitterSeed, uint64(i))) //nolint:gosec // G404: non-crypto TTL jitter
		c.shards[i].init(perEntries, perBytes, rng)
	}
	return c, nil
}

func (c *Cache[V]) index(key string) uint32 {
	return uint32(maphash.String(c.seed, key) & c.mask) //nolint:gosec // G115: masked to the shard count
}

// Seq returns the captured counter. Two tokens of the same key are equal
// exactly when no invalidation touched its shard between their captures, so
// callers can use it to tell work started before an invalidation from work
// started after it.
func (t Token) Seq() uint64 { return t.n }

// Token captures the invalidation counter of key's shard. Capture it before
// reading the lower tier and pass it to SetIfUnchanged.
func (c *Cache[V]) Token(key string) Token {
	i := c.index(key)
	return Token{shard: i, n: c.shards[i].inval.Load()}
}

// Get returns the value of key if present and not expired.
func (c *Cache[V]) Get(key string) (V, bool) {
	return c.get(key, 0)
}

// GetMaxAge is Get that also treats entries stored more than maxAge ago as
// expired (a positive maxAge caps the TTL of existing entries).
func (c *Cache[V]) GetMaxAge(key string, maxAge time.Duration) (V, bool) {
	return c.get(key, maxAge)
}

func (c *Cache[V]) get(key string, maxAge time.Duration) (V, bool) {
	s := &c.shards[c.index(key)]
	now := c.clock.Now().UnixNano()
	s.mu.Lock()
	n := s.lookupLocked(key)
	if n == nil || (n.expiresAt != 0 && now >= n.expiresAt) || (maxAge > 0 && now-n.storedAt >= int64(maxAge)) {
		s.mu.Unlock()
		var zero V
		return zero, false
	}
	v := n.value
	s.mu.Unlock()
	return v, true
}

// GetStale returns the value of key if present and expired for less than
// maxStale (or not expired at all). Invalidated entries are gone and never
// returned.
func (c *Cache[V]) GetStale(key string, maxStale time.Duration) (V, bool) {
	s := &c.shards[c.index(key)]
	now := c.clock.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.lookupLocked(key)
	if n == nil || (n.expiresAt != 0 && now >= n.expiresAt+int64(maxStale)) {
		var zero V
		return zero, false
	}
	return n.value, true
}

// Set counts an invalidation of key's shard and stores e, unless an entry
// with a higher version exists or e does not fit in the shard. It reports
// whether e was stored.
func (c *Cache[V]) Set(key string, e Entry[V]) bool {
	s := &c.shards[c.index(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidateLocked()
	return s.insertLocked(c.newNodeLocked(s, key, e))
}

// SetIfUnchanged stores e only if no invalidation touched key's shard since
// tok was captured (and under the same rules as Set, without counting an
// invalidation). It reports whether e was stored.
func (c *Cache[V]) SetIfUnchanged(tok Token, key string, e Entry[V]) bool {
	i := c.index(key)
	s := &c.shards[i]
	s.mu.Lock()
	defer s.mu.Unlock()
	if tok.shard != i || s.inval.Load() != tok.n {
		return false
	}
	return s.insertLocked(c.newNodeLocked(s, key, e))
}

func (c *Cache[V]) newNodeLocked(s *shard[V], key string, e Entry[V]) *node[V] {
	now := c.clock.Now().UnixNano()
	n := &node[V]{key: key, value: e.Value, size: int64(e.Size), version: e.Version, storedAt: now}
	if e.Size < 0 {
		n.size = 0
	}
	if ttl := e.TTL; ttl > 0 {
		if j := min(c.jitter, ttl/2); j > 0 {
			ttl -= time.Duration(s.rng.Int64N(int64(j)))
		}
		n.expiresAt = now + int64(ttl)
	}
	return n
}

// Invalidate counts an invalidation of key's shard and removes key unless
// its entry has a version of at least version.
func (c *Cache[V]) Invalidate(key string, version uint64) {
	s := &c.shards[c.index(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidateLocked()
	if n := s.items[key]; n != nil && n.version < version {
		s.removeLocked(n)
	}
}

// Delete counts an invalidation of key's shard and removes key.
func (c *Cache[V]) Delete(key string) {
	s := &c.shards[c.index(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidateLocked()
	if n := s.items[key]; n != nil {
		s.removeLocked(n)
	}
}

// Flush counts an invalidation of every shard and removes every entry.
func (c *Cache[V]) Flush() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.invalidateLocked()
		s.clearLocked()
		s.mu.Unlock()
	}
}

// Len returns the number of stored entries, expired ones included.
func (c *Cache[V]) Len() int {
	total := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		total += len(s.items)
		s.mu.Unlock()
	}
	return total
}

// Bytes returns the accounted bytes of stored entries.
func (c *Cache[V]) Bytes() int64 {
	var total int64
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		total += s.bytes
		s.mu.Unlock()
	}
	return total
}
