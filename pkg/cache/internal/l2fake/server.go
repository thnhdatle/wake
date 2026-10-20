// SPDX-License-Identifier: Apache-2.0

package l2fake

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

// Clock is the server time source (Redis TIME and key expiry).
type Clock interface {
	Now() time.Time
}

// Server models one Redis deployment shared by several clients.
type Server struct {
	mu         sync.Mutex // protects every field below and the Client flags
	clock      Clock
	lastMicros int64
	// scriptMicros is TIME as read once by the running script.
	scriptMicros int64
	data         map[string]record
	subs         map[*subscription]struct{}
	down         bool
	queueLimit   int
}

type record struct {
	val       string
	expiresAt time.Time // zero: no expiry
}

// DefaultQueueLimit is the per-subscriber buffer (messages) before the
// server drops the subscriber's connection.
const DefaultQueueLimit = 4096

// NewServer returns an empty server reading time from clock.
func NewServer(clock Clock) *Server {
	return &Server{
		clock:      clock,
		data:       make(map[string]record),
		subs:       make(map[*subscription]struct{}),
		queueLimit: DefaultQueueLimit,
	}
}

// SetQueueLimit changes the per-subscriber buffer for later publishes.
func (s *Server) SetQueueLimit(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queueLimit = n
}

// Client returns a new connection for one cache instance with its own bus
// subscription to cfg.Channel.
func (s *Server) Client(cfg l2.Config) *Client {
	c := &Client{srv: s, cfg: cfg, calls: make(map[Op]int)}
	c.sub = newSubscription(cfg.Channel)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[c.sub] = struct{}{}
	c.sub.owner = c
	s.refreshLocked(c.sub)
	return c
}

// SetDown makes the whole server unreachable (true) or reachable again.
func (s *Server) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
	for sub := range s.subs {
		s.refreshLocked(sub)
	}
}

// WaitIdle blocks until every subscriber has received all queued events and
// its receiver has asked for the next one (so the previous one is handled).
func (s *Server) WaitIdle(ctx context.Context) error {
	s.mu.Lock()
	subs := make([]*subscription, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		if err := sub.waitIdle(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Gen returns the stored generation of key in namespace ns and epoch.
func (s *Server) Gen(ns string, epoch uint64, key string) (uint64, bool) {
	_, g, _ := l2.KeyNames(ns, epoch, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.getLocked(g)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	return n, err == nil
}

// GenTTL returns the remaining TTL of the generation key.
func (s *Server) GenTTL(ns string, epoch uint64, key string) (time.Duration, bool) {
	_, g, _ := l2.KeyNames(ns, epoch, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.getLocked(g); !ok {
		return 0, false
	}
	return s.data[g].expiresAt.Sub(s.clock.Now()), true
}

// Value returns the stored value record of key, if any.
func (s *Server) Value(ns string, epoch uint64, key string) (l2.StoredValue, bool) {
	v, _, _ := l2.KeyNames(ns, epoch, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.getLocked(v)
	if !ok {
		return l2.StoredValue{}, false
	}
	sv, err := l2.DecodeValue([]byte(raw))
	return sv, err == nil
}

// EpochOf returns the stored epoch of namespace ns.
func (s *Server) EpochOf(ns string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.getLocked(l2.EpochKey(ns))
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	return n, err == nil
}

// Locked reports whether the load lock of key is held.
func (s *Server) Locked(ns string, epoch uint64, key string) bool {
	_, _, l := l2.KeyNames(ns, epoch, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.getLocked(l)
	return ok
}

// RawSet stores a raw string without expiry (tests use it to corrupt data).
func (s *Server) RawSet(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = record{val: val}
}

// Delete removes a key, as if it had expired or been evicted.
func (s *Server) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// timeMicrosLocked models Redis TIME in microseconds, strictly increasing.
func (s *Server) timeMicrosLocked() int64 {
	t := s.clock.Now().UnixMicro()
	if t <= s.lastMicros {
		t = s.lastMicros + 1
	}
	s.lastMicros = t
	return t
}

func (s *Server) getLocked(key string) (string, bool) {
	r, ok := s.data[key]
	if !ok {
		return "", false
	}
	if !r.expiresAt.IsZero() && !s.clock.Now().Before(r.expiresAt) {
		delete(s.data, key)
		return "", false
	}
	return r.val, true
}

// setLocked models SET key val [PX ttl]; ttl <= 0 means no expiry.
func (s *Server) setLocked(key, val string, ttl time.Duration) {
	r := record{val: val}
	if ttl > 0 {
		r.expiresAt = s.clock.Now().Add(ttl)
	}
	s.data[key] = r
}

// extendLocked raises the TTL of an existing key to at least ttl.
func (s *Server) extendLocked(key string, ttl time.Duration) {
	r, ok := s.data[key]
	if !ok || r.expiresAt.IsZero() {
		return
	}
	if at := s.clock.Now().Add(ttl); r.expiresAt.Before(at) {
		r.expiresAt = at
		s.data[key] = r
	}
}

// genLocked returns the generation of gkey, initializing it to TIME and
// extending its TTL to at least genTTL, as every script does.
func (s *Server) genLocked(gkey string, genTTL time.Duration) uint64 {
	if raw, ok := s.getLocked(gkey); ok {
		if n, err := strconv.ParseUint(raw, 10, 64); err == nil {
			s.extendLocked(gkey, genTTL)
			return n
		}
	}
	n := uint64(s.scriptMicros) //nolint:gosec // G115: TIME in microseconds is positive
	s.setLocked(gkey, strconv.FormatUint(n, 10), genTTL)
	return n
}

// incrGenLocked models INCR on the generation key (initialized first).
func (s *Server) incrGenLocked(gkey string, genTTL time.Duration) uint64 {
	n := s.genLocked(gkey, genTTL) + 1
	r := s.data[gkey]
	r.val = strconv.FormatUint(n, 10)
	s.data[gkey] = r
	return n
}

// epochLocked returns the namespace epoch, initializing it with SET NX TIME.
func (s *Server) epochLocked(ns string) uint64 {
	key := l2.EpochKey(ns)
	if raw, ok := s.getLocked(key); ok {
		if n, err := strconv.ParseUint(raw, 10, 64); err == nil {
			return n
		}
	}
	n := uint64(s.scriptMicros) //nolint:gosec // G115: TIME in microseconds is positive
	s.setLocked(key, strconv.FormatUint(n, 10), 0)
	return n
}

// publishLocked models PUBLISH channel msg.
func (s *Server) publishLocked(channel string, e l2.Event) {
	msg := l2.EncodeMessage(e)
	for sub := range s.subs {
		if sub.channel == channel {
			sub.deliver(msg, s.queueLimit)
		}
	}
}

// refreshLocked recomputes whether sub's connection is up.
func (s *Server) refreshLocked(sub *subscription) {
	c := sub.owner
	sub.setConnected(!s.down && !c.down && !c.busDown && !c.closed)
}
