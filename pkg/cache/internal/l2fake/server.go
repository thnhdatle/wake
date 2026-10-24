// SPDX-License-Identifier: Apache-2.0

package l2fake

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/thnhdatle/wake/pkg/cache/internal/l2"
)

type Clock interface {
	Now() time.Time
}

// Server models one Redis deployment shared by several clients.
type Server struct {
	mu           sync.Mutex
	clock        Clock
	lastMicros   int64
	scriptMicros int64
	data         map[string]record
	subs         map[*subscription]struct{}
	down         bool
	queueLimit   int
}

type record struct {
	val       string
	expiresAt time.Time
}

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

func (s *Server) SetQueueLimit(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queueLimit = n
}

// Client returns a new connection with its own bus subscription.
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

func (s *Server) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
	for sub := range s.subs {
		s.refreshLocked(sub)
	}
}

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

func (s *Server) GenTTL(ns string, epoch uint64, key string) (time.Duration, bool) {
	_, g, _ := l2.KeyNames(ns, epoch, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.getLocked(g); !ok {
		return 0, false
	}
	return s.data[g].expiresAt.Sub(s.clock.Now()), true
}

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

func (s *Server) Locked(ns string, epoch uint64, key string) bool {
	_, _, l := l2.KeyNames(ns, epoch, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.getLocked(l)
	return ok
}

func (s *Server) RawSet(key, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = record{val: val}
}

func (s *Server) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

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

func (s *Server) setLocked(key, val string, ttl time.Duration) {
	r := record{val: val}
	if ttl > 0 {
		r.expiresAt = s.clock.Now().Add(ttl)
	}
	s.data[key] = r
}

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

func (s *Server) incrGenLocked(gkey string, genTTL time.Duration) uint64 {
	n := s.genLocked(gkey, genTTL) + 1
	r := s.data[gkey]
	r.val = strconv.FormatUint(n, 10)
	s.data[gkey] = r
	return n
}

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

func (s *Server) publishLocked(channel string, e l2.Event) {
	msg := l2.EncodeMessage(e)
	for sub := range s.subs {
		if sub.channel == channel {
			sub.deliver(msg, s.queueLimit)
		}
	}
}

func (s *Server) refreshLocked(sub *subscription) {
	c := sub.owner
	sub.setConnected(!s.down && !c.down && !c.busDown && !c.closed)
}
