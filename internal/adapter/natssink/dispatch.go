// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Run publishes queued events in order until ctx is canceled.
func (s *Sink) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	c := s.connect(ctx)
	if c == nil {
		return nil
	}
	bg, stop := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Go(func() { s.collect(bg) })
	wg.Go(func() { s.watch(bg, c) })
	s.dispatch(ctx, c)
	dctx, cancel := context.WithTimeout(bg, s.shutdownTimeout)
	s.waitIdle(dctx)
	cancel()
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	stop()
	wg.Wait()
	c.close()
	return nil
}

func (s *Sink) connect(ctx context.Context) conn {
	for attempt := 0; ; attempt++ {
		c, err := s.dial(s)
		if err == nil {
			s.unblock()
			return c
		}
		s.noteBlocked(err)
		if !s.sleep(ctx, s.delay(attempt)) {
			return nil
		}
	}
}

type retryState struct {
	block   int
	recover int
}

func (s *Sink) dispatch(ctx context.Context, c conn) {
	var st dispatchState
	for ctx.Err() == nil && s.step(ctx, c, &st) {
	}
}

type dispatchState struct {
	next     *unit
	seq      uint64
	attempts retryState
}

func (s *Sink) step(ctx context.Context, c conn, st *dispatchState) bool {
	if s.hasFailures() {
		return s.recoverFailed(ctx, c, &st.attempts)
	}
	if st.next == nil {
		if st.next = s.take(ctx); st.next != nil {
			st.seq++
			st.next.seq = st.seq
		}
		return true
	}
	if err := s.prepare(ctx, c, st.next); err != nil {
		return ctx.Err() == nil && s.blocked(ctx, err, &st.attempts.block)
	}
	s.unblock()
	st.attempts.block = 0
	if s.hasFailures() {
		return true
	}
	if errors.Is(s.publish(c, st.next, false), errEpochChanged) {
		return s.waitIdle(ctx)
	}
	st.next = nil
	return true
}

func (s *Sink) take(ctx context.Context) *unit {
	select {
	case <-ctx.Done():
		return nil
	case u := <-s.queue:
		return u
	case <-s.notify:
		return nil
	}
}

func (s *Sink) hasFailures() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.failed) > 0
}

func (s *Sink) prepare(ctx context.Context, c conn, u *unit) error {
	if err := s.readyStream(ctx, c); err != nil {
		return err
	}
	payloadLimit := c.maxPayload()
	if payloadLimit <= 0 {
		return fmt.Errorf("%w: waiting for the first connection to %s", ErrNotConnected, s.url)
	}
	limit := s.maxEventBytes
	if limit <= 0 {
		limit = payloadLimit
	}
	if u.size > limit {
		return fmt.Errorf("%w: %s", ErrEventTooLarge, tooLargeReason(u, limit))
	}
	if u.size > payloadLimit {
		return fmt.Errorf("%w: message of %d bytes for subject %s, server max_payload %d (id %s)",
			ErrMessageTooLarge, u.size, u.subject, payloadLimit, u.id)
	}
	if u.msg == nil {
		u.msg = &nats.Msg{Subject: u.subject, Data: u.rec.Payload, Header: nats.Header{}}
	}
	return nil
}

func (s *Sink) readyStream(ctx context.Context, c conn) error {
	s.mu.Lock()
	ready := s.streamReady
	s.mu.Unlock()
	if ready {
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	var err error
	if s.createStream {
		err = c.ensureStream(rctx, streamSpec{name: s.stream, subject: s.prefix + ".>", window: s.window})
	} else {
		err = c.streamExists(rctx, s.stream)
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.streamReady = true
	s.mu.Unlock()
	return nil
}

func (s *Sink) blocked(ctx context.Context, err error, attempt *int) bool {
	s.noteBlocked(err)
	d := s.delay(*attempt)
	*attempt++
	return s.sleep(ctx, d)
}

func (s *Sink) noteBlocked(err error) {
	v := classify(err)
	s.mu.Lock()
	tr := s.health.set(faultBlock, &fault{v: v, reason: err.Error()}, s.clock.Now())
	s.mu.Unlock()
	s.logTransition(tr)
	s.obs.ObserveRetry(s.name, v.reason)
}

func (s *Sink) unblock() {
	s.mu.Lock()
	tr := s.health.clear(s.clock.Now(), faultBlock)
	s.mu.Unlock()
	s.logTransition(tr)
}

func (s *Sink) publish(c conn, u *unit, retry bool) error {
	ep := c.epoch()
	now := s.clock.Now()
	s.mu.Lock()
	if s.inflight > 0 && ep != s.pubEpoch {
		s.mu.Unlock()
		return errEpochChanged
	}
	if s.inflight == 0 {
		s.progress = now
	}
	s.pubEpoch = ep
	s.inflight++
	if retry {
		s.recovering++
	}
	u.retry = retry
	u.sentAt = now
	s.mu.Unlock()
	s.obs.ObserveSent(s.name, 1, int(u.size))
	fut, err := c.publish(u.msg, u.id)
	if err != nil {
		s.onResult(u, err)
		return err
	}
	s.pending <- pendingAck{u: u, fut: fut}
	return nil
}

func (s *Sink) collect(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-s.pending:
			select {
			case <-p.fut.Ok():
				s.onResult(p.u, nil)
			case err := <-p.fut.Err():
				s.onResult(p.u, err)
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Sink) onResult(u *unit, err error) {
	now := s.clock.Now()
	s.mu.Lock()
	s.inflight--
	if u.retry {
		u.retry = false
		s.recovering--
	}
	idle := s.inflight == 0
	if err != nil {
		tr, v, keep := s.failLocked(u, err, now)
		s.mu.Unlock()
		if keep {
			s.obs.ObserveRetry(s.name, v.reason)
			s.logTransition(tr)
		}
		s.signal()
		return
	}
	s.progress = now
	s.accepted--
	accepted := s.accepted
	latency := now.Sub(u.sentAt)
	var tr transition
	if len(s.failed) == 0 && s.recovering == 0 {
		tr = s.health.clear(now, faultConn, faultWrite)
	} else {
		tr = s.health.clear(now, faultConn)
	}
	s.mu.Unlock()
	if idle {
		s.signal()
	}
	s.logTransition(tr)
	s.complete(u, latency, accepted)
}

func (s *Sink) failLocked(u *unit, err error, now time.Time) (transition, verdict, bool) {
	if s.closing {
		return transition{}, verdict{}, false
	}
	s.failed = append(s.failed, u)
	v := classify(err)
	reason := fmt.Sprintf("publish to %s: %v", u.subject, err)
	if !v.fatal && s.lastConnErr != nil {
		if cv := classify(s.lastConnErr); cv.fatal {
			v = cv
			reason += ": " + s.lastConnErr.Error()
		}
	}
	if v.reason == RetryReasonServer || v.reason == RetryReasonUnknownStream {
		s.streamReady = false
	}
	return s.health.set(faultWrite, &fault{v: v, reason: reason}, now), v, true
}

func (s *Sink) complete(u *unit, latency time.Duration, accepted int) {
	s.obs.ObserveAcked(s.name, 1)
	s.obs.ObserveLatency(s.name, latency)
	s.obs.SetInflight(s.name, accepted)
	s.ackObs.ObserveAck(s.name, u.rec.TsMs)
	if err := u.rec.Ack.Ack(); err != nil {
		s.log.Error("acknowledge event", slog.String("error", err.Error()), slog.String("lsn", u.rec.ID.LSN.String()))
	}
	s.bytes.Release(u.reserved)
	<-s.slots
}

func (s *Sink) recoverFailed(ctx context.Context, c conn, attempts *retryState) bool {
	if !s.waitIdle(ctx) {
		return false
	}
	s.mu.Lock()
	us := s.failed
	s.failed = nil
	s.mu.Unlock()
	slices.SortFunc(us, func(a, b *unit) int { return cmp.Compare(a.seq, b.seq) })
	d := s.delay(attempts.recover)
	attempts.recover++
	if !s.sleep(ctx, d) {
		s.requeue(us)
		return false
	}
	for i, u := range us {
		if err := s.prepare(ctx, c, u); err != nil {
			s.requeue(us[i:])
			if ctx.Err() != nil {
				return false
			}
			s.noteBlocked(err)
			return true
		}
		switch err := s.publish(c, u, true); {
		case errors.Is(err, errEpochChanged):
			s.requeue(us[i:])
			return true
		case err != nil:
			s.requeue(us[i+1:])
			return true
		}
	}
	s.unblock()
	if !s.waitIdle(ctx) {
		return false
	}
	if !s.hasFailures() {
		attempts.recover = 0
	}
	return true
}

func (s *Sink) requeue(us []*unit) {
	if len(us) == 0 {
		return
	}
	s.mu.Lock()
	s.failed = append(s.failed, us...)
	s.mu.Unlock()
}

func (s *Sink) waitIdle(ctx context.Context) bool {
	for {
		s.mu.Lock()
		n := s.inflight
		s.mu.Unlock()
		if n == 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-s.notify:
		}
	}
}

func (s *Sink) watch(ctx context.Context, c conn) {
	t := s.clock.NewTicker(max(s.stallTimeout/4, time.Millisecond))
	defer t.Stop()
	var downSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			downSince = s.checkConn(c, downSince)
		}
	}
}

func (s *Sink) checkConn(c conn, downSince time.Time) time.Time {
	connected, lastErr := c.state()
	now := s.clock.Now()
	if connected {
		downSince = time.Time{}
	} else if downSince.IsZero() {
		downSince = now
	}
	s.mu.Lock()
	if lastErr == nil {
		lastErr = s.lastConnErr
	}
	var f *fault
	switch {
	case !connected:
		v := verdict{reason: RetryReasonNetwork}
		reason := "not connected to " + s.url
		if lastErr != nil {
			v = classify(lastErr)
			reason += ": " + lastErr.Error()
		}
		if v.fatal || now.Sub(downSince) >= s.stallTimeout {
			f = &fault{v: v, reason: reason}
		}
	case s.inflight > 0 && now.Sub(s.progress) >= s.stallTimeout:
		f = &fault{v: verdict{reason: RetryReasonNetwork}, reason: fmt.Sprintf(
			"no PubAck for %s with %d messages in flight", now.Sub(s.progress).Round(time.Second), s.inflight)}
	}
	var tr transition
	if f != nil {
		tr = s.health.set(faultConn, f, now)
	} else {
		tr = s.health.clear(now, faultConn)
	}
	s.mu.Unlock()
	s.logTransition(tr)
	return downSince
}

func tooLargeReason(u *unit, limit int64) string {
	table := u.rec.Table()
	if table == "" {
		table = u.rec.Destination
	}
	key := u.rec.Key
	if len(key) > maxReasonKey {
		key = key[:maxReasonKey]
	}
	return fmt.Sprintf("event of %d bytes exceeds max_event_bytes %d: table %s, key %s, lsn %s, id %s",
		u.size, limit, table, key, u.rec.ID.LSN, u.id)
}

const maxReasonKey = 128
