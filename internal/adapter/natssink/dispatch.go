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

// Run implements pipeline.Sink: it connects (retrying until ctx ends),
// creates or checks the stream, publishes accepted events in Send order and
// acknowledges each on its PubAck. It returns nil when ctx ends, after
// waiting at most the shutdown timeout for outstanding PubAcks.
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

// connect dials until it gets a connection or ctx ends. nats.go retries a
// failed first connect itself, so this loop only repeats on errors such as
// an unreadable credentials file.
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

// retryState counts consecutive failed attempts for the backoff.
type retryState struct {
	block   int // prepare failures of the head event
	recover int // failed re-publication rounds
}

// dispatch is the single publisher loop. It re-publishes failed events
// before anything newer, checks each event (stream, size) before publishing
// it and publishes in the order Send accepted them. Once the connection was
// re-established it waits until every message published on the previous
// connection is resolved before publishing more, so a message lost with the
// old connection is re-published before anything newer is stored.
func (s *Sink) dispatch(ctx context.Context, c conn) {
	var st dispatchState
	for ctx.Err() == nil && s.step(ctx, c, &st) {
	}
}

// dispatchState is the dispatcher's state between steps.
type dispatchState struct {
	next     *unit // taken from the queue, not yet published
	seq      uint64
	attempts retryState
}

// step makes one move of the dispatcher; it reports false once ctx ended.
func (s *Sink) step(ctx context.Context, c conn, st *dispatchState) bool {
	if s.hasFailures() {
		return s.recoverFailed(ctx, c, &st.attempts)
	}
	if st.next == nil {
		if st.next = s.take(ctx); st.next != nil {
			st.seq++
			st.next.seq = st.seq
		}
		// A failure may have arrived while take waited: look again
		// before publishing anything newer.
		return true
	}
	if err := s.prepare(ctx, c, st.next); err != nil {
		return ctx.Err() == nil && s.blocked(ctx, err, &st.attempts.block)
	}
	s.unblock()
	st.attempts.block = 0
	if s.hasFailures() {
		return true // re-publish those first
	}
	if errors.Is(s.publish(c, st.next, false), errEpochChanged) {
		return s.waitIdle(ctx)
	}
	st.next = nil
	return true
}

// take returns the next accepted event, or nil when ctx ended or a publish
// failed meanwhile.
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

// prepare makes sure the stream exists and checks u against
// max_event_bytes and the server's max_payload.
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

// readyStream creates or updates the stream (nats.create_stream) or checks
// that it exists, once per run and again after a publish found no stream.
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

// blocked records a prepare failure and waits the backoff. It reports
// whether ctx is still live.
func (s *Sink) blocked(ctx context.Context, err error, attempt *int) bool {
	s.noteBlocked(err)
	d := s.delay(*attempt)
	*attempt++
	return s.sleep(ctx, d)
}

// noteBlocked records a prepare failure in Health and the metrics.
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

// publish hands u to JetStream. It returns errEpochChanged, without
// publishing, when the connection changed while earlier messages are still
// unresolved; any other error has queued u for re-publication.
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
	s.pending <- pendingAck{u: u, fut: fut} // never blocks: one entry per event in flight
	return nil
}

// collect resolves PubAck futures in publish order.
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

// onResult settles one published event.
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

// failLocked queues a failed event for re-publication and records the
// error in Health; keep is false while shutting down. A disconnection
// caused by a rejected login is reported as that rejection.
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

// complete acknowledges an event and releases its buffer space.
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

// recoverFailed re-publishes failed events once every message in flight is
// resolved, in dispatch order and after the backoff, then waits for them
// before anything newer is published.
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

// waitIdle waits until no published message is unresolved.
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

// watch reports a connection that is down (fatal at once when the server
// rejected the credentials, retrying after the stall timeout otherwise) and
// published messages without PubAck for the stall timeout.
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

// tooLargeReason describes an event above the limit with its table, key
// and lsn; it never contains row values.
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

// maxReasonKey caps the key quoted in an event_too_large reason.
const maxReasonKey = 128
