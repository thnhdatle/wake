// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Run implements pipeline.Sink: it creates the franz-go client, hands
// accepted events to it in Send order and acknowledges them when Kafka has
// them. It returns nil when ctx ends, after flushing for at most the
// shutdown timeout; it returns an error only when the client cannot be
// built.
func (s *Sink) Run(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	cl, err := s.newClient(s.kgoOptions())
	if err != nil {
		return err
	}
	wctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { s.watch(wctx) })
	s.dispatch(ctx, cl)
	stop()
	wg.Wait()
	s.shutdown(ctx, cl)
	return nil
}

// dispatch is the single producer loop. It re-produces failed writes before
// anything newer, checks each event (size, topic) before producing it, and
// produces events in the order Send accepted them, so records of one key
// reach their partition in order.
func (s *Sink) dispatch(ctx context.Context, cl client) {
	var (
		next     *unit
		seq      uint64
		attempts retryState
	)
	for ctx.Err() == nil {
		if s.hasFailures() {
			if !s.recoverFailed(ctx, cl, &attempts) {
				return
			}
			continue
		}
		if next == nil {
			// A failure may arrive while take waits: the loop looks at
			// failures again before producing anything newer.
			if next = s.take(ctx); next != nil {
				seq++
				next.seq = seq
			}
			continue
		}
		if err := s.prepare(ctx, cl, next); err != nil {
			if ctx.Err() != nil || !s.blocked(ctx, err, &attempts.block) {
				return
			}
			continue
		}
		s.unblock()
		attempts.block = 0
		if s.hasFailures() {
			continue // re-produce those first
		}
		s.produce(ctx, cl, next.writes, false)
		next = nil
	}
}

// retryState counts consecutive failed attempts for the backoff.
type retryState struct {
	block   int // prepare failures of the head event
	recover int // failed re-production rounds
}

// take returns the next accepted event, or nil when ctx ended or a write
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

// prepare checks u against max_event_bytes and its topic (existence,
// auto-creation, max.message.bytes) and builds its records.
func (s *Sink) prepare(ctx context.Context, cl client, u *unit) error {
	if u.size > s.maxEventBytes {
		return fmt.Errorf("%w: %s", ErrEventTooLarge, tooLargeReason(&u.rec, u.size, s.maxEventBytes))
	}
	info, err := s.topic(ctx, cl, u.topic, u.writes == nil && isTruncate(&u.rec))
	if err != nil {
		return err
	}
	if info.maxMessageBytes > 0 && u.size+batchOverhead > info.maxMessageBytes {
		s.forgetTopic(u.topic)
		return fmt.Errorf("%w: record of about %d bytes for topic %s with max.message.bytes %d (id %s)",
			ErrMessageTooLarge, u.size+batchOverhead, u.topic, info.maxMessageBytes, u.rec.ID)
	}
	if u.writes == nil {
		buildWrites(u, info.partitions, s.tombstones)
	}
	return nil
}

// topic returns the cached facts about a topic, reading them on first use
// or when fresh is set (a `t` event needs the current partition count). A
// missing topic is created when kafka.auto_create_topics is on.
func (s *Sink) topic(ctx context.Context, cl client, topic string, fresh bool) (topicInfo, error) {
	s.mu.Lock()
	info, ok := s.topics[topic]
	s.mu.Unlock()
	if ok && !fresh {
		return info, nil
	}
	rctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	info, err := cl.describeTopic(rctx, topic)
	if err != nil {
		return topicInfo{}, err
	}
	if !info.exists {
		if info, err = s.createTopic(rctx, cl, topic); err != nil {
			return topicInfo{}, err
		}
	}
	if info.partitions < 1 {
		// A topic seen while its creation propagates: a `t` fan-out
		// would miss partitions, so wait for the full metadata.
		return topicInfo{}, fmt.Errorf("%w: topic %s has no partitions in metadata yet", errTopicNotReady, topic)
	}
	s.mu.Lock()
	s.topics[topic] = info
	s.mu.Unlock()
	return info, nil
}

// createTopic creates a missing topic when kafka.auto_create_topics is on
// and reads it back.
func (s *Sink) createTopic(ctx context.Context, cl client, topic string) (topicInfo, error) {
	if !s.autoCreate {
		return topicInfo{}, fmt.Errorf("%w: topic %s does not exist and kafka.auto_create_topics is false",
			ErrUnknownTopic, topic)
	}
	if err := cl.createTopic(ctx, topic, s.partitions, s.replication); err != nil {
		return topicInfo{}, err
	}
	s.log.Info("topic created", slog.String("operation", "create_topic"), slog.String("table", topic))
	info, err := cl.describeTopic(ctx, topic)
	if err != nil {
		return topicInfo{}, err
	}
	if !info.exists {
		return topicInfo{}, fmt.Errorf("%w: topic %s created but not in metadata yet", ErrUnknownTopic, topic)
	}
	return info, nil
}

func (s *Sink) forgetTopic(topic string) {
	s.mu.Lock()
	delete(s.topics, topic)
	s.mu.Unlock()
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
	v := classify(err, s.autoCreate)
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

// produce hands writes to franz-go. Never called with s.mu held: a promise
// may run before Produce returns.
func (s *Sink) produce(ctx context.Context, cl client, ws []*write, retry bool) {
	if len(ws) == 0 {
		return
	}
	now := s.clock.Now()
	s.mu.Lock()
	if s.inflight == 0 {
		s.progress = now
	}
	s.inflight += len(ws)
	var sent []*unit
	for _, w := range ws {
		w.retry = retry
		if retry {
			s.recovering++
		}
		if len(sent) == 0 || sent[len(sent)-1] != w.u {
			w.u.sentAt = now
			sent = append(sent, w.u)
		}
	}
	s.mu.Unlock()
	for _, u := range sent {
		s.obs.ObserveSent(s.name, 1, int(u.size))
	}
	// The records outlive Run's context on purpose: shutdown flushes them.
	pctx := context.WithoutCancel(ctx)
	for _, w := range ws {
		cl.Produce(pctx, w.rec, func(_ *kgo.Record, err error) { s.onResult(w, err) })
	}
}

// onResult is franz-go's promise for one write.
func (s *Sink) onResult(w *write, err error) {
	now := s.clock.Now()
	s.mu.Lock()
	s.inflight--
	if w.retry {
		w.retry = false
		s.recovering--
	}
	idle := s.inflight == 0
	if err != nil {
		tr, v, keep := s.failLocked(w, err, now)
		s.mu.Unlock()
		if keep {
			s.obs.ObserveRetry(s.name, v.reason)
			s.logTransition(tr)
		}
		s.signal()
		return
	}
	s.progress = now
	w.u.pending--
	done := w.u.pending == 0
	var latency time.Duration
	if done {
		s.accepted--
		latency = now.Sub(w.u.sentAt)
	}
	accepted := s.accepted
	var tr transition
	if len(s.failed) == 0 && s.recovering == 0 {
		tr = s.health.clear(now, faultStall, faultWrite)
	} else {
		tr = s.health.clear(now, faultStall)
	}
	s.mu.Unlock()
	if idle {
		s.signal()
	}
	s.logTransition(tr)
	if done {
		s.complete(w.u, latency, accepted)
	}
}

// failLocked queues a failed write for re-production and records the error
// in Health; keep is false while shutting down (the write is dropped,
// unacknowledged).
func (s *Sink) failLocked(w *write, err error, now time.Time) (transition, verdict, bool) {
	if s.closing {
		return transition{}, verdict{}, false
	}
	s.failed = append(s.failed, w)
	v := classify(err, s.autoCreate)
	if v.reason == RetryReasonUnknownTopic || v.reason == RetryReasonMessageTooLarge {
		delete(s.topics, w.u.topic)
	}
	reason := fmt.Sprintf("produce to topic %s: %v", w.u.topic, err)
	return s.health.set(faultWrite, &fault{v: v, reason: reason}, now), v, true
}

// complete acknowledges an event whose writes Kafka all holds and releases
// its buffer space.
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

// recoverFailed re-produces failed writes once every write in flight has
// completed, in dispatch order and after the backoff, then waits for them
// before anything newer is produced: per partition franz-go fails a whole
// suffix of buffered records, so re-producing in order restores the order.
func (s *Sink) recoverFailed(ctx context.Context, cl client, attempts *retryState) bool {
	if !s.waitIdle(ctx) {
		return false
	}
	s.mu.Lock()
	ws := s.failed
	s.failed = nil
	s.mu.Unlock()
	sortWrites(ws)
	d := s.delay(attempts.recover)
	attempts.recover++
	if !s.sleep(ctx, d) {
		return false
	}
	for i, w := range ws {
		if i > 0 && ws[i-1].u == w.u {
			continue
		}
		if err := s.prepare(ctx, cl, w.u); err != nil {
			s.requeue(ws)
			if ctx.Err() != nil {
				return false
			}
			s.noteBlocked(err)
			return true
		}
	}
	s.unblock()
	for _, w := range ws {
		w.rec = retryRecord(w.rec)
	}
	s.produce(ctx, cl, ws, true)
	if !s.waitIdle(ctx) {
		return false
	}
	if !s.hasFailures() {
		attempts.recover = 0
	}
	return true
}

func (s *Sink) requeue(ws []*write) {
	s.mu.Lock()
	s.failed = append(ws, s.failed...)
	s.mu.Unlock()
}

// waitIdle waits until no write is in flight.
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

// watch reports a stall: records in flight and no acknowledgement for the
// stall timeout, as when every broker is down and franz-go retries
// internally.
func (s *Sink) watch(ctx context.Context) {
	t := s.clock.NewTicker(max(s.stallTimeout/4, time.Millisecond))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			s.checkStall()
		}
	}
}

func (s *Sink) checkStall() {
	now := s.clock.Now()
	s.mu.Lock()
	if s.inflight == 0 || now.Sub(s.progress) < s.stallTimeout {
		s.mu.Unlock()
		return
	}
	reason := fmt.Sprintf("no acknowledgement from Kafka for %s with %d records in flight",
		now.Sub(s.progress).Round(time.Second), s.inflight)
	v := verdict{reason: RetryReasonNetwork}
	if s.lastConnErr != nil {
		reason += ": " + s.lastConnErr.Error()
		v = classify(s.lastConnErr, s.autoCreate)
	}
	tr := s.health.set(faultStall, &fault{v: v, reason: reason}, now)
	s.mu.Unlock()
	s.logTransition(tr)
}

// shutdown flushes what franz-go holds for at most the shutdown timeout,
// then closes the client; writes still pending stay unacknowledged.
func (s *Sink) shutdown(ctx context.Context, cl client) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultShutdownTimeout)
	defer cancel()
	if err := cl.Flush(fctx); err != nil {
		s.log.Warn("flush on shutdown", slog.String("error", err.Error()))
	}
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	cl.Close()
}
