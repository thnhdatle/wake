// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Run delivers queued events until ctx is canceled.
func (s *Sink) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	var wg sync.WaitGroup
	defer s.client.CloseIdleConnections()
	defer wg.Wait()
	var lingerC <-chan time.Time
	for {
		batches, wait := s.ready()
		for _, b := range batches {
			wg.Go(func() { s.deliver(ctx, b) })
		}
		if wait > 0 && lingerC == nil {
			lingerC = s.clock.After(wait)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.notify:
		case <-lingerC:
			lingerC = nil
		}
	}
}

func (s *Sink) ready() ([]pipeline.LaneBatch, time.Duration) {
	s.mu.Lock()
	if s.sched.Len() == 0 {
		s.mu.Unlock()
		return nil, 0
	}
	if wait := s.lingerLeft(); wait > 0 {
		s.mu.Unlock()
		return nil, wait
	}
	var out []pipeline.LaneBatch
	for {
		b, ok := s.sched.Next(s.limits)
		if !ok {
			break
		}
		for i := range b.Records {
			s.queuedBytes -= len(b.Records[i].Payload)
		}
		s.inflight += len(b.Records)
		out = append(out, b)
	}
	inflight := s.inflight
	if len(out) > 0 {
		close(s.space)
		s.space = make(chan struct{})
	}
	s.mu.Unlock()
	if len(out) > 0 {
		s.obs.SetInflight(s.name, inflight)
	}
	return out, 0
}

func (s *Sink) lingerLeft() time.Duration {
	if s.linger <= 0 || s.sched.Len() >= s.limits.MaxEvents ||
		(s.limits.MaxBytes > 0 && s.queuedBytes >= s.limits.MaxBytes) {
		return 0
	}
	return s.linger - s.clock.Now().Sub(s.firstQueued)
}

type delivery struct {
	batch   pipeline.LaneBatch
	id      string
	body    []byte
	poison  int
	blocked bool
}

func (s *Sink) deliver(ctx context.Context, b pipeline.LaneBatch) {
	d := &delivery{batch: b, body: encodeBody(b.Records)}
	for attempt := 0; ; attempt++ {
		res := s.attempt(ctx, d)
		if ctx.Err() != nil {
			return
		}
		wait, done := s.handle(ctx, d, &res, attempt)
		if done {
			s.complete(ctx, d)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(wait):
		}
	}
}

func encodeBody(recs []pipeline.Record) []byte {
	n := 2
	for i := range recs {
		n += len(recs[i].Payload) + 1
	}
	body := make([]byte, 0, n)
	body = append(body, '[')
	for i := range recs {
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, recs[i].Payload...)
	}
	return append(body, ']')
}

func (s *Sink) complete(ctx context.Context, d *delivery) {
	s.mu.Lock()
	err := s.sched.Complete(d.batch)
	s.inflight -= len(d.batch.Records)
	inflight := s.inflight
	s.mu.Unlock()
	if err != nil {
		s.log.LogAttrs(ctx, slog.LevelError, "complete batch", slog.String("operation", "complete"),
			slog.String("request_id", d.id), slog.Any("error", err))
	}
	s.obs.SetInflight(s.name, inflight)
	s.wake()
}

func (s *Sink) ack(ctx context.Context, d *delivery, delivered bool) {
	recs := d.batch.Records
	for i := range recs {
		if err := recs[i].Ack.Ack(); err != nil {
			s.log.LogAttrs(ctx, slog.LevelError, "acknowledge record", slog.String("operation", "ack"),
				slog.String("request_id", d.id), slog.String("lsn", recs[i].ID.LSN.String()), slog.Any("error", err))
		}
		if delivered {
			s.ackObs.ObserveAck(s.name, recs[i].TsMs)
		}
	}
	if delivered {
		s.obs.ObserveAcked(s.name, len(recs))
	}
}
