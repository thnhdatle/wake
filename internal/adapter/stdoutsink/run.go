// SPDX-License-Identifier: Apache-2.0

package stdoutsink

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Run writes queued events in order until ctx is canceled.
func (s *Sink) Run(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	for {
		batch, ok := s.collect(ctx)
		if !ok {
			return nil
		}
		s.write(ctx, batch)
	}
}

func (s *Sink) collect(ctx context.Context) ([]pipeline.Record, bool) {
	var batch []pipeline.Record
	select {
	case <-ctx.Done():
		return nil, false
	case rec := <-s.queue:
		batch = append(batch, rec)
	}
	size := len(batch[0].Payload)
	var lingerC <-chan time.Time
	if s.linger > 0 {
		lingerC = s.clock.After(s.linger)
	}
	for len(batch) < s.limits.MaxEvents && (s.limits.MaxBytes <= 0 || size < s.limits.MaxBytes) {
		var rec pipeline.Record
		if lingerC == nil {
			select {
			case rec = <-s.queue:
			default:
				return batch, true
			}
		} else {
			select {
			case <-ctx.Done():
				return nil, false
			case <-lingerC:
				return batch, true
			case rec = <-s.queue:
			}
		}
		batch = append(batch, rec)
		size += len(rec.Payload)
	}
	return batch, true
}

func (s *Sink) write(ctx context.Context, batch []pipeline.Record) {
	s.buf = s.buf[:0]
	for i := range batch {
		s.buf = append(s.buf, batch[i].Payload...)
		s.buf = append(s.buf, '\n')
	}
	s.obs.SetInflight(s.name, len(batch))
	defer s.obs.SetInflight(s.name, 0)
	pending := s.buf
	for attempt := 0; ; attempt++ {
		start := s.clock.Now()
		s.obs.ObserveSent(s.name, len(batch), len(pending))
		n, err := s.out.Write(pending)
		pending = pending[min(max(n, 0), len(pending)):]
		if err == nil && len(pending) > 0 {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = s.flush()
		}
		s.obs.ObserveLatency(s.name, s.clock.Now().Sub(start))
		if err == nil {
			break
		}
		s.obs.ObserveRetry(s.name, RetryReasonWrite)
		s.setWriteErr(err.Error())
		level := slog.LevelDebug
		if attempt == 0 {
			level = slog.LevelWarn
		}
		s.log.LogAttrs(ctx, level, "write failed, retrying", slog.String("operation", "write"),
			slog.Int("attempt", attempt+1), slog.Int("count", len(batch)), slog.Any("error", err))
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(pipeline.Backoff{}.Delay(min(attempt, cappedAttempt), s.rnd.Float64())):
		}
	}
	s.setWriteErr("")
	for i := range batch {
		if err := batch[i].Ack.Ack(); err != nil {
			s.log.LogAttrs(ctx, slog.LevelError, "acknowledge record", slog.String("operation", "ack"),
				slog.String("lsn", batch[i].ID.LSN.String()), slog.Any("error", err))
		}
		s.ackObs.ObserveAck(s.name, batch[i].TsMs)
	}
	s.obs.ObserveAcked(s.name, len(batch))
}

func (s *Sink) flush() error {
	f, ok := s.out.(interface{ Flush() error })
	if !ok {
		return nil
	}
	if err := f.Flush(); err != nil {
		return fmt.Errorf("stdoutsink %s: flush: %w", s.name, err)
	}
	return nil
}
