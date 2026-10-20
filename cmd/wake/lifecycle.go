// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	// drainTimeout bounds the wait for the sinks to acknowledge what they
	// already hold once reading stopped (together with the sinks' own final
	// flush it stays below the 30 s shutdown budget).
	drainTimeout = 10 * time.Second
	// drainPoll is how often the drain checks for pending deliveries.
	drainPoll = 50 * time.Millisecond
	// flushTimeout bounds the final standby status update.
	flushTimeout = 5 * time.Second
)

// run runs the pipeline until ctx ends or a component fails, then shuts down
// in this order:
//
//  1. stop reading: the reader, the snapshots, the heartbeat, the lag
//     monitor and the table refresh end (their context is canceled);
//  2. drain: wait, bounded by drainTimeout, until the sinks acknowledged
//     every delivery they were handed, then stop the sinks (each flushes
//     within its own bound);
//  3. stop the status updater and send a final standby status update with
//     the confirmed LSN;
//  4. the caller closes the replication connection and the pool.
//
// A failing component (replication connection lost, sink client error)
// cancels reading the same way; run then returns its error (see runService
// for the restart policy).
func (s *service) run(ctx context.Context) error {
	readCtx, stopRead := context.WithCancel(ctx)
	defer stopRead()
	base := context.WithoutCancel(ctx)
	sinkCtx, stopSinks := context.WithCancel(base)
	defer stopSinks()
	statusCtx, stopStatus := context.WithCancel(base)
	defer stopStatus()

	// failFast stops reading when a sink or the status updater fails.
	failFast := func(run func(context.Context) error, c context.Context) func() error {
		return func() error {
			err := run(c)
			if err != nil {
				stopRead()
			}
			return err
		}
	}
	var sinks, status errgroup.Group
	for _, sk := range s.sinks {
		sinks.Go(failFast(sk.Run, sinkCtx))
	}
	status.Go(failFast(s.status.Run, statusCtx))

	readers, rctx := errgroup.WithContext(readCtx)
	readers.Go(func() error { return s.runReader(rctx) })
	if s.initial != nil {
		readers.Go(func() error { return s.initial.Run(rctx) })
	}
	readers.Go(func() error { return s.snap.Run(rctx) })
	readers.Go(func() error { return s.heartbeat.Run(rctx) })
	readers.Go(func() error { return s.monitor.Run(rctx) })
	readers.Go(func() error { return s.refresher.Run(rctx) })
	readErr := readers.Wait()

	s.log.Info("reading stopped; draining sinks", "component", "main")
	s.drain(base)
	stopSinks()
	sinkErr := sinks.Wait()
	stopStatus()
	statusErr := status.Wait()
	if err := s.flushStatus(base); err != nil {
		// After a failure the connection is usually gone too; the slot
		// keeps the last confirmed LSN the server received.
		s.log.Warn("final standby status update failed", "component", "main", "error", err,
			"lsn", s.tracker.Confirmed().String())
	}
	return errors.Join(readErr, sinkErr, statusErr)
}

// runReader runs the stream reader and records that it runs (readiness).
func (s *service) runReader(ctx context.Context) error {
	s.readerRunning.Store(true)
	defer s.readerRunning.Store(false)
	return s.reader.Run(ctx)
}

// drain waits until no delivery is pending or drainTimeout passed.
func (s *service) drain(ctx context.Context) {
	deadline := s.clk.Now().Add(drainTimeout)
	ticker := s.clk.NewTicker(drainPoll)
	defer ticker.Stop()
	for {
		if _, pending := s.tracker.OldestUnacked(); !pending {
			return
		}
		if !s.clk.Now().Before(deadline) {
			s.log.Warn("sinks did not acknowledge every delivery before shutdown; they are delivered again "+
				"after the restart", "component", "main", "lsn", s.tracker.Confirmed().String())
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
		}
	}
}

// flushStatus sends the final standby status update.
func (s *service) flushStatus(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	return s.status.Flush(ctx)
}
