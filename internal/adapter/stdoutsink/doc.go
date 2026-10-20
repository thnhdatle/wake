// SPDX-License-Identifier: Apache-2.0

// Package stdoutsink implements pipeline.Sink for the `stdout` sink type:
// every record's JSON payload is written as one line to an
// io.Writer (default os.Stdout), in Send order. Records are acknowledged
// once their batch is written and flushed; a failed write is retried without
// limit with capped, jittered backoff, resuming after the bytes
// already written.
//
// Main types: Sink, Observer, AckObserver.
//
//	s, err := stdoutsink.New(cfg, clock.System{}, logger)
//	if err != nil {
//		return err
//	}
//	g.Go(func() error { return s.Run(ctx) })
//	err = s.Send(ctx, rec) // blocks while the queue is full
package stdoutsink
