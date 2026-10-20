// SPDX-License-Identifier: Apache-2.0

// Package webhooksink implements pipeline.Sink for the `webhook` sink type:
// it POSTs batches of events as a JSON array to webhook.url.
//
// Records hash by partition key into webhook.lanes lanes
// (pipeline.LaneScheduler), each lane has at most one batch in flight, so
// records of one key arrive in order. Barrier records (schema changes, `t`
// events, forwarded messages) wait until every lane has flushed, are sent
// alone, and the lanes resume after them. Each request carries
//
//	Content-Type: application/json
//	Wake-Delivery-Id: <ULID of the batch, unchanged across retries>
//	Wake-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, t + "." + body)>
//
// A 2xx response acknowledges every record of the batch. 408, 429, 5xx and
// network errors are retried without limit with capped, jittered backoff.
// Any other status is a poison response: after
// webhook.max_attempts poison responses the batch is written to
// webhook.dead_letter_dir as JSONL and acknowledged; without a dead-letter
// directory it keeps being retried at the capped backoff and Health reports
// sink_fatal until the receiver accepts it. An event larger than
// max_event_bytes blocks Send with Health event_too_large.
//
// Main types: Sink, Observer, AckObserver; Sign and Verify implement the
// signature scheme.
//
//	s, err := webhooksink.New(cfg, os.Getenv, clock.System{}, logger,
//		webhooksink.WithObserver(m), webhooksink.WithAckObserver(lat))
//	if err != nil {
//		return err
//	}
//	g.Go(func() error { return s.Run(ctx) })
//	err = s.Send(ctx, rec) // blocks while the queue is full
package webhooksink
