// SPDX-License-Identifier: Apache-2.0

// Package natssink implements pipeline.Sink for the `nats` sink type
// (the NATS transport of the CDC event contract) with the nats.go JetStream API.
//
// # Messages
//
// An event is published to subject `<nats.subject_prefix>.<Destination>`
// (`<schema>.<table>`, `_schema` or `_messages`) with the header
// Nats-Msg-Id = the event id (never the key), so the stream's duplicate
// window drops re-published copies, and Nats-Expected-Stream = nats.stream,
// so a subject captured by another stream is rejected instead of stored
// elsewhere. With nats.create_stream (default) Run creates stream
// nats.stream (default wake_<source.name>) on `<subject_prefix>.>` with
// nats.duplicate_window (default 2 min), or adds the subject and window to
// an existing stream, keeping its other settings; otherwise it checks that
// the stream exists. rec.Ack fires on the event's PubAck (a duplicate PubAck
// counts: the stream already holds the message).
//
// # Ordering
//
// JetStream stores the messages of one connection in the order it receives
// them, so a single dispatcher goroutine publishes asynchronously in Send
// order. What could reorder a key is a message lost or rejected while a
// later one is stored, followed by a re-publication; the sink prevents it
// three ways. nats.go's own retry on "no responders" is off (it would
// re-publish later behind newer messages). The reconnect buffer is off, so
// a publish while disconnected fails at once instead of being replayed after
// reconnecting. And after a reconnect the dispatcher publishes nothing new
// until every message published on the previous connection got its PubAck
// or failed (nats.go fails them on disconnect, and after the ack timeout,
// default 10 s). A failed message stops the dispatcher: once nothing is in
// flight it re-publishes the failed messages in their original order after
// a capped, jittered backoff, waits for them, then continues;
// copies that the stream already stored are dropped by the duplicate
// window. Remaining limitation: JetStream rejecting one message with a
// transient error (e.g. no stream leader during a cluster election) while
// accepting the next one on the same connection lets the next one overtake
// it. Re-publications after more than the duplicate window (long outages,
// snapshot replays) may store duplicates (sinks.md).
//
// # Errors
//
// Before an event is published the dispatcher checks it: the stream must be
// ready; an event above max_event_bytes, or above the server's max_payload
// when max_event_bytes is 0 (the default), blocks the sink with code
// event_too_large (table, key, lsn in the reason); an event above the
// server's max_payload while max_event_bytes is larger blocks it with
// sink_fatal. Message size counts the payload and the headers. Publish
// errors are classified: authentication, authorization or permission
// failures, certificate errors, a missing stream with nats.create_stream
// off, a message above the stream's limit and other JetStream rejections
// are sink_fatal; disconnections, timeouts, no responders and 5xx are
// retrying. Every blocked check and failed message is retried without limit
// at the capped backoff, nothing newer is published and nothing is
// acknowledged, and Health clears without a restart once the cause is
// fixed. nats.go reconnects forever, also after rejected credentials, and
// re-reads nats.credentials_file on every attempt. A connection that is down
// makes Health report retrying after the stall timeout (default 10 s), or
// fatal at once when the server rejected the login.
//
// # Bounds and backpressure
//
// Send blocks while the sink holds DefaultMaxBufferedRecords (10,000) events
// or DefaultMaxBufferedBytes (64 MiB) of message bytes accepted and not yet
// acknowledged (WithMaxBuffered); the JetStream async publish limit is one
// above the record bound, so it never stalls first. max_events, max_bytes
// and linger do not apply: each event is one message.
//
// Main types: Sink, Observer, AckObserver.
//
//	s, err := natssink.New(cfg, clock.System{}, logger,
//		natssink.WithObserver(m), natssink.WithAckObserver(acks))
//	if err != nil {
//		return err
//	}
//	g.Go(func() error { return s.Run(ctx) })
//	err = s.Send(ctx, rec) // blocks while the buffer is full
package natssink
