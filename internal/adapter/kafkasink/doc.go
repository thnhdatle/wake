// SPDX-License-Identifier: Apache-2.0

// Package kafkasink implements pipeline.Sink for the `kafka` sink type
// (the Kafka transport of the CDC event contract) with franz-go.
//
// # Records
//
// An event goes to topic `<kafka.topic_prefix>.<Destination>`
// (`<schema>.<table>`, `_schema` or `_messages`) with the record key
// Record.Key (canonical key JSON) and the headers wake-event-id and
// wake-schema-version. With kafka.tombstones (default) a `d` event is
// followed by a tombstone: same key, null value, no headers. A `t` event
// (no key) is written to every partition of its topic, with the same id;
// the partition count is read from the cluster for each `t` event. A
// forwarded message (no key) is partitioned by its id.
//
// # Acknowledgements
//
// rec.Ack fires once Kafka acknowledged every record of the event (acks=all):
// a `d` event once its tombstone is, a `t` event once every partition write
// is. Acknowledgements may come out of Send order; the ack tracker accepts
// that.
//
// # Ordering
//
// The producer is idempotent with acks=all, unlimited record retries and no
// delivery timeout (franz-go keeps at most five produce requests in flight
// per broker, which idempotence keeps in order), so franz-go retries
// transient errors (network, leader change, not enough replicas, timeouts)
// itself without reordering. Keys hash to partitions with
// Kafka's murmur2 (the Java client's partition for the same key and count).
// A single dispatcher goroutine produces events in Send order. When franz-go
// fails a record (non-retriable error, or an unknown topic after its
// internal retries) it fails every later buffered record of that partition
// too; the dispatcher then stops producing, waits until every record in
// flight completed, and re-produces the failed records in their original
// order after a capped, jittered backoff, waiting for them before
// producing anything newer. Remaining limitation: a record produced in the
// instant between a broker-side rejection and its promise reaching the sink
// may be written before the rejected records are retried; it needs a
// non-retriable broker rejection (message too large at the broker,
// authorization) that clears within that instant. Consumers ordering by
// `seq` are unaffected.
//
// # Errors
//
// Before an event is produced the dispatcher checks it: an event above
// max_event_bytes (default 1 MiB; size = key + value + headers) blocks the
// sink with code event_too_large (table, key, lsn in the reason; only a
// configuration change on restart fixes it); a missing topic is created
// when kafka.auto_create_topics is on (kafka.partitions,
// kafka.replication_factor, -1 = broker default) and blocks the sink with
// sink_fatal otherwise; a record above the topic's max.message.bytes blocks
// it with sink_fatal. Kafka failures are classified: authentication,
// authorization, certificate errors, message too large, unknown topic (with
// auto-create off) and other non-retriable codes are sink_fatal; everything
// else is retrying. Every blocked check and failed record is retried
// without limit at the capped backoff, nothing after it is produced and
// nothing is acknowledged, and Health clears without a restart once the
// cause is fixed (topic created, limit raised, credentials or ACLs fixed).
// Records in flight without an acknowledgement for the stall timeout
// (default 10 s, e.g. every broker down) make Health report retrying.
//
// # Bounds and backpressure
//
// Send blocks while the sink holds DefaultMaxBufferedRecords (10,000) events
// or DefaultMaxBufferedBytes (64 MiB) of event bytes accepted and not yet
// acknowledged (WithMaxBuffered); an event larger than the byte bound holds
// the whole bound. franz-go's own buffer is sized above that (two records
// per event plus room for `t` fan-out), so it never blocks first. Batches
// are bounded by max_bytes (raised to fit one event of max_event_bytes,
// lowered to the topic's max.message.bytes) and linger; franz-go has no
// per-batch record limit, so max_events does not apply.
//
// Main types: Sink, Observer, AckObserver.
//
//	s, err := kafkasink.New(cfg, os.Getenv, clock.System{}, logger,
//		kafkasink.WithObserver(m), kafkasink.WithAckObserver(acks))
//	if err != nil {
//		return err
//	}
//	g.Go(func() error { return s.Run(ctx) })
//	err = s.Send(ctx, rec) // blocks while the buffer is full
package kafkasink
