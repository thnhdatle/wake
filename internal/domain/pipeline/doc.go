// SPDX-License-Identifier: Apache-2.0

// Package pipeline declares the ports of Wake's CDC pipeline and the plain
// data they exchange, plus the pure delivery logic that several adapters share.
//
// Ports (ports.go), implemented by internal/adapter packages and consumed by
// internal/app: SlotManager and ReplicationSource (replication connection),
// Catalog, ChunkReader and MessageEmitter (SQL connection), Sink, StateStore,
// SpillStore and AlertNotifier. Acker is the reverse port through which sinks
// report acknowledgements back to the stream's ack tracker.
//
// Data: Record (one routed event, schema change or forwarded message with its
// pre-encoded JSON payload, partition key and AckHandle), TxBatch (an
// assembled transaction), ReplicationMessage, StandbyStatus, State (the state
// file), Alert, SinkHealth and the snapshot chunk types.
//
// Stream/snapshot contract: TxObserver (the snapshotter sees every stream
// transaction in WAL order and may hold a HIGH watermark transaction open),
// HeldTx and TxGroup (a registered, unsealed transaction and its completion
// groups, into which `r` events are routed), so that internal/app/snapshot
// never imports internal/app/stream.
//
// Shared pure logic: TableFilter (the `tables` glob semantics), TablePolicy
// (the capture, name_map and keyless rules behind TableResolver), LaneScheduler
// (webhook lanes with barrier records), LaneOf (key hashing) and Backoff
// (capped jittered exponential retry delay).
//
//	rec := pipeline.NewEventRecord(&ev)
//	lane := pipeline.LaneOf(rec.PartitionKey(), 8)
//	// a sink acknowledges the delivery once the destination holds it:
//	err := rec.Ack.Ack()
package pipeline
