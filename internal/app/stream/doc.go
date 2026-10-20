// SPDX-License-Identifier: Apache-2.0

// Package stream implements the streaming use case: reading
// the replication stream, assembling transactions into CDC event records, routing
// them to sinks, tracking their acknowledgements, computing the confirmed
// LSN and sending standby status updates.
//
// Reader starts replication, decodes pgoutput, assigns ids (index_in_tx,
// schema-change placement, forwarded messages), spills transactions above
// stream.max_tx_bytes through pipeline.SpillStore, holds transactions back
// behind a Gate during the initial snapshot, feeds a pipeline.TxObserver
// (the snapshotter) and dispatches each transaction at Commit in WAL order.
// Schemas is the concurrency-safe schema version registry; Router.OpenTx
// registers the initial-snapshot pseudo-transaction.
//
// AckTracker holds the registered transactions in commit order and computes
// the confirmed LSN as the end LSN of the contiguous acknowledged prefix.
// Router maps tables to sinks by `tables` globs, registers a
// delivery per record and sink, and hands the records to the sinks.
// StatusUpdater reports the confirmed LSN to Postgres on its own goroutine,
// so updates continue while reading is paused.
//
//	tracker, _ := stream.NewAckTracker(slotConfirmedLSN)
//	router, _ := stream.NewRouter(tracker, []stream.SinkRoute{{Sink: kafka, Tables: []string{"*"}}})
//	status, _ := stream.NewStatusUpdater(replication, tracker, clock.System{})
//	go status.Run(ctx)
//	// per assembled transaction, in commit order:
//	_, err := router.Dispatch(ctx, &batch, clk.Now())
package stream
