// SPDX-License-Identifier: Apache-2.0

// Package snapshot implements startup and the snapshot use cases: the
// startup decision (Decide, Prepare), the state file owner at run
// time (StateKeeper), the initial snapshot at the slot's exported snapshot
// (Initial) and table snapshots through watermarks (Snapshotter).
//
// It never imports the stream package: the stream reader reaches the
// Snapshotter through pipeline.TxObserver, and the snapshot side reaches
// the ack tracker through pipeline.HeldTx and the consumer interfaces
// TxOpener and VersionSource, which stream.Router and stream.Schemas
// implement. Wiring order:
//
//	plan, err := snapshot.Prepare(ctx, &snapshot.PrepareDeps{...}, snapshot.PrepareConfig{...})
//	keeper, _ := snapshot.NewStateKeeper(store, &plan.State)
//	schemas, _ := stream.NewSchemas(plan.Schemas)
//	tracker, _ := stream.NewAckTracker(plan.StartLSN)
//	router, _ := stream.NewRouter(tracker, routes)
//	var gate stream.Gate // nil unless plan.Initial != nil
//	initial, _ := snapshot.NewInitial(snapshot.InitialDeps{
//		Keeper: keeper, Opener: router, Versions: schemas, ...}, plan.Initial)
//	snap, _ := snapshot.NewSnapshotter(snapshot.SnapshotterDeps{...}, cfg, snapshot.WithAfter(initial.Done()))
//	reader, _ := stream.NewReader(deps, rcfg, stream.WithObserver(snap), stream.WithGate(initial),
//		stream.WithSchemaStore(keeper))
//	// errgroup: reader.Run, initial.Run, snap.Run, status.Run, sinks' Run
package snapshot
