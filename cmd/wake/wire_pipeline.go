// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/oklog/ulid/v2"

	"github.com/thnhdatle/wake/internal/adapter/alerthook"
	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/adapter/spill"
	"github.com/thnhdatle/wake/internal/adapter/statefile"
	"github.com/thnhdatle/wake/internal/app/lag"
	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/app/stream"
)

// wirePipeline connects to the source, runs the startup checks and the
// publication step, then the startup procedure (snapshot.Prepare) and wires
// every component in the order of the snapshot package documentation.
func wirePipeline(ctx context.Context, w *wiring) (*service, error) {
	cfg := &w.cfg
	col, err := newCollectors(w.reg)
	if err != nil {
		return nil, err
	}
	latency, err := lag.NewLatencyRecorder(col.lag, w.clk)
	if err != nil {
		return nil, err
	}
	sinks, err := buildSinks(cfg, w.clk, w.log, col.sinks, latencyAcks{rec: latency})
	if err != nil {
		return nil, err
	}
	base, err := newResolver(cfg)
	if err != nil {
		return nil, &exitError{code: exitConfig, err: err}
	}
	resolver := newLiveResolver(base)
	src, err := openSource(ctx, cfg, w.dsn, w.log)
	if err != nil {
		return nil, err
	}
	s := &service{cfg: cfg, log: w.log, clk: w.clk, src: src, sinks: sinks}
	if err := s.startup(ctx, w, resolver, col); err != nil {
		return nil, errors.Join(err, src.close(ctx))
	}
	return s, nil
}

// startup runs the pre-flight checks and the publication step, then builds
// the runtime components.
func (s *service) startup(ctx context.Context, w *wiring, resolver *liveResolver, col *collectors) error {
	report, err := runChecks(ctx, s.src, s.cfg, resolver)
	if err != nil {
		return err
	}
	if err = judge(ctx, s.log, &report); err != nil {
		return err
	}
	synced, err := syncPublications(ctx, s.src, s.cfg, resolver)
	if err != nil {
		return err
	}
	tables, err := snapshot.NewCatalogTables(s.src.catalog, synced.publications, resolver)
	if err != nil {
		return err
	}
	captured, err := tables.CapturedTables(ctx)
	if err != nil {
		return err
	}
	names := report.Findings[:0:0]
	names = append(names, collisionFinding(captured))
	if err := judge(ctx, s.log, &pgsql.Report{Findings: names}); err != nil {
		return err
	}
	rt := &runtimeDeps{publications: synced.publications, tables: tables, resolver: resolver, col: col}
	if err := s.build(ctx, w, rt); err != nil {
		return err
	}
	known := make([]string, len(captured))
	for i := range captured {
		known[i] = captured[i].Name
	}
	s.refresher = newRefresher(s, w, resolver, known)
	if s.plan.Decision.Action == snapshot.ActionResume && s.cfg.Snapshot.AddedTables && len(synced.added) > 0 {
		s.log.Info(fmt.Sprintf("tables published while Wake was stopped: %v", synced.added), "component", "startup",
			"count", len(synced.added))
		return s.snap.AddTables(ctx, synced.added)
	}
	return nil
}

// runtimeDeps are the startup outputs build needs.
type runtimeDeps struct {
	publications []string
	tables       *snapshot.CatalogTables
	resolver     *liveResolver
	col          *collectors
}

// build runs the startup procedure and constructs the pipeline: Prepare →
// StateKeeper → Schemas → AckTracker → Router → StatusUpdater → Initial
// (only with an initial snapshot) → Snapshotter → Heartbeat → Reader → lag
// Monitor.
func (s *service) build(ctx context.Context, w *wiring, rt *runtimeDeps) error {
	cfg := s.cfg
	store, err := statefile.New(cfg.State.Path)
	if err != nil {
		return withCode(codeConfigInvalid, err)
	}
	chunks, err := pgsql.NewChunkReader(s.src.pool, pgsql.WithLogger(s.log))
	if err != nil {
		return err
	}
	s.plan, err = snapshot.Prepare(ctx, &snapshot.PrepareDeps{
		Slots: s.src.conn, Catalog: s.src.catalog, Store: store, Chunks: chunks, Tables: rt.tables,
		Clock: s.clk, Logger: s.log,
	}, snapshot.PrepareConfig{
		SlotName: slotName(cfg), Mode: snapshot.Mode(cfg.Snapshot.Mode), Reinit: w.reinit,
		ChunkRows: cfg.Snapshot.ChunkRows, ParallelTables: cfg.Snapshot.ParallelTables,
	})
	if err != nil {
		return err
	}
	if err := s.buildStream(rt, store, chunks); err != nil {
		if s.plan.Initial != nil {
			err = errors.Join(err, s.plan.Initial.Close(ctx))
		}
		return err
	}
	return nil
}

// buildStream constructs the components after Prepare.
func (s *service) buildStream(rt *runtimeDeps, store *statefile.Store, chunks *pgsql.ChunkReader) error {
	router, schemas, err := s.buildCore(store)
	if err != nil {
		return err
	}
	readerOpts, err := s.buildSnapshot(router, schemas, rt, chunks)
	if err != nil {
		return err
	}
	if err := s.buildReader(router, schemas, rt, readerOpts); err != nil {
		return err
	}
	return s.buildMonitor(rt.col)
}

// buildCore constructs the state keeper, schema registry, ack tracker,
// router and standby status updater.
func (s *service) buildCore(store *statefile.Store) (*stream.Router, *stream.Schemas, error) {
	var err error
	if s.keeper, err = snapshot.NewStateKeeper(store, &s.plan.State); err != nil {
		return nil, nil, err
	}
	schemas, err := stream.NewSchemas(s.plan.Schemas)
	if err != nil {
		return nil, nil, err
	}
	if s.tracker, err = stream.NewAckTracker(s.plan.StartLSN); err != nil {
		return nil, nil, err
	}
	routes := make([]stream.SinkRoute, len(s.sinks))
	for i, sk := range s.sinks {
		routes[i] = stream.SinkRoute{Sink: sk.Sink, Tables: sk.tables}
	}
	router, err := stream.NewRouter(s.tracker, routes)
	if err != nil {
		return nil, nil, err
	}
	s.repl = newReplActivity(s.src.conn, s.clk)
	s.status, err = stream.NewStatusUpdater(s.repl, s.tracker, s.clk,
		stream.WithStatusInterval(s.cfg.Stream.StatusInterval.Std()),
		stream.WithMinConfirmBytes(uint64(max(s.cfg.Stream.MinConfirmBytes.Int64(), 0))))
	if err != nil {
		return nil, nil, err
	}
	return router, schemas, nil
}

// buildSnapshot constructs the initial snapshot (when Prepare planned one),
// the table snapshotter and the heartbeat, and returns the reader options
// that connect them.
func (s *service) buildSnapshot(router *stream.Router, schemas *stream.Schemas, rt *runtimeDeps,
	chunks *pgsql.ChunkReader,
) ([]stream.ReaderOption, error) {
	cfg := s.cfg
	opts := []stream.ReaderOption{
		stream.WithSchemaStore(s.keeper), stream.WithKeyColumns(s.src.catalog),
		stream.WithReaderLogger(s.log), stream.WithReaderMetrics(rt.col.reader),
	}
	var (
		after <-chan struct{}
		err   error
	)
	if s.plan.Initial != nil {
		s.initial, err = snapshot.NewInitial(snapshot.InitialDeps{
			Keeper: s.keeper, Opener: router, Versions: schemas, Clock: s.clk, Logger: s.log,
			Metrics: rt.col.snapshot,
		}, s.plan.Initial)
		if err != nil {
			return nil, err
		}
		opts = append(opts, stream.WithGate(s.initial))
		after = s.initial.Done()
	}
	emitter, err := pgsql.NewEmitter(s.src.pool, pgsql.WithLogger(s.log))
	if err != nil {
		return nil, err
	}
	// The nonce is a ULID per process: watermarks left by a crashed process
	// carry another one and are ignored.
	s.snap, err = snapshot.NewSnapshotter(snapshot.SnapshotterDeps{
		Keeper: s.keeper, Chunks: chunks, Emitter: emitter, Tables: rt.tables, Versions: schemas, Clock: s.clk,
	}, snapshot.SnapshotterConfig{
		Nonce: ulid.Make().String(), ChunkRows: cfg.Snapshot.ChunkRows, AddedTables: cfg.Snapshot.AddedTables,
	}, snapshot.WithAfter(after), snapshot.WithMetrics(rt.col.snapshot), snapshot.WithLogger(s.log))
	if err != nil {
		return nil, err
	}
	s.heartbeat, err = lag.NewHeartbeat(emitter, s.clk, lag.WithHeartbeatInterval(cfg.Heartbeat.Interval.Std()),
		lag.WithHeartbeatLogger(s.log))
	if err != nil {
		return nil, err
	}
	return append(opts, stream.WithObserver(s.snap), stream.WithActivity(s.heartbeat)), nil
}

// buildReader constructs the stream reader; its spill store deletes the
// spill files of a crashed run when the reader starts.
func (s *service) buildReader(router *stream.Router, schemas *stream.Schemas, rt *runtimeDeps,
	opts []stream.ReaderOption,
) error {
	cfg := s.cfg
	spillStore, err := spill.New(cfg.Stream.SpillDir)
	if err != nil {
		return withCode(codeConfigInvalid, err)
	}
	s.reader, err = stream.NewReader(stream.ReaderDeps{
		Source: s.repl, Router: router, Status: s.status, Schemas: schemas, Tables: rt.resolver,
		Spill: spillStore, Clock: s.clk,
	}, stream.ReaderConfig{
		Slot: slotName(cfg), Publications: rt.publications, Start: s.plan.StartLSN,
		MaxTxBytes: cfg.Stream.MaxTxBytes.Int64(), EmitTruncate: cfg.Events.EmitTruncate,
		EmitMessages: cfg.Events.EmitMessages,
	}, opts...)
	return err
}

// buildMonitor constructs the lag monitor; the alert notifier exists only
// when lag.alert_webhook is set.
func (s *service) buildMonitor(col *collectors) error {
	lc := s.cfg.Lag
	reporters := make([]lag.SinkHealthReporter, len(s.sinks))
	for i, sk := range s.sinks {
		reporters[i] = sk.Sink
	}
	opts := []lag.MonitorOption{
		lag.WithInterval(lc.Interval.Std()), lag.WithWarnBytes(lc.WarnBytes.Int64()),
		lag.WithCriticalBytes(lc.CriticalBytes.Int64()), lag.WithMaxAckAge(lc.MaxAckAge.Std()),
		lag.WithSinks(reporters...), lag.WithObserver(col.lag), lag.WithLogger(s.log),
	}
	if lc.AlertWebhook != "" {
		hook, err := alerthook.New(lc.AlertWebhook)
		if err != nil {
			return withCode(codeConfigInvalid, err)
		}
		opts = append(opts, lag.WithNotifier(hook))
	}
	var err error
	s.monitor, err = lag.NewMonitor(s.src.catalog, slotName(s.cfg), s.tracker, s.clk, opts...)
	return err
}
