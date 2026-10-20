// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// Defaults of the snapshot configuration.
const (
	DefaultChunkRows      = 10_000
	DefaultParallelTables = 4
)

// SlotCatalog is the part of pipeline.Catalog startup needs.
type SlotCatalog interface {
	Slot(ctx context.Context, name string) (pipeline.SlotStatus, error)
	CurrentTime(ctx context.Context) (time.Time, error)
}

// PrepareDeps are the ports startup uses.
type PrepareDeps struct {
	Slots   pipeline.SlotManager
	Catalog SlotCatalog
	Store   pipeline.StateStore
	Chunks  pipeline.ChunkReader
	Tables  TableSource
	Clock   clock.Clock
	Logger  *slog.Logger
}

// PrepareConfig is startup's part of the configuration.
type PrepareConfig struct {
	// SlotName is wake_<source.name>.
	SlotName string
	// Mode is snapshot.mode.
	Mode Mode
	// Reinit is `wake run --reinit-state`.
	Reinit bool
	// ChunkRows and ParallelTables are snapshot.chunk_rows and
	// snapshot.parallel_tables; zero means the default.
	ChunkRows      int
	ParallelTables int
}

// Plan is the outcome of Prepare: where streaming starts and what the
// pipeline must seed and run.
type Plan struct {
	Decision Decision
	SystemID string
	// State is the state as saved; NewStateKeeper takes it.
	State pipeline.State
	// StartLSN is the ack tracker's start and the reader's start: the slot's
	// confirmed LSN, or the consistent point of a slot just created.
	StartLSN lsn.LSN
	// Schemas seeds the stream's schema registry (stream.NewSchemas).
	Schemas schema.RegistryState
	// Initial is set when the initial snapshot runs (NewInitial); its
	// sessions are open, so replication may start.
	Initial *InitialSetup
}

// InitialSetup is an initial snapshot ready to copy: the run in the state,
// the slot's consistent point and exported-snapshot sessions.
type InitialSetup struct {
	Run             pipeline.SnapshotRun
	ConsistentPoint lsn.LSN
	// TsMs is the clock_timestamp() read before slot creation.
	TsMs     int64
	Tables   []Table
	Sessions []pipeline.ChunkSession
}

// Close ends the sessions; for error paths before the initial snapshot
// runs.
func (s *InitialSetup) Close(ctx context.Context) error {
	errs := make([]error, 0, len(s.Sessions))
	for _, sess := range s.Sessions {
		errs = append(errs, sess.Close(ctx))
	}
	s.Sessions = nil
	return errors.Join(errs...)
}

// Prepare runs the startup procedure before replication
// starts: it identifies the system, reads the slot and the state file,
// decides with Decide and carries the decision out. Creating the slot reads
// clock_timestamp(), saves the state without a consistent point (with the
// initial run when snapshot.mode is initial), creates the slot with
// EXPORT_SNAPSHOT, saves the consistent point and opens
// snapshot.parallel_tables sessions on the exported snapshot, so that
// START_REPLICATION may follow.
func Prepare(ctx context.Context, deps *PrepareDeps, cfg PrepareConfig) (*Plan, error) {
	if err := validatePrepare(deps, &cfg); err != nil {
		return nil, err
	}
	sys, err := deps.Slots.IdentifySystem(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: identify system: %w", err)
	}
	slot, err := deps.Catalog.Slot(ctx, cfg.SlotName)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read slot %s: %w", cfg.SlotName, err)
	}
	old, err := deps.Store.Load(ctx)
	found := err == nil
	if err != nil && !errors.Is(err, pipeline.ErrStateNotFound) {
		return nil, fmt.Errorf("snapshot: load state: %w", err)
	}
	facts := Facts{
		SlotName: cfg.SlotName, SystemID: sys.SystemID, Slot: slot, StateFound: found, State: old,
		Reinit: cfg.Reinit, Mode: cfg.Mode,
	}
	d, err := Decide(&facts)
	if err != nil {
		return nil, err
	}
	deps.Logger.LogAttrs(ctx, slog.LevelInfo, "startup decision: "+d.Reason,
		slog.String("component", "snapshot"), slog.String("operation", d.Action.String()),
		slog.String("lsn", slot.ConfirmedFlush.String()))
	p := &preparer{deps: deps, cfg: cfg, plan: &Plan{Decision: d, SystemID: sys.SystemID}}
	st, err := p.baseState(ctx, &facts)
	if err != nil {
		return nil, err
	}
	switch d.Action {
	case ActionResume:
		err = p.resume(ctx, &st, slot.ConfirmedFlush)
	case ActionRecreateSlot:
		if err = deps.Slots.DropSlot(ctx, cfg.SlotName); err == nil {
			err = p.create(ctx, &st)
		}
	case ActionCreateSlot:
		err = p.create(ctx, &st)
	}
	if err != nil {
		return nil, err
	}
	return p.plan, nil
}

func validatePrepare(deps *PrepareDeps, cfg *PrepareConfig) error {
	if !deps.complete() {
		return fmt.Errorf("%w: nil prepare dependency", ErrInvalidOption)
	}
	if cfg.SlotName == "" || (cfg.Mode != ModeInitial && cfg.Mode != ModeNever) {
		return fmt.Errorf("%w: slot %q, mode %q", ErrInvalidOption, cfg.SlotName, cfg.Mode)
	}
	if cfg.ChunkRows == 0 {
		cfg.ChunkRows = DefaultChunkRows
	}
	if cfg.ParallelTables == 0 {
		cfg.ParallelTables = DefaultParallelTables
	}
	if cfg.ChunkRows < 0 || cfg.ParallelTables < 0 {
		return fmt.Errorf("%w: chunk rows %d, parallel tables %d", ErrInvalidOption, cfg.ChunkRows, cfg.ParallelTables)
	}
	return nil
}

func (d *PrepareDeps) complete() bool {
	return d != nil && d.Slots != nil && d.Catalog != nil && d.Store != nil && d.Chunks != nil &&
		d.Tables != nil && d.Clock != nil && d.Logger != nil
}

type preparer struct {
	deps *PrepareDeps
	cfg  PrepareConfig
	plan *Plan
}

// baseState returns the state to continue from: the loaded one, or a fresh
// one that keeps the old schema versions and run counter; with Reinit the
// schema versions are reseeded and unfinished runs are logged.
func (p *preparer) baseState(ctx context.Context, f *Facts) (pipeline.State, error) {
	d := p.plan.Decision
	st := f.State.Clone()
	if !f.StateFound || d.FreshState {
		fresh := pipeline.NewState(p.cfg.SlotName, f.SystemID)
		if f.StateFound {
			fresh.RunCounter = f.State.RunCounter
			fresh.Schemas, fresh.SchemaFloor = st.Schemas, st.SchemaFloor
		}
		st = fresh
	}
	reg := schema.NewRegistry()
	if err := reg.Seed(RegistryState(&st)); err != nil {
		return pipeline.State{}, fmt.Errorf("snapshot: schema versions in the state file: %w", err)
	}
	if d.Reinit {
		reg.Reinit(p.deps.Clock.Now().Unix())
		p.logDiscarded(ctx, &f.State)
		st.Snapshot = pipeline.SnapshotState{}
	}
	rs := reg.State()
	setRegistryState(&st, &rs)
	p.plan.Schemas = rs
	return st, nil
}

// logDiscarded logs the unfinished runs and queued tables --reinit-state
// discards (rerun `wake snapshot table`).
func (p *preparer) logDiscarded(ctx context.Context, old *pipeline.State) {
	for _, r := range old.Snapshot.Runs {
		for _, t := range r.Tables {
			if !t.Done {
				p.deps.Logger.LogAttrs(ctx, slog.LevelWarn,
					"--reinit-state discards an unfinished snapshot; rerun `wake snapshot table`",
					slog.String("component", "snapshot"), slog.String("operation", "reinit"),
					slog.String("table", t.Table), slog.Uint64("count", t.Rows))
			}
		}
	}
	for _, t := range old.Snapshot.Queue {
		p.deps.Logger.LogAttrs(ctx, slog.LevelWarn, "--reinit-state discards a queued snapshot request",
			slog.String("component", "snapshot"), slog.String("operation", "reinit"), slog.String("table", t))
	}
}

func (p *preparer) resume(ctx context.Context, st *pipeline.State, confirmed lsn.LSN) error {
	if p.plan.Decision.FreshState {
		// The slot's own consistent point is unknown; its confirmed LSN marks
		// that streaming happened from it.
		st.ConsistentPoint = confirmed
		if err := p.deps.Store.Save(ctx, *st); err != nil {
			return fmt.Errorf("snapshot: save state: %w", err)
		}
	}
	p.plan.State, p.plan.StartLSN = *st, confirmed
	return nil
}

func (p *preparer) create(ctx context.Context, st *pipeline.State) error {
	now, err := p.deps.Catalog.CurrentTime(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: read clock_timestamp(): %w", err)
	}
	st.ConsistentPoint, st.ConsistentPointTsMs = lsn.InvalidLSN, now.UnixMilli()
	st.Snapshot = pipeline.SnapshotState{}
	var tables []Table
	if p.plan.Decision.Initial {
		if tables, err = p.deps.Tables.CapturedTables(ctx); err != nil {
			return err
		}
	}
	if len(tables) > 0 {
		run := pipeline.SnapshotRun{ID: st.NextRunID(), Kind: pipeline.SnapshotKindInitial, ChunkRows: p.cfg.ChunkRows}
		for _, t := range tables {
			run.Tables = append(run.Tables, pipeline.TableProgress{Table: t.Name})
		}
		st.Snapshot.Runs = []pipeline.SnapshotRun{run}
	}
	// Written before the slot exists: a crash before the next
	// save leaves no consistent point, and the slot is recreated.
	if err = p.deps.Store.Save(ctx, *st); err != nil {
		return fmt.Errorf("snapshot: save state before slot creation: %w", err)
	}
	info, err := p.deps.Slots.CreateSlot(ctx, p.cfg.SlotName)
	if err != nil {
		return fmt.Errorf("snapshot: create slot %s: %w", p.cfg.SlotName, err)
	}
	st.ConsistentPoint = info.ConsistentPoint
	if err = p.deps.Store.Save(ctx, *st); err != nil {
		return fmt.Errorf("snapshot: save consistent point: %w", err)
	}
	p.plan.State, p.plan.StartLSN = *st, info.ConsistentPoint
	if len(tables) == 0 {
		return nil
	}
	setup := &InitialSetup{
		Run: st.Snapshot.Runs[0], ConsistentPoint: info.ConsistentPoint, TsMs: st.ConsistentPointTsMs, Tables: tables,
	}
	for range min(p.cfg.ParallelTables, len(tables)) {
		sess, err := p.deps.Chunks.OpenExported(ctx, info.SnapshotName)
		if err != nil {
			return errors.Join(fmt.Errorf("snapshot: import exported snapshot %s: %w", info.SnapshotName, err),
				setup.Close(ctx))
		}
		setup.Sessions = append(setup.Sessions, sess)
	}
	p.plan.Initial = setup
	return nil
}
