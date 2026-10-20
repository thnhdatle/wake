// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// TxOpener registers a transaction with the stream's ack tracker and
// returns it unsealed; stream.Router implements it.
type TxOpener interface {
	OpenTx(ctx context.Context, commit, end lsn.LSN, readAt time.Time) (pipeline.HeldTx, error)
}

// VersionSource returns a table's current schema version; stream.Schemas
// implements it.
type VersionSource interface {
	Version(table string) uint64
}

// InitialDeps are the initial snapshot's dependencies.
type InitialDeps struct {
	Keeper   *StateKeeper
	Opener   TxOpener
	Versions VersionSource
	Clock    clock.Clock
	Logger   *slog.Logger
	// Metrics is optional.
	Metrics Metrics
}

// Initial copies the tables of the initial snapshot at the slot's exported
// snapshot. Each worker owns one session opened by Prepare
// and copies whole tables in keyset chunks of the run's chunk_rows; chunk
// counters are allocated and persisted before each chunk is emitted. The
// `r` events (id <consistent point>-r<run>-<n>, lsn the consistent point,
// ts_ms the persisted clock_timestamp, no tx_id) are routed into the
// pseudo-transaction at the consistent point, one completion group per
// chunk, registered in the ack tracker before any stream transaction.
//
// Initial implements stream.Gate: Released is closed once every row was
// routed and the pseudo-transaction sealed, so the stream's events follow
// the snapshot's on every sink. Progress (last acknowledged key per table)
// is persisted as chunks complete; Done is closed once every table is
// acknowledged and the run removed from the state.
type Initial struct {
	deps     InitialDeps
	setup    *InitialSetup
	released chan struct{}
	done     chan struct{}

	mu       sync.Mutex // protects progress
	progress []*chunkProgress
}

// NewInitial validates the dependencies. setup comes from Prepare.
func NewInitial(deps InitialDeps, setup *InitialSetup) (*Initial, error) {
	if deps.Keeper == nil || deps.Opener == nil || deps.Versions == nil || deps.Clock == nil || deps.Logger == nil {
		return nil, fmt.Errorf("%w: nil initial snapshot dependency", ErrInvalidOption)
	}
	if setup == nil || len(setup.Sessions) == 0 || setup.Run.ChunkRows <= 0 {
		return nil, fmt.Errorf("%w: initial snapshot without sessions or chunk size", ErrInvalidOption)
	}
	if deps.Metrics == nil {
		deps.Metrics = nopMetrics{}
	}
	return &Initial{deps: deps, setup: setup, released: make(chan struct{}), done: make(chan struct{})}, nil
}

// Released implements stream.Gate.
func (in *Initial) Released() <-chan struct{} { return in.released }

// Done returns a channel closed once the initial snapshot is acknowledged
// and finished; table snapshots wait for it.
func (in *Initial) Done() <-chan struct{} { return in.done }

// Run copies the tables, releases the stream, waits for the
// acknowledgements and finishes the run. It returns nil when ctx ends.
func (in *Initial) Run(ctx context.Context) error {
	err := in.run(ctx)
	if ctx.Err() != nil {
		return nil //nolint:nilerr // cancellation is the normal shutdown
	}
	return err
}

func (in *Initial) run(ctx context.Context) error {
	cp := in.setup.ConsistentPoint
	pseudo, err := in.deps.Opener.OpenTx(ctx, cp, cp, in.deps.Clock.Now())
	if err != nil {
		return errors.Join(fmt.Errorf("snapshot: register initial snapshot transaction: %w", err),
			in.setup.Close(ctx))
	}
	if err := in.copyAll(ctx, pseudo); err != nil {
		return err
	}
	if err := pseudo.Seal(); err != nil {
		return fmt.Errorf("snapshot: seal initial snapshot transaction: %w", err)
	}
	close(in.released)
	in.deps.Logger.LogAttrs(ctx, slog.LevelInfo, "initial snapshot emitted; stream released",
		slog.String("component", "snapshot"), slog.String("operation", "initial"),
		slog.String("lsn", cp.String()), slog.Int("count", len(in.setup.Tables)))
	if err := in.finishAll(ctx); err != nil {
		return err
	}
	if err := in.deps.Keeper.finishRun(ctx, in.setup.Run.ID); err != nil {
		return err
	}
	close(in.done)
	return nil
}

// finishAll waits for the acknowledgements of every table concurrently,
// persisting each table's progress as its chunks complete.
func (in *Initial) finishAll(ctx context.Context) error {
	errs := make([]error, len(in.progress))
	var wg sync.WaitGroup
	for i, p := range in.progress {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.finish(ctx)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// copyAll runs one worker per session over the table queue.
func (in *Initial) copyAll(ctx context.Context, pseudo pipeline.HeldTx) error {
	queue := make(chan Table, len(in.setup.Tables))
	for _, t := range in.setup.Tables {
		queue <- t
	}
	close(queue)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make([]error, len(in.setup.Sessions))
	var wg sync.WaitGroup
	for i, sess := range in.setup.Sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if errs[i] = in.worker(wctx, sess, queue, pseudo); errs[i] != nil {
				cancel()
			}
		}()
	}
	wg.Wait()
	in.setup.Sessions = nil
	return errors.Join(errs...)
}

func (in *Initial) worker(ctx context.Context, sess pipeline.ChunkSession, queue <-chan Table,
	pseudo pipeline.HeldTx,
) (err error) {
	defer func() {
		if cerr := sess.Close(ctx); cerr != nil && err == nil {
			err = fmt.Errorf("snapshot: close snapshot session: %w", cerr)
		}
	}()
	for t := range queue {
		if err := in.copyTable(ctx, sess, &t, pseudo); err != nil {
			return err
		}
	}
	return nil
}

func (in *Initial) copyTable(ctx context.Context, sess pipeline.ChunkSession, t *Table, pseudo pipeline.HeldTx) error {
	run := in.setup.Run
	p := newChunkProgress(in.deps.Keeper, in.deps.Metrics, run.ID, &pipeline.TableProgress{Table: t.Name})
	in.mu.Lock()
	in.progress = append(in.progress, p)
	in.mu.Unlock()
	version := in.deps.Versions.Version(t.Name)
	req := pipeline.ChunkRequest{Schema: t.Schema, Table: t.Table, Limit: run.ChunkRows}
	for {
		c, err := in.deps.Keeper.allocChunk(ctx, run.ID)
		if err != nil {
			return err
		}
		chunk, err := sess.ReadChunk(ctx, req)
		if err != nil {
			return fmt.Errorf("snapshot: read chunk %d of %s: %w", c, t.Name, err)
		}
		g, err := routeChunk(ctx, pseudo, &chunkEmit{
			table: t, run: run.ID, chunk: c, chunkRows: run.ChunkRows, at: in.setup.ConsistentPoint,
			tsMs: in.setup.TsMs, version: version, rows: chunk.Rows, readAt: in.deps.Clock.Now(),
		})
		if err != nil {
			return err
		}
		if err := p.add(ctx, c, chunk.Last, uint64(len(chunk.Rows)), g.Done()); err != nil {
			return err
		}
		if chunk.Done {
			return nil
		}
		req.After = chunk.Last
	}
}
