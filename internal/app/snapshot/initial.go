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

type TxOpener interface {
	OpenTx(ctx context.Context, commit, end lsn.LSN, readAt time.Time) (pipeline.HeldTx, error)
}

type VersionSource interface {
	Version(table string) uint64
}

type InitialDeps struct {
	Keeper   *StateKeeper
	Opener   TxOpener
	Versions VersionSource
	Clock    clock.Clock
	Logger   *slog.Logger
	Metrics  Metrics
}

// Initial copies the tables of the initial snapshot at the slot's exported snapshot.
type Initial struct {
	deps     InitialDeps
	setup    *InitialSetup
	released chan struct{}
	done     chan struct{}

	mu       sync.Mutex
	progress []*chunkProgress
}

// NewInitial validates the dependencies.
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

func (in *Initial) Released() <-chan struct{} { return in.released }

func (in *Initial) Done() <-chan struct{} { return in.done }

// Run copies the tables, releases the stream and waits for the acknowledgements.
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
