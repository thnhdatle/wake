// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/watermark"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// DefaultMaxChunkAttempts bounds the consecutive failed attempts of one
// chunk (read or emit errors, visibility retries) before Run fails.
const DefaultMaxChunkAttempts = 50

// SnapshotterDeps are the table snapshotter's dependencies.
type SnapshotterDeps struct {
	Keeper   *StateKeeper
	Chunks   pipeline.ChunkReader
	Emitter  pipeline.MessageEmitter
	Tables   TableSource
	Versions VersionSource
	Clock    clock.Clock
}

// SnapshotterConfig is the table snapshotter's configuration.
type SnapshotterConfig struct {
	// Nonce is this process' ULID in watermark payloads.
	Nonce string
	// ChunkRows is snapshot.chunk_rows for new runs; zero means the default.
	ChunkRows int
	// AddedTables is snapshot.added_tables.
	AddedTables bool
}

// SnapshotterOption configures a Snapshotter.
type SnapshotterOption func(*Snapshotter)

// WithAfter delays table snapshots until ch is closed (Initial.Done).
func WithAfter(ch <-chan struct{}) SnapshotterOption { return func(s *Snapshotter) { s.after = ch } }

// WithMetrics sets the metrics receiver.
func WithMetrics(m Metrics) SnapshotterOption { return func(s *Snapshotter) { s.metrics = m } }

// WithLogger sets the logger (default: discard).
func WithLogger(l *slog.Logger) SnapshotterOption { return func(s *Snapshotter) { s.logger = l } }

// WithRetryBackoff sets the delay between chunk attempts.
func WithRetryBackoff(b pipeline.Backoff) SnapshotterOption {
	return func(s *Snapshotter) { s.backoff = b }
}

// WithMaxChunkAttempts sets the bound on consecutive failed attempts of one
// chunk (default DefaultMaxChunkAttempts).
func WithMaxChunkAttempts(n int) SnapshotterOption { return func(s *Snapshotter) { s.maxAttempts = n } }

// Snapshotter runs table snapshots through watermarks: the
// unfinished runs of the state first (a resumed initial snapshot keeps its
// run_id), then queued tables (added tables and wake.snapshot requests),
// one table at a time, each queued table in a new run.
//
// Per chunk it persists the chunk counter, emits LOW, reads the chunk in
// its own REPEATABLE READ transaction, hands the keys to the merger, emits
// HIGH with the read's snapshot and committed xids and waits for the
// stream to read HIGH. The Snapshotter is also the stream reader's
// pipeline.TxObserver: it feeds every transaction to the table's
// watermark.Merger and, at the HIGH transaction, either reports a retry or
// asks the reader to hold the transaction open and routes the kept rows into
// it as `r` events (id <HIGH lsn>-r<run>-<n>, ts_ms the HIGH commit time)
// before any later stream transaction is dispatched. A chunk is complete
// when its group is acknowledged; progress advances over complete chunks.
type Snapshotter struct {
	deps        SnapshotterDeps
	cfg         SnapshotterConfig
	after       <-chan struct{}
	metrics     Metrics
	logger      *slog.Logger
	backoff     pipeline.Backoff
	maxAttempts int

	mu             sync.Mutex // protects the observer state below
	inTx           bool
	tx             pipeline.ObservedTx
	active         *activeTable
	pendingInstall *activeTable
	installed      chan struct{}
	txHigh         *highResult
	txTriggers     []string
	held           *highResult
	results        []chunkResult
	resultSignal   chan struct{} // capacity 1
}

// activeTable is the table being snapshotted, as the observer sees it.
type activeTable struct {
	run       uint64
	chunkRows int
	table     Table
	merger    *watermark.Merger
	rowsChunk uint64
	hasRows   bool
	rows      []pipeline.ChunkRow
}

// highResult is the merger's decision at a HIGH watermark of this process.
type highResult struct {
	chunk uint64
	out   watermark.Outcome
	err   error
	tx    pipeline.ObservedTx
}

// chunkResult is what the snapshot goroutine learns about a chunk.
type chunkResult struct {
	chunk    uint64
	retry    bool
	retryXID uint64
	err      error
	kept     uint64
	done     <-chan struct{}
}

// maxResults bounds undelivered chunk results; only results of abandoned
// attempts can accumulate, and the oldest is dropped.
const maxResults = 8

// NewSnapshotter validates the dependencies and configuration.
func NewSnapshotter(deps SnapshotterDeps, cfg SnapshotterConfig, opts ...SnapshotterOption) (*Snapshotter, error) {
	if deps.Keeper == nil || deps.Chunks == nil || deps.Emitter == nil || deps.Tables == nil ||
		deps.Versions == nil || deps.Clock == nil {
		return nil, fmt.Errorf("%w: nil snapshotter dependency", ErrInvalidOption)
	}
	probe := watermark.Payload{Nonce: cfg.Nonce, Run: 1, Table: "a.b", Mark: watermark.MarkLow}
	if err := probe.Validate(); err != nil {
		return nil, fmt.Errorf("%w: nonce: %w", ErrInvalidOption, err)
	}
	if cfg.ChunkRows == 0 {
		cfg.ChunkRows = DefaultChunkRows
	}
	s := &Snapshotter{
		deps: deps, cfg: cfg, metrics: nopMetrics{}, logger: slog.New(slog.DiscardHandler),
		maxAttempts: DefaultMaxChunkAttempts, resultSignal: make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(s)
	}
	if cfg.ChunkRows < 0 || s.maxAttempts <= 0 || s.metrics == nil || s.logger == nil {
		return nil, fmt.Errorf("%w: chunk rows %d, max attempts %d", ErrInvalidOption, cfg.ChunkRows, s.maxAttempts)
	}
	return s, nil
}

// AddTables queues table snapshots of tables detected after startup
// when snapshot.added_tables is on.
func (s *Snapshotter) AddTables(ctx context.Context, tables []string) error {
	if !s.cfg.AddedTables || len(tables) == 0 {
		return nil
	}
	return s.deps.Keeper.EnqueueTables(ctx, tables)
}

// Run executes snapshot runs until ctx ends; it returns nil then.
func (s *Snapshotter) Run(ctx context.Context) error {
	err := s.run(ctx)
	if ctx.Err() != nil {
		return nil //nolint:nilerr // cancellation is the normal shutdown
	}
	return err
}

func (s *Snapshotter) run(ctx context.Context) error {
	if s.after != nil {
		select {
		case <-s.after:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		w, ok := s.deps.Keeper.nextWork()
		if !ok {
			select {
			case <-s.deps.Keeper.Changed():
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		var err error
		if w.run != nil {
			err = s.execRun(ctx, w.run)
		} else {
			err = s.startQueued(ctx, w.queued)
		}
		if err != nil {
			return err
		}
	}
}

// startQueued starts a run for the queued table, or drops a request for a
// table that is not captured.
func (s *Snapshotter) startQueued(ctx context.Context, name string) error {
	tables, err := s.deps.Tables.CapturedTables(ctx)
	if err != nil {
		return err
	}
	if _, ok := findTable(tables, name); !ok {
		s.metrics.Request(OtherTable)
		s.logger.LogAttrs(ctx, slog.LevelWarn, "snapshot request ignored: table not captured",
			slog.String("component", "snapshot"), slog.String("operation", "request"), slog.String("table", name))
		return s.deps.Keeper.dropQueued(ctx, name)
	}
	s.metrics.Request(name)
	run, err := s.deps.Keeper.startRun(ctx, name, s.cfg.ChunkRows)
	if err != nil {
		return err
	}
	return s.execRun(ctx, &run)
}

// execRun snapshots the unfinished tables of a run, one at a time, and
// removes the run.
func (s *Snapshotter) execRun(ctx context.Context, run *pipeline.SnapshotRun) error {
	tables, err := s.deps.Tables.CapturedTables(ctx)
	if err != nil {
		return err
	}
	for i := range run.Tables {
		tp := &run.Tables[i]
		if tp.Done {
			continue
		}
		t, ok := findTable(tables, tp.Table)
		if !ok {
			s.logger.LogAttrs(ctx, slog.LevelWarn, "snapshot of a table no longer captured abandoned",
				slog.String("component", "snapshot"), slog.String("operation", "run"), slog.String("table", tp.Table))
			if err := s.deps.Keeper.setProgress(ctx, run.ID, tp.Table, tp.Position, tp.Rows, true); err != nil {
				return err
			}
			continue
		}
		if err := s.snapshotTable(ctx, run, &t, tp); err != nil {
			return err
		}
	}
	return s.deps.Keeper.finishRun(ctx, run.ID)
}

func (s *Snapshotter) snapshotTable(ctx context.Context, run *pipeline.SnapshotRun, t *Table,
	tp *pipeline.TableProgress,
) error {
	defer s.uninstall()
	if err := s.install(ctx, run, t); err != nil {
		return err
	}
	s.logger.LogAttrs(ctx, slog.LevelInfo, "table snapshot started",
		slog.String("component", "snapshot"), slog.String("operation", "run"), slog.String("table", t.Name),
		slog.Uint64("count", tp.Rows))
	p := newChunkProgress(s.deps.Keeper, s.metrics, run.ID, tp)
	pos := tp.Position
	failures := 0
	for {
		c, err := s.deps.Keeper.allocChunk(ctx, run.ID)
		if err != nil {
			return err
		}
		chunk, res, err := s.chunk(ctx, run, t, c, pos)
		if err == nil && res.err != nil {
			err = res.err
		}
		if err != nil || res.retry {
			if failures++; failures >= s.maxAttempts {
				return fmt.Errorf("%w: %s chunk %d: %w", ErrChunkAttempts, t.Name, c, err)
			}
			if werr := s.retryWait(ctx, t, c, &res, err, failures); werr != nil {
				return werr
			}
			continue
		}
		failures = 0
		if err := p.add(ctx, c, chunk.Last, res.kept, res.done); err != nil {
			return err
		}
		if chunk.Done {
			return p.finish(ctx)
		}
		pos = chunk.Last
	}
}

// retryWait logs a failed attempt and waits the backoff delay.
func (s *Snapshotter) retryWait(ctx context.Context, t *Table, c uint64, res *chunkResult, err error,
	attempt int,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if res.retry {
		s.metrics.ChunkRetry(t.Name)
		s.logger.LogAttrs(ctx, slog.LevelInfo, "chunk re-run: a transaction committed before the retention "+
			"horizon is invisible to the chunk read", slog.String("component", "snapshot"),
			slog.String("operation", "chunk"), slog.String("table", t.Name), slog.Uint64("key", res.retryXID),
			slog.Uint64("count", c), slog.Int("attempt", attempt))
	} else {
		s.logger.LogAttrs(ctx, slog.LevelWarn, "chunk failed, retrying", slog.String("component", "snapshot"),
			slog.String("operation", "chunk"), slog.String("table", t.Name), slog.Uint64("count", c),
			slog.Int("attempt", attempt), slog.Any("error", err))
	}
	select {
	case <-s.deps.Clock.After(s.backoff.Delay(attempt-1, 0.5)):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// chunk runs one attempt of the watermark procedure for chunk c.
func (s *Snapshotter) chunk(ctx context.Context, run *pipeline.SnapshotRun, t *Table, c uint64,
	pos json.RawMessage,
) (pipeline.Chunk, chunkResult, error) {
	low := watermark.Payload{Nonce: s.cfg.Nonce, Run: run.ID, Table: t.Name, Chunk: c, Mark: watermark.MarkLow}
	if err := s.emit(ctx, &low); err != nil {
		return pipeline.Chunk{}, chunkResult{}, err
	}
	req := pipeline.ChunkRequest{Schema: t.Schema, Table: t.Table, After: pos, Limit: run.ChunkRows}
	chunk, err := s.deps.Chunks.ReadChunk(ctx, req)
	if err != nil {
		return pipeline.Chunk{}, chunkResult{}, fmt.Errorf("snapshot: read chunk %d of %s: %w", c, t.Name, err)
	}
	s.setRows(c, chunk.Rows)
	high := low
	high.Mark = watermark.MarkHigh
	high.Snapshot = watermark.Snapshot{Xmin: chunk.Snapshot.XMin, Xmax: chunk.Snapshot.XMax, XIP: chunk.Snapshot.XIP}
	high.CommittedXIP = chunk.CommittedXIP
	if err = s.emit(ctx, &high); err != nil {
		return pipeline.Chunk{}, chunkResult{}, err
	}
	res, err := s.await(ctx, c)
	return chunk, res, err
}

func (s *Snapshotter) emit(ctx context.Context, p *watermark.Payload) error {
	content, err := p.MarshalJSON()
	if err != nil {
		return fmt.Errorf("snapshot: %s watermark of chunk %d: %w", p.Mark, p.Chunk, err)
	}
	if _, err := s.deps.Emitter.EmitMessage(ctx, watermark.Prefix, content); err != nil {
		return fmt.Errorf("snapshot: emit %s watermark of chunk %d: %w", p.Mark, p.Chunk, err)
	}
	return nil
}

// await waits for the stream's result of chunk c; results of other
// (abandoned) chunks are discarded.
func (s *Snapshotter) await(ctx context.Context, c uint64) (chunkResult, error) {
	for {
		s.mu.Lock()
		for i, r := range s.results {
			if r.chunk == c {
				s.results = s.results[i+1:]
				s.mu.Unlock()
				return r, nil
			}
		}
		s.results = nil
		s.mu.Unlock()
		select {
		case <-s.resultSignal:
		case <-ctx.Done():
			return chunkResult{}, fmt.Errorf("snapshot: wait for high watermark of chunk %d: %w", c, ctx.Err())
		}
	}
}

// pushResultLocked delivers a chunk result to the snapshot goroutine.
func (s *Snapshotter) pushResultLocked(r chunkResult) {
	if len(s.results) >= maxResults {
		s.results = s.results[1:]
	}
	s.results = append(s.results, r)
	select {
	case s.resultSignal <- struct{}{}:
	default:
	}
}

// install makes t the table the observer feeds to a new merger, at a
// transaction boundary of the stream: a merger must see every transaction
// whose Begin it is fed after its creation, which must precede the first
// LOW (watermark.Merger).
func (s *Snapshotter) install(ctx context.Context, run *pipeline.SnapshotRun, t *Table) error {
	a := &activeTable{run: run.ID, chunkRows: run.ChunkRows, table: *t, merger: watermark.NewMerger()}
	s.mu.Lock()
	if !s.inTx {
		s.active = a
		s.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	s.pendingInstall, s.installed = a, ch
	s.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("snapshot: wait for a transaction boundary: %w", ctx.Err())
	}
}

func (s *Snapshotter) uninstall() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active, s.pendingInstall, s.installed, s.held = nil, nil, nil, nil
}

// setRows hands the chunk's keys to the merger before HIGH is emitted.
func (s *Snapshotter) setRows(c uint64, rows []pipeline.ChunkRow) {
	keys := make([][]byte, len(rows))
	for i := range rows {
		keys[i] = change.AppendCanonicalKey(nil, rows[i].Key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		return
	}
	s.active.merger.Rows(c, keys)
	s.active.rowsChunk, s.active.hasRows, s.active.rows = c, true, rows
}
