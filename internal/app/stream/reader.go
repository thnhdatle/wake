// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

// Defaults of the reader configuration.
const (
	// DefaultProtoVersion is the pgoutput protocol version.
	DefaultProtoVersion = 1
	// DefaultMaxTxBytes is the default stream.max_tx_bytes.
	DefaultMaxTxBytes = 256 << 20
	// maxSchemaQueue bounds the schema states waiting for their transaction
	// to be confirmed; beyond it the newest entry is replaced (coalesced).
	maxSchemaQueue = 16
)

// Gate holds stream transactions back while the initial snapshot runs.
// The initial snapshot implements it.
type Gate interface {
	// Released returns a channel closed once stream transactions may be
	// dispatched.
	Released() <-chan struct{}
}

// KeyColumnSource returns a table's primary key columns in key order (adapter
// pgsql). The reader asks it for REPLICA IDENTITY FULL tables, whose Relation
// message flags every column (the key rule).
type KeyColumnSource interface {
	KeyColumns(ctx context.Context, schema, table string) ([]string, error)
}

// ActivityRecorder is told about every transaction with captured changes;
// lag.Heartbeat implements it.
type ActivityRecorder interface {
	Touch(at time.Time)
}

// SchemaStore persists schema versions in the state file.
type SchemaStore interface {
	SaveSchemas(ctx context.Context, state schema.RegistryState) error
}

// ReaderMetrics receives the reader's counters. Implementations must not
// block.
type ReaderMetrics interface {
	// UnknownType counts a column type outside the type mapping, once per
	// type.
	UnknownType(typeName string)
	// UnknownMessage counts a pgoutput message type outside protocol
	// version 1, once per type; such messages are skipped.
	UnknownMessage(msgType byte)
	// TruncateSkipped counts a TRUNCATE not emitted (events.emit_truncate
	// off).
	TruncateSkipped(table string)
	// TableSkipped counts a published table that is not captured, once per
	// table and reason.
	TableSkipped(table string, reason pipeline.SkipReason)
	// Spilled counts a transaction moved to a spill file.
	Spilled(bytes int64)
}

// ReaderConfig is the reader's part of the configuration.
type ReaderConfig struct {
	// Slot is the replication slot (wake_<source.name>).
	Slot string
	// Publications are passed as publication_names.
	Publications []string
	// ProtoVersion is the pgoutput proto_version; zero means
	// DefaultProtoVersion.
	ProtoVersion int
	// Start is where streaming starts: the slot's confirmed LSN (or the
	// consistent point after slot creation). Transactions ending at or
	// before it were already confirmed and are skipped.
	Start lsn.LSN
	// MaxTxBytes is stream.max_tx_bytes; zero means DefaultMaxTxBytes.
	MaxTxBytes int64
	// EmitTruncate is events.emit_truncate; EmitMessages events.emit_messages.
	EmitTruncate bool
	EmitMessages bool
}

// ReaderDeps are the reader's required dependencies.
type ReaderDeps struct {
	Source pipeline.ReplicationSource
	// Router routes records and holds the ack tracker.
	Router *Router
	// Status receives received positions and reply requests.
	Status  *StatusUpdater
	Schemas *Schemas
	Tables  pipeline.TableResolver
	Spill   pipeline.SpillStore
	Clock   clock.Clock
}

// ReaderOption configures a Reader.
type ReaderOption func(*Reader)

// WithObserver sets the transaction observer (the snapshotter).
func WithObserver(o pipeline.TxObserver) ReaderOption { return func(r *Reader) { r.observer = o } }

// WithGate holds transactions back until the gate is released.
func WithGate(g Gate) ReaderOption { return func(r *Reader) { r.gate = g } }

// WithKeyColumns sets the primary key lookup for REPLICA IDENTITY FULL
// tables; without it their key is every column.
func WithKeyColumns(k KeyColumnSource) ReaderOption { return func(r *Reader) { r.keys = k } }

// WithActivity sets the recorder of transactions with captured changes.
func WithActivity(a ActivityRecorder) ReaderOption { return func(r *Reader) { r.activity = a } }

// WithSchemaStore persists schema versions once the transaction that
// changed them is confirmed.
func WithSchemaStore(s SchemaStore) ReaderOption { return func(r *Reader) { r.schemaStore = s } }

// WithReaderMetrics sets the metrics receiver.
func WithReaderMetrics(m ReaderMetrics) ReaderOption { return func(r *Reader) { r.metrics = m } }

// WithReaderLogger sets the logger (default: discard).
func WithReaderLogger(l *slog.Logger) ReaderOption { return func(r *Reader) { r.logger = l } }

// Reader is the streaming use case: it starts replication,
// decodes pgoutput messages, assembles transactions into records with event
// ids, spills transactions larger than stream.max_tx_bytes, and dispatches
// each transaction at Commit in WAL order: registered with the ack tracker,
// routed to the sinks and sealed (or handed to the observer when it asks to
// hold the transaction open).
//
// Receive runs on a helper goroutine that fetches one message at a time on
// request, so the reader can wait for the gate while reading is paused; the
// message stays valid until the next request. While the gate is closed,
// assembled transactions are buffered; once their size reaches
// stream.max_tx_bytes reading pauses (standby status updates continue from
// the StatusUpdater). A Reader runs once.
type Reader struct {
	cfg         ReaderConfig
	src         pipeline.ReplicationSource
	router      *Router
	tracker     *AckTracker
	status      *StatusUpdater
	schemas     *Schemas
	tables      pipeline.TableResolver
	spill       pipeline.SpillStore
	clock       clock.Clock
	observer    pipeline.TxObserver
	gate        Gate
	keys        KeyColumnSource
	activity    ActivityRecorder
	schemaStore SchemaStore
	metrics     ReaderMetrics
	logger      *slog.Logger

	// Fields below are owned by the Run goroutine.
	dec           *pgoutput.Decoder
	rels          map[uint32]*relEntry
	pendingSchema map[uint32]schema.Observation
	schemaDirty   bool
	schemaQueue   []schemaPersist
	skipLogged    map[string]pipeline.SkipReason
	warnedTypes   map[uint32]bool
	warnedMsgs    map[byte]bool
	tx            txState
	released      bool
	pending       []*assembledTx
	pendingBytes  int64
}

// relEntry is the reader's view of a cached relation.
type relEntry struct {
	name     string // Postgres "<schema>.<table>"
	decision pipeline.TableDecision
	keyCols  []string
}

type schemaPersist struct {
	end   lsn.LSN
	state schema.RegistryState
}

// NewReader validates the dependencies and configuration.
func NewReader(deps ReaderDeps, cfg ReaderConfig, opts ...ReaderOption) (*Reader, error) {
	if err := validateReader(&deps, &cfg); err != nil {
		return nil, err
	}
	r := &Reader{
		cfg: cfg, src: deps.Source, router: deps.Router, tracker: deps.Router.tracker, status: deps.Status,
		schemas: deps.Schemas, tables: deps.Tables, spill: deps.Spill, clock: deps.Clock,
		metrics: nopReaderMetrics{}, logger: slog.New(slog.DiscardHandler),
		dec: pgoutput.NewDecoder(), rels: map[uint32]*relEntry{}, pendingSchema: map[uint32]schema.Observation{},
		skipLogged: map[string]pipeline.SkipReason{}, warnedTypes: map[uint32]bool{}, warnedMsgs: map[byte]bool{},
	}
	for _, o := range opts {
		o(r)
	}
	if r.logger == nil || r.metrics == nil {
		return nil, fmt.Errorf("%w: nil logger or metrics", ErrInvalidOption)
	}
	r.tx.buf = txBuffer{maxBytes: cfg.MaxTxBytes, store: deps.Spill}
	return r, nil
}

// validateReader checks the dependencies and applies configuration
// defaults.
func validateReader(deps *ReaderDeps, cfg *ReaderConfig) error {
	if deps.Source == nil || deps.Router == nil || deps.Status == nil || deps.Schemas == nil ||
		deps.Tables == nil || deps.Spill == nil || deps.Clock == nil {
		return fmt.Errorf("%w: nil reader dependency", ErrInvalidOption)
	}
	if cfg.Slot == "" || len(cfg.Publications) == 0 {
		return fmt.Errorf("%w: slot and publications are required", ErrInvalidOption)
	}
	if cfg.ProtoVersion == 0 {
		cfg.ProtoVersion = DefaultProtoVersion
	}
	if cfg.MaxTxBytes == 0 {
		cfg.MaxTxBytes = DefaultMaxTxBytes
	}
	if cfg.MaxTxBytes < 0 {
		return fmt.Errorf("%w: max tx bytes %d", ErrInvalidOption, cfg.MaxTxBytes)
	}
	return nil
}

// received is one Receive result handed from the receive goroutine.
type received struct {
	msg pipeline.ReplicationMessage
	err error
}

// Run deletes spill files of a crashed run, starts replication at the
// configured start LSN and processes the stream until ctx ends (it then
// returns nil) or an error occurs.
func (r *Reader) Run(ctx context.Context) error {
	if err := r.spill.RemoveAll(ctx); err != nil {
		return unlessDone(ctx, fmt.Errorf("stream: remove spill files of a previous run: %w", err))
	}
	opts := pipeline.StartOptions{
		Slot: r.cfg.Slot, Start: r.cfg.Start, ProtoVersion: r.cfg.ProtoVersion,
		Publications: r.cfg.Publications, Messages: true,
	}
	if err := r.src.StartReplication(ctx, opts); err != nil {
		return unlessDone(ctx, fmt.Errorf("stream: start replication at %s: %w", r.cfg.Start, err))
	}
	rctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	next := make(chan struct{})
	recv := make(chan received)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.receive(rctx, next, recv)
	}()
	err := r.loop(rctx, next, recv)
	cancel()
	wg.Wait()
	r.cleanup(ctx)
	return unlessDone(ctx, err)
}

// unlessDone returns err unless ctx ended, which is a normal shutdown.
func unlessDone(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		return err
	}
}

// receive fetches one message per request on next until ctx ends or
// Receive fails.
func (r *Reader) receive(ctx context.Context, next <-chan struct{}, out chan<- received) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-next:
		}
		msg, err := r.src.Receive(ctx)
		select {
		case out <- received{msg: msg, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (r *Reader) loop(ctx context.Context, next chan<- struct{}, recv <-chan received) error {
	requested := false
	for {
		if !requested && !r.paused() {
			select {
			case next <- struct{}{}:
				requested = true
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		var in <-chan received
		if requested {
			in = recv
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.gateChan():
			if err := r.release(ctx); err != nil {
				return err
			}
		case m := <-in:
			requested = false
			if err := r.step(ctx, &m); err != nil {
				return err
			}
		}
	}
}

// step processes one received message.
func (r *Reader) step(ctx context.Context, m *received) error {
	if m.err != nil {
		return fmt.Errorf("stream: receive: %w", m.err)
	}
	if err := r.handle(ctx, &m.msg); err != nil {
		return err
	}
	return r.persistSchemas(ctx)
}

// paused reports whether reading waits for the gate: transactions held
// back reached stream.max_tx_bytes.
func (r *Reader) paused() bool {
	return !r.released && r.gate != nil && r.pendingBytes >= r.cfg.MaxTxBytes
}

// gateChan returns the gate's channel while transactions are held back,
// else nil.
func (r *Reader) gateChan() <-chan struct{} {
	if r.released || r.gate == nil {
		return nil
	}
	return r.gate.Released()
}

// isReleased reports whether transactions may be dispatched, dispatching
// the held-back ones when the gate just opened.
func (r *Reader) isReleased(ctx context.Context) (bool, error) {
	ch := r.gateChan()
	if ch == nil {
		return true, nil
	}
	select {
	case <-ch:
		return true, r.release(ctx)
	default:
		return false, nil
	}
}

// release dispatches the held-back transactions in order.
func (r *Reader) release(ctx context.Context) error {
	r.released = true
	r.logger.LogAttrs(ctx, slog.LevelInfo, "stream released",
		slog.String("component", "stream"), slog.String("operation", "release"),
		slog.Int("count", len(r.pending)), slog.Int64("bytes", r.pendingBytes))
	for len(r.pending) > 0 {
		tx := r.pending[0]
		r.pending[0] = nil
		r.pending = r.pending[1:]
		r.pendingBytes -= tx.bytes
		if err := r.dispatch(ctx, tx); err != nil {
			return err
		}
	}
	r.pending, r.pendingBytes = nil, 0
	return nil
}

// complete dispatches an assembled transaction or holds it back.
func (r *Reader) complete(ctx context.Context, tx *assembledTx) error {
	ok, err := r.isReleased(ctx)
	if err != nil {
		return err
	}
	if ok {
		return r.dispatch(ctx, tx)
	}
	r.pending = append(r.pending, tx)
	r.pendingBytes += tx.bytes
	return nil
}

// dispatch registers the transaction, routes its records and seals it or
// hands it to the observer.
func (r *Reader) dispatch(ctx context.Context, tx *assembledTx) (err error) {
	defer func() {
		if rerr := tx.release(); err == nil {
			err = rerr
		}
	}()
	h, err := r.tracker.Begin(ctx, tx.commit, tx.end, tx.readAt)
	if err != nil {
		return err
	}
	if err = tx.route(ctx, r.router, h); err != nil {
		return err
	}
	if tx.hold && r.observer != nil {
		if err := r.observer.ObserveHeld(ctx, &heldTx{router: r.router, tx: h}); err != nil {
			return fmt.Errorf("stream: hand over held transaction %s: %w", tx.commit, err)
		}
		return nil
	}
	return r.tracker.Seal(h)
}

// queueSchemas remembers the registry state to persist once the
// transaction ending at end is confirmed.
func (r *Reader) queueSchemas(end lsn.LSN) {
	if r.schemaStore == nil {
		return
	}
	p := schemaPersist{end: end, state: r.schemas.State()}
	if n := len(r.schemaQueue); n >= maxSchemaQueue {
		r.schemaQueue[n-1] = p
		return
	}
	r.schemaQueue = append(r.schemaQueue, p)
}

// persistSchemas saves the newest queued registry state whose transaction
// is confirmed. Persisting only confirmed states keeps a restart from
// skipping a schema-change event that was never acknowledged: the replayed
// Relation message then differs from the stored shape again.
func (r *Reader) persistSchemas(ctx context.Context) error {
	if len(r.schemaQueue) == 0 {
		return nil
	}
	confirmed := r.tracker.Confirmed()
	n := 0
	for n < len(r.schemaQueue) && r.schemaQueue[n].end <= confirmed {
		n++
	}
	if n == 0 {
		return nil
	}
	if err := r.schemaStore.SaveSchemas(ctx, r.schemaQueue[n-1].state); err != nil {
		return fmt.Errorf("stream: persist schema versions: %w", err)
	}
	clear(r.schemaQueue[:n])
	r.schemaQueue = r.schemaQueue[n:]
	return nil
}

// cleanup removes the spill files of transactions not dispatched.
func (r *Reader) cleanup(ctx context.Context) {
	errs := make([]error, 0, 1+len(r.pending))
	errs = append(errs, r.tx.buf.discard())
	for _, tx := range r.pending {
		errs = append(errs, tx.release())
	}
	r.pending, r.pendingBytes = nil, 0
	if err := errors.Join(errs...); err != nil {
		r.logger.LogAttrs(ctx, slog.LevelWarn, "spill files left behind; removed at next start",
			slog.String("component", "stream"), slog.String("operation", "cleanup"), slog.Any("error", err))
	}
}

type nopReaderMetrics struct{}

func (nopReaderMetrics) UnknownType(string)                       {}
func (nopReaderMetrics) UnknownMessage(byte)                      {}
func (nopReaderMetrics) TruncateSkipped(string)                   {}
func (nopReaderMetrics) TableSkipped(string, pipeline.SkipReason) {}
func (nopReaderMetrics) Spilled(int64)                            {}
