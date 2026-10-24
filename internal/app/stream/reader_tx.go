// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pgoutput"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

type txState struct {
	open     bool
	skip     bool
	begin    pgoutput.Begin
	index    uint64
	msgN     uint64
	captured bool
	buf      txBuffer
}

func (r *Reader) handle(ctx context.Context, m *pipeline.ReplicationMessage) error {
	switch m.Kind {
	case pipeline.ReplicationMessageKeepalive:
		r.status.ObserveReceived(m.Keepalive.ServerWALEnd)
		if m.Keepalive.ReplyRequested {
			r.status.RequestReply()
		}
		return nil
	case pipeline.ReplicationMessageXLogData:
		r.status.ObserveReceived(m.XLogData.WALStart)
		return r.handleXLogData(ctx, &m.XLogData)
	default:
		return fmt.Errorf("%w: replication message kind %d", ErrProtocol, m.Kind)
	}
}

func (r *Reader) handleXLogData(ctx context.Context, x *pipeline.XLogData) error {
	msg, err := r.dec.Decode(x.Data)
	if err != nil {
		var unsupported *pgoutput.UnsupportedMessageError
		if errors.As(err, &unsupported) && !errors.Is(err, pgoutput.ErrStreaming) {
			r.warnUnknownMessage(ctx, unsupported.Type)
			return nil
		}
		return fmt.Errorf("stream: decode message at %s: %w", x.WALStart, err)
	}
	switch m := msg.(type) {
	case *pgoutput.Begin:
		return r.begin(m)
	case *pgoutput.Commit:
		return r.commit(ctx, m)
	case *pgoutput.Relation:
		return r.relation(ctx, m)
	case *pgoutput.Insert:
		return r.row(ctx, m, m.RelationOID)
	case *pgoutput.Update:
		return r.row(ctx, m, m.RelationOID)
	case *pgoutput.Delete:
		return r.row(ctx, m, m.RelationOID)
	case *pgoutput.Truncate:
		return r.truncate(ctx, m)
	case *pgoutput.LogicalMessage:
		return r.message(ctx, m, x.ServerTime)
	default:
		return nil
	}
}

func (r *Reader) begin(m *pgoutput.Begin) error {
	if r.tx.open {
		return fmt.Errorf("%w: begin of %s inside %s", ErrProtocol, m.FinalLSN, r.tx.begin.FinalLSN)
	}
	skip := m.FinalLSN < r.cfg.Start
	r.tx = txState{open: true, skip: skip, begin: *m, buf: r.tx.buf}
	r.tx.buf.reset(m.FinalLSN, m.XID)
	if !skip && r.observer != nil {
		r.observer.ObserveBegin(pipeline.ObservedTx{XID: m.XID, CommitLSN: m.FinalLSN, TsMs: m.TsMs})
	}
	return nil
}

func (r *Reader) commit(ctx context.Context, m *pgoutput.Commit) error {
	if !r.tx.open || m.CommitLSN != r.tx.begin.FinalLSN {
		return fmt.Errorf("%w: commit %s without matching begin", ErrProtocol, m.CommitLSN)
	}
	r.tx.open = false
	if r.schemaDirty {
		r.schemaDirty = false
		r.queueSchemas(m.EndLSN)
	}
	if r.tx.skip {
		return r.tx.buf.discard()
	}
	hold := false
	if r.observer != nil {
		var err error
		if hold, err = r.observer.ObserveCommit(ctx); err != nil {
			return fmt.Errorf("stream: observe commit %s: %w", m.CommitLSN, err)
		}
	}
	now := r.clock.Now()
	tx := &assembledTx{commit: m.CommitLSN, end: m.EndLSN, readAt: now, hold: hold}
	r.tx.buf.take(tx)
	if r.tx.captured && r.activity != nil {
		r.activity.Touch(now)
	}
	return r.complete(ctx, tx)
}

func (r *Reader) relation(ctx context.Context, m *pgoutput.Relation) error {
	rel := &m.Relation
	name := rel.QualifiedName()
	info := pipeline.TableInfo{Schema: rel.Namespace, Table: rel.Name, OID: rel.OID, HasKey: !rel.IsKeyless()}
	d := r.tables.Resolve(info)
	e := &relEntry{name: name, decision: d}
	r.rels[rel.OID] = e
	if !d.Capture {
		delete(r.pendingSchema, rel.OID)
		r.logSkip(ctx, name, d.Skip)
		return nil
	}
	delete(r.skipLogged, name)
	r.warnUnknownTypes(ctx, rel)
	if err := r.overrideKey(ctx, rel); err != nil {
		return fmt.Errorf("stream: key columns of %s: %w", name, err)
	}
	e.keyCols, _ = r.dec.KeyColumns(rel.OID)
	obs, first := r.schemas.Observe(rel)
	r.schemaDirty = r.schemaDirty || obs.Changed || first
	if obs.Changed && r.tx.open && !r.tx.skip {
		r.pendingSchema[rel.OID] = obs
	} else {
		delete(r.pendingSchema, rel.OID)
	}
	return nil
}

func (r *Reader) overrideKey(ctx context.Context, rel *schema.Relation) error {
	if rel.Identity != schema.IdentityFull || r.keys == nil {
		return nil
	}
	keys, err := r.keys.KeyColumns(ctx, rel.Namespace, rel.Name)
	if err != nil || len(keys) == 0 {
		return err
	}
	return r.dec.SetKeyColumns(rel.OID, keys)
}

func (r *Reader) capturedRelation(oid uint32) (e *relEntry, emit bool, err error) {
	if !r.tx.open {
		return nil, false, fmt.Errorf("%w: change of relation %d outside a transaction", ErrProtocol, oid)
	}
	e, ok := r.rels[oid]
	if !ok {
		return nil, false, fmt.Errorf("stream: %w: oid %d", pgoutput.ErrUnknownRelation, oid)
	}
	return e, !r.tx.skip && e.decision.Capture, nil
}

func (r *Reader) row(ctx context.Context, m pgoutput.Message, oid uint32) error {
	e, emit, err := r.capturedRelation(oid)
	if !emit {
		return err
	}
	img, err := r.dec.RowImages(m)
	if err != nil {
		return fmt.Errorf("stream: row of %s in %s: %w", e.name, r.tx.begin.FinalLSN, err)
	}
	if err := r.emitPendingSchema(ctx, oid, e); err != nil {
		return err
	}
	ev := change.Event{
		ID: change.StreamID(r.tx.begin.FinalLSN, r.tx.index), Source: e.decision.Source, Op: img.Op,
		TsMs: r.tx.begin.TsMs, TxID: r.tx.begin.XID, Key: img.Key, Before: img.Before, After: img.After,
		Unchanged: img.Unchanged, SchemaVersion: r.schemas.Version(e.name),
	}
	rec := pipeline.NewEventRecord(&ev)
	if err := r.buffer(ctx, &rec); err != nil {
		return err
	}
	if r.observer != nil && ev.Key != nil {
		r.observer.ObserveChange(e.name, rec.Key, beforeKey(m, e, &img, rec.Key))
	}
	return nil
}

func beforeKey(m pgoutput.Message, e *relEntry, img *pgoutput.RowImages, key []byte) []byte {
	u, ok := m.(*pgoutput.Update)
	if !ok || u.OldKind == pgoutput.OldNone || len(e.keyCols) == 0 {
		return nil
	}
	row := make(change.Row, 0, len(e.keyCols))
	for _, c := range e.keyCols {
		v, ok := img.Before.Lookup(c)
		if !ok {
			return nil
		}
		row = append(row, change.Column{Name: c, Value: v})
	}
	before := change.AppendCanonicalKey(nil, row)
	if bytes.Equal(before, key) {
		return nil
	}
	return before
}

func (r *Reader) buffer(ctx context.Context, rec *pipeline.Record) error {
	r.tx.index++
	r.tx.captured = true
	return r.add(ctx, rec)
}

func (r *Reader) add(ctx context.Context, rec *pipeline.Record) error {
	started, err := r.tx.buf.add(ctx, rec)
	if err != nil {
		return err
	}
	if started {
		spilled := r.tx.buf.file.Size()
		r.metrics.Spilled(spilled)
		r.logger.LogAttrs(ctx, slog.LevelInfo, "transaction exceeds stream.max_tx_bytes, spilling to disk",
			slog.String("component", "stream"), slog.String("operation", "spill"),
			slog.String("lsn", r.tx.begin.FinalLSN.String()), slog.Int64("bytes", spilled))
	}
	return nil
}

func (r *Reader) emitPendingSchema(ctx context.Context, oid uint32, e *relEntry) error {
	obs, ok := r.pendingSchema[oid]
	if !ok {
		return nil
	}
	delete(r.pendingSchema, oid)
	sc := change.SchemaChange{
		ID: change.SchemaID(r.tx.begin.FinalLSN, r.tx.index), Source: e.decision.Source, TsMs: r.tx.begin.TsMs,
		SchemaVersion: obs.Version, ColumnsBefore: obs.Before, ColumnsAfter: obs.After,
	}
	rec := pipeline.NewSchemaChangeRecord(&sc)
	return r.add(ctx, &rec)
}

func (r *Reader) truncate(ctx context.Context, m *pgoutput.Truncate) error {
	for _, oid := range m.RelationOIDs {
		e, emit, err := r.capturedRelation(oid)
		if err != nil {
			return err
		}
		if !emit {
			continue
		}
		if r.observer != nil {
			r.observer.ObserveTruncate(e.name)
		}
		if !r.cfg.EmitTruncate {
			r.tx.captured = true
			r.metrics.TruncateSkipped(e.name)
			r.logger.LogAttrs(ctx, slog.LevelWarn, "truncate not emitted (events.emit_truncate is off)",
				slog.String("component", "stream"), slog.String("operation", "truncate"),
				slog.String("table", e.name), slog.String("lsn", r.tx.begin.FinalLSN.String()))
			continue
		}
		if err := r.emitPendingSchema(ctx, oid, e); err != nil {
			return err
		}
		ev := change.Event{
			ID: change.StreamID(r.tx.begin.FinalLSN, r.tx.index), Source: e.decision.Source, Op: change.OpTruncate,
			TsMs: r.tx.begin.TsMs, TxID: r.tx.begin.XID, SchemaVersion: r.schemas.Version(e.name),
		}
		rec := pipeline.NewEventRecord(&ev)
		if err := r.buffer(ctx, &rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) message(ctx context.Context, m *pgoutput.LogicalMessage, serverTime time.Time) error {
	if m.Transactional() != r.tx.open {
		return fmt.Errorf("%w: message at %s (transactional %t) with transaction open %t",
			ErrProtocol, m.LSN, m.Transactional(), r.tx.open)
	}
	switch {
	case m.IsInternal():
		r.internalMessage(m)
		return nil
	case !r.cfg.EmitMessages:
		return nil
	case m.Transactional():
		if r.tx.skip {
			return nil
		}
		msg := change.Message{
			ID: change.MessageID(r.tx.begin.FinalLSN, r.tx.msgN), TsMs: r.tx.begin.TsMs, Transactional: true,
			Prefix: m.Prefix, Content: m.Content,
		}
		r.tx.msgN++
		rec := pipeline.NewMessageRecord(&msg)
		return r.add(ctx, &rec)
	case m.LSN < r.cfg.Start:
		return nil
	default:
		return r.complete(ctx, r.messageTx(m, serverTime))
	}
}

func (r *Reader) internalMessage(m *pgoutput.LogicalMessage) {
	if !m.Transactional() || r.tx.skip || r.observer == nil {
		return
	}
	if m.Prefix == pgoutput.PrefixWatermark || m.Prefix == pgoutput.PrefixSnapshot {
		r.observer.ObserveMessage(m.Prefix, m.Content)
	}
}

func (r *Reader) messageTx(m *pgoutput.LogicalMessage, serverTime time.Time) *assembledTx {
	var tsMs int64
	if !serverTime.IsZero() {
		tsMs = serverTime.UnixMilli()
	}
	msg := change.Message{ID: change.MessageID(m.LSN, 0), TsMs: tsMs, Prefix: m.Prefix, Content: m.Content}
	rec := pipeline.NewMessageRecord(&msg)
	return &assembledTx{
		commit: m.LSN, end: m.LSN, readAt: r.clock.Now(), records: []pipeline.Record{rec}, bytes: recordSize(&rec),
	}
}

func (r *Reader) logSkip(ctx context.Context, table string, reason pipeline.SkipReason) {
	if r.skipLogged[table] == reason {
		return
	}
	r.skipLogged[table] = reason
	r.metrics.TableSkipped(table, reason)
	level, msg := slog.LevelError, "table skipped: name is not a valid event identifier; map it in tables.name_map"
	switch reason {
	case pipeline.SkipKeyless:
		msg = "table skipped: keyless table (set tables.emit_unkeyed to emit it)"
	case pipeline.SkipNotCaptured, pipeline.SkipNone:
		level, msg = slog.LevelInfo, "table skipped: not captured by tables.include/exclude"
	case pipeline.SkipIdentifierInvalid:
	}
	r.logger.LogAttrs(ctx, level, msg, slog.String("component", "stream"), slog.String("operation", "relation"),
		slog.String("table", table), slog.String("error", string(reason)))
}

func (r *Reader) warnUnknownTypes(ctx context.Context, rel *schema.Relation) {
	for _, oid := range rel.UnknownTypes() {
		if r.warnedTypes[oid] {
			continue
		}
		r.warnedTypes[oid] = true
		name := schema.FallbackTypeName(oid)
		for i := range rel.Columns {
			if rel.Columns[i].TypeOID == oid {
				name = rel.Columns[i].TypeName
				break
			}
		}
		r.metrics.UnknownType(name)
		r.logger.LogAttrs(ctx, slog.LevelWarn, "column type outside the type mapping, emitted as text",
			slog.String("component", "stream"), slog.String("operation", "relation"),
			slog.String("table", rel.QualifiedName()), slog.String("key", name))
	}
}

func (r *Reader) warnUnknownMessage(ctx context.Context, t byte) {
	if r.warnedMsgs[t] {
		return
	}
	r.warnedMsgs[t] = true
	r.metrics.UnknownMessage(t)
	r.logger.LogAttrs(ctx, slog.LevelWarn, "unsupported pgoutput message type skipped",
		slog.String("component", "stream"), slog.String("operation", "decode"),
		slog.String("key", strconv.QuoteRune(rune(t))))
}
