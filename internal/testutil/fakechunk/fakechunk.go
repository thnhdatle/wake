// SPDX-License-Identifier: Apache-2.0

package fakechunk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// ErrBadPosition reports a ChunkRequest.After that is not [id].
var ErrBadPosition = errors.New("fakechunk: bad position")

// ErrClosed reports a read on a closed session.
var ErrClosed = errors.New("fakechunk: session closed")

// tables maps "<schema>.<table>" to rows by id; values are JSON.
type tables map[string]map[int64]string

func (t tables) clone() tables {
	c := make(tables, len(t))
	for name, rows := range t {
		c[name] = maps.Clone(rows)
	}
	return c
}

// Reader is the fake. It is safe for concurrent use.
type Reader struct {
	mu        sync.Mutex // protects every field below
	tables    tables
	snapshot  pipeline.TxSnapshot
	committed []uint64
	reads     []pipeline.ChunkRequest
	opened    []string
	closed    int
	readErr   error
	openErr   error
	onRead    func(n int)
}

// New returns a reader without tables.
func New() *Reader { return &Reader{tables: tables{}} }

// Put sets the row id of a table to the JSON value v.
func (r *Reader) Put(table string, id int64, v string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tables[table] == nil {
		r.tables[table] = map[int64]string{}
	}
	r.tables[table][id] = v
}

// Delete removes the row id of a table.
func (r *Reader) Delete(table string, id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tables[table], id)
}

// SetSnapshot sets the snapshot and committed xids ReadChunk reports.
func (r *Reader) SetSnapshot(s pipeline.TxSnapshot, committed []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s.XIP = slices.Clone(s.XIP)
	r.snapshot, r.committed = s, slices.Clone(committed)
}

// OnRead sets a hook called (without the lock) after the n-th ReadChunk
// call (1-based) has read its rows and before it returns.
func (r *Reader) OnRead(fn func(n int)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onRead = fn
}

// FailRead makes ReadChunk and session reads return err; nil restores them.
func (r *Reader) FailRead(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readErr = err
}

// FailOpen makes OpenExported return err; nil restores it.
func (r *Reader) FailOpen(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.openErr = err
}

// Reads returns the requests of every ReadChunk and session read, in order.
func (r *Reader) Reads() []pipeline.ChunkRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reads)
}

// Sessions returns the snapshot names of the opened sessions and the
// number of closed ones.
func (r *Reader) Sessions() (opened []string, closed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.opened), r.closed
}

// ReadChunk implements pipeline.ChunkReader over the current rows.
func (r *Reader) ReadChunk(ctx context.Context, req pipeline.ChunkRequest) (pipeline.Chunk, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.Chunk{}, err
	}
	r.mu.Lock()
	c, n, err := r.readLocked(r.tables, req)
	c.Snapshot = pipeline.TxSnapshot{XMin: r.snapshot.XMin, XMax: r.snapshot.XMax, XIP: slices.Clone(r.snapshot.XIP)}
	c.CommittedXIP = slices.Clone(r.committed)
	hook := r.onRead
	r.mu.Unlock()
	if err != nil {
		return pipeline.Chunk{}, err
	}
	if hook != nil {
		hook(n)
	}
	return c, nil
}

// readLocked reads a chunk from t and returns the read count.
func (r *Reader) readLocked(t tables, req pipeline.ChunkRequest) (pipeline.Chunk, int, error) {
	r.reads = append(r.reads, req)
	n := len(r.reads)
	if r.readErr != nil {
		return pipeline.Chunk{}, n, r.readErr
	}
	after := int64(-1 << 63)
	hasAfter := req.After != nil
	if hasAfter {
		var pos []int64
		if err := json.Unmarshal(req.After, &pos); err != nil || len(pos) != 1 {
			return pipeline.Chunk{}, n, fmt.Errorf("%w: %s", ErrBadPosition, req.After)
		}
		after = pos[0]
	}
	rows := t[req.Schema+"."+req.Table]
	ids := slices.Sorted(maps.Keys(rows))
	var c pipeline.Chunk
	for _, id := range ids {
		if hasAfter && id <= after {
			continue
		}
		if len(c.Rows) == req.Limit {
			return c, n, nil
		}
		c.Rows = append(c.Rows, Row(id, rows[id]))
		c.Last = json.RawMessage("[" + strconv.FormatInt(id, 10) + "]")
	}
	c.Done = true
	if c.Last == nil {
		c.Last = req.After
	}
	return c, n, nil
}

// Row returns the chunk row of id with the JSON value v: key {"id"} and
// after {"id","v"}.
func Row(id int64, v string) pipeline.ChunkRow {
	idJSON := []byte(strconv.FormatInt(id, 10))
	return pipeline.ChunkRow{
		Key:   change.Row{{Name: "id", Value: idJSON}},
		After: change.Row{{Name: "id", Value: idJSON}, {Name: "v", Value: []byte(v)}},
	}
}

// OpenExported implements pipeline.ChunkReader: the session reads the rows
// as of now.
func (r *Reader) OpenExported(ctx context.Context, snapshotName string) (pipeline.ChunkSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.openErr != nil {
		return nil, r.openErr
	}
	r.opened = append(r.opened, snapshotName)
	return &Session{r: r, tables: r.tables.clone()}, nil
}

// Session is a fake session at an imported snapshot.
type Session struct {
	r      *Reader
	tables tables
	closed bool
}

// ReadChunk implements pipeline.ChunkSession.
func (s *Session) ReadChunk(ctx context.Context, req pipeline.ChunkRequest) (pipeline.Chunk, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.Chunk{}, err
	}
	s.r.mu.Lock()
	if s.closed {
		s.r.mu.Unlock()
		return pipeline.Chunk{}, ErrClosed
	}
	c, n, err := s.r.readLocked(s.tables, req)
	hook := s.r.onRead
	s.r.mu.Unlock()
	if err != nil {
		return pipeline.Chunk{}, err
	}
	if hook != nil {
		hook(n)
	}
	return c, nil
}

// Close implements pipeline.ChunkSession.
func (s *Session) Close(_ context.Context) error {
	s.r.mu.Lock()
	defer s.r.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.closed = true
	s.r.closed++
	return nil
}
