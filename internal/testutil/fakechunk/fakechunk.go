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

var ErrBadPosition = errors.New("fakechunk: bad position")

var ErrClosed = errors.New("fakechunk: session closed")

type tables map[string]map[int64]string

func (t tables) clone() tables {
	c := make(tables, len(t))
	for name, rows := range t {
		c[name] = maps.Clone(rows)
	}
	return c
}

// Reader is an in-memory chunk reader.
type Reader struct {
	mu        sync.Mutex
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

func (r *Reader) Put(table string, id int64, v string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tables[table] == nil {
		r.tables[table] = map[int64]string{}
	}
	r.tables[table][id] = v
}

func (r *Reader) Delete(table string, id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tables[table], id)
}

func (r *Reader) SetSnapshot(s pipeline.TxSnapshot, committed []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s.XIP = slices.Clone(s.XIP)
	r.snapshot, r.committed = s, slices.Clone(committed)
}

func (r *Reader) OnRead(fn func(n int)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onRead = fn
}

func (r *Reader) FailRead(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readErr = err
}

func (r *Reader) FailOpen(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.openErr = err
}

func (r *Reader) Reads() []pipeline.ChunkRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reads)
}

func (r *Reader) Sessions() (opened []string, closed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.opened), r.closed
}

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

func Row(id int64, v string) pipeline.ChunkRow {
	idJSON := []byte(strconv.FormatInt(id, 10))
	return pipeline.ChunkRow{
		Key:   change.Row{{Name: "id", Value: idJSON}},
		After: change.Row{{Name: "id", Value: idJSON}, {Name: "v", Value: []byte(v)}},
	}
}

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

type Session struct {
	r      *Reader
	tables tables
	closed bool
}

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
