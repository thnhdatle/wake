// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Session reads chunks inside one transaction at an imported snapshot
// (pipeline.ChunkSession). It is safe for concurrent use; reads are
// serialized.
type Session struct {
	r *ChunkReader

	mu     sync.Mutex // protects every field below
	conn   *pgx.Conn
	tx     pgx.Tx
	closed bool
}

// ReadChunk implements pipeline.ChunkSession; Chunk.Snapshot and
// CommittedXIP stay empty.
func (s *Session) ReadChunk(ctx context.Context, req pipeline.ChunkRequest) (pipeline.Chunk, error) {
	if err := validateRequest(&req); err != nil {
		return pipeline.Chunk{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return pipeline.Chunk{}, ErrSessionClosed
	}
	ctx, cancel := context.WithTimeout(ctx, s.r.o.chunkTimeout)
	defer cancel()
	return readChunk(ctx, s.tx, &req)
}

// Close implements pipeline.ChunkSession: it ends the transaction and
// returns the connection to the pool. A second Close is ErrSessionClosed.
func (s *Session) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSessionClosed
	}
	s.closed = true
	ctx, cancel := context.WithTimeout(ctx, s.r.o.queryTimeout)
	defer cancel()
	err := s.tx.Rollback(ctx)
	s.r.pool.release(ctx, s.conn)
	if err != nil {
		return fmt.Errorf("pgsql: end snapshot transaction: %w", err)
	}
	return nil
}

// Compile-time check of the port Session implements.
var _ pipeline.ChunkSession = (*Session)(nil)
