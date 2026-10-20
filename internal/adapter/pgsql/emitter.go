// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Emitter writes transactional logical messages (watermarks, heartbeats,
// snapshot requests). It implements pipeline.MessageEmitter and is safe for
// concurrent use.
type Emitter struct {
	pool *Pool
	o    options
}

// NewEmitter validates the dependencies.
func NewEmitter(pool *Pool, opts ...Option) (*Emitter, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrInvalidArgument)
	}
	o, err := newOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Emitter{pool: pool, o: o}, nil
}

// EmitMessage implements pipeline.MessageEmitter: SELECT
// pg_logical_emit_message(true, prefix, content) in its own transaction; it
// returns the message's LSN.
func (e *Emitter) EmitMessage(ctx context.Context, prefix string, content []byte) (lsn.LSN, error) {
	if prefix == "" {
		return 0, fmt.Errorf("%w: empty message prefix", ErrInvalidArgument)
	}
	if content == nil {
		content = []byte{} // the function is strict: NULL content emits nothing
	}
	ctx, cancel := context.WithTimeout(ctx, e.o.queryTimeout)
	defer cancel()
	var text string
	err := e.pool.withConn(ctx, func(conn *pgx.Conn) error {
		err := conn.QueryRow(ctx, `SELECT pg_catalog.pg_logical_emit_message(true, $1::text, $2::bytea)::text`,
			prefix, content).Scan(&text)
		if err != nil {
			return fmt.Errorf("pgsql: emit message %s: %w", prefix, err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	at, err := lsn.Parse(text)
	if err != nil {
		return 0, fmt.Errorf("pgsql: emit message %s: %w", prefix, err)
	}
	return at, nil
}

// Compile-time check of the port Emitter implements.
var _ pipeline.MessageEmitter = (*Emitter)(nil)
