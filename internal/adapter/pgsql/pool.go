// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Pool defaults.
const (
	// DefaultMaxConns is the pool size when WithMaxConns is not given: the
	// default snapshot.parallel_tables (4) exported-snapshot sessions plus
	// the catalog, chunk reader, emitter and lag monitor.
	DefaultMaxConns = 8
	// DefaultMaxIdle is how long an unused connection is kept.
	DefaultMaxIdle = 5 * time.Minute
	// DefaultConnectTimeout bounds opening one connection.
	DefaultConnectTimeout = 30 * time.Second
)

// ErrPoolClosed reports Acquire after Close.
var ErrPoolClosed = errors.New("pgsql: pool closed")

// sessionParams are the session settings schema.AppendJSON relies on; they
// match the replication connection's so that `r` and stream events render
// values identically. Options cannot override them.
var sessionParams = map[string]string{
	"DateStyle":          "ISO",
	"IntervalStyle":      "postgres",
	"client_encoding":    "UTF8",
	"TimeZone":           "UTC",
	"extra_float_digits": "1",
}

// PoolOption configures a Pool.
type PoolOption func(*Pool)

// WithMaxConns sets the pool size (default DefaultMaxConns). It must exceed
// snapshot.parallel_tables: every exported-snapshot session holds a
// connection for the whole initial snapshot.
func WithMaxConns(n int) PoolOption { return func(p *Pool) { p.maxConns = n } }

// WithMaxIdle sets how long an unused connection is kept (default
// DefaultMaxIdle).
func WithMaxIdle(d time.Duration) PoolOption { return func(p *Pool) { p.maxIdle = d } }

// WithConnectTimeout bounds opening one connection (default
// DefaultConnectTimeout).
func WithConnectTimeout(d time.Duration) PoolOption { return func(p *Pool) { p.connectTimeout = d } }

// WithApplicationName sets application_name.
func WithApplicationName(name string) PoolOption {
	return func(p *Pool) { p.cfg.RuntimeParams["application_name"] = name }
}

// Pool is a bounded pool of SQL connections with Wake's session settings.
// It is safe for concurrent use and starts no goroutines: connections open
// on demand, at most maxConns at a time; Acquire blocks while all are in
// use. (pgxpool is not used: it needs a module outside the approved set.)
type Pool struct {
	cfg            *pgx.ConnConfig
	maxConns       int
	maxIdle        time.Duration
	connectTimeout time.Duration
	// tokens holds one token per checked-out connection; Acquire blocks
	// while it is full.
	tokens chan struct{}
	// idle holds unused open connections, most recently released last.
	idle chan idleConn

	mu     sync.Mutex // protects closed
	closed bool
}

type idleConn struct {
	conn  *pgx.Conn
	since time.Time
}

// NewPool parses the DSN and validates the options. It does not connect.
func NewPool(dsn string, opts ...PoolOption) (*Pool, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: parse dsn: %w", ErrInvalidArgument, err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	p := &Pool{cfg: cfg, maxConns: DefaultMaxConns, maxIdle: DefaultMaxIdle, connectTimeout: DefaultConnectTimeout}
	for _, o := range opts {
		o(p)
	}
	if p.maxConns < 1 || p.maxIdle <= 0 || p.connectTimeout <= 0 {
		return nil, fmt.Errorf("%w: pool size and timeouts must be positive", ErrInvalidArgument)
	}
	maps.Copy(cfg.RuntimeParams, sessionParams)
	p.tokens = make(chan struct{}, p.maxConns)
	p.idle = make(chan idleConn, p.maxConns)
	return p, nil
}

// User returns the role name of the connections.
func (p *Pool) User() string { return p.cfg.User }

// acquire checks out a connection; release must be called exactly once.
func (p *Pool) acquire(ctx context.Context) (*pgx.Conn, error) {
	select {
	case p.tokens <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("pgsql: acquire connection: %w", ctx.Err())
	}
	if p.isClosed() {
		<-p.tokens
		return nil, ErrPoolClosed
	}
	if conn := p.takeIdle(ctx); conn != nil {
		return conn, nil
	}
	cctx, cancel := context.WithTimeout(ctx, p.connectTimeout)
	defer cancel()
	conn, err := pgx.ConnectConfig(cctx, p.cfg)
	if err != nil {
		<-p.tokens
		return nil, fmt.Errorf("pgsql: connect: %w", err)
	}
	return conn, nil
}

// takeIdle returns a usable idle connection, or nil; stale ones are closed.
func (p *Pool) takeIdle(ctx context.Context) *pgx.Conn {
	for {
		select {
		case ic := <-p.idle:
			if ic.conn.IsClosed() || time.Since(ic.since) > p.maxIdle {
				closeConn(ctx, ic.conn)
				continue
			}
			return ic.conn
		default:
			return nil
		}
	}
}

// release returns a connection; a closed connection or one left inside a
// transaction is closed instead of reused.
func (p *Pool) release(ctx context.Context, conn *pgx.Conn) {
	defer func() { <-p.tokens }()
	if p.isClosed() || conn.IsClosed() || conn.PgConn().TxStatus() != 'I' {
		closeConn(ctx, conn)
		return
	}
	select {
	case p.idle <- idleConn{conn: conn, since: time.Now()}:
	default:
		closeConn(ctx, conn)
	}
}

// withConn runs fn on a checked-out connection.
func (p *Pool) withConn(ctx context.Context, fn func(*pgx.Conn) error) error {
	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	defer p.release(ctx, conn)
	return fn(conn)
}

// Ping checks that a connection can be opened and used.
func (p *Pool) Ping(ctx context.Context) error {
	return p.withConn(ctx, func(c *pgx.Conn) error {
		if err := c.Ping(ctx); err != nil {
			return fmt.Errorf("pgsql: ping: %w", err)
		}
		return nil
	})
}

func (p *Pool) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Close closes the idle connections; checked-out connections are closed
// when released. Acquire fails afterwards.
func (p *Pool) Close(ctx context.Context) {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	for {
		select {
		case ic := <-p.idle:
			closeConn(ctx, ic.conn)
		default:
			return
		}
	}
}

// closeConn closes a connection within a short bounded time, also when ctx
// is already done (the connection must be released either way).
func closeConn(ctx context.Context, conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = conn.Close(ctx) //nolint:errcheck // the connection is discarded either way
}
