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

const (
	DefaultMaxConns       = 8
	DefaultMaxIdle        = 5 * time.Minute
	DefaultConnectTimeout = 30 * time.Second
)

var ErrPoolClosed = errors.New("pgsql: pool closed")

var sessionParams = map[string]string{
	"DateStyle":          "ISO",
	"IntervalStyle":      "postgres",
	"client_encoding":    "UTF8",
	"TimeZone":           "UTC",
	"extra_float_digits": "1",
}

type PoolOption func(*Pool)

func WithMaxConns(n int) PoolOption { return func(p *Pool) { p.maxConns = n } }

func WithMaxIdle(d time.Duration) PoolOption { return func(p *Pool) { p.maxIdle = d } }

func WithConnectTimeout(d time.Duration) PoolOption { return func(p *Pool) { p.connectTimeout = d } }

func WithApplicationName(name string) PoolOption {
	return func(p *Pool) { p.cfg.RuntimeParams["application_name"] = name }
}

// Pool is a small pool of Postgres connections.
type Pool struct {
	cfg            *pgx.ConnConfig
	maxConns       int
	maxIdle        time.Duration
	connectTimeout time.Duration
	tokens         chan struct{}
	idle           chan idleConn

	mu     sync.Mutex
	closed bool
}

type idleConn struct {
	conn  *pgx.Conn
	since time.Time
}

// NewPool returns a Pool for the given DSN.
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

func (p *Pool) User() string { return p.cfg.User }

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

func (p *Pool) withConn(ctx context.Context, fn func(*pgx.Conn) error) error {
	conn, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	defer p.release(ctx, conn)
	return fn(conn)
}

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

func closeConn(ctx context.Context, conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = conn.Close(ctx) //nolint:errcheck // the connection is discarded either way
}
