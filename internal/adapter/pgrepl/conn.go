// SPDX-License-Identifier: Apache-2.0

package pgrepl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	DefaultCommandTimeout    = 30 * time.Second
	DefaultCreateSlotTimeout = 10 * time.Minute
	DefaultStatusTimeout     = 10 * time.Second
)

var sessionParams = map[string]string{
	"replication":        "database",
	"DateStyle":          "ISO",
	"IntervalStyle":      "postgres",
	"client_encoding":    "UTF8",
	"TimeZone":           "UTC",
	"extra_float_digits": "1",
}

type connState uint8

const (
	stateIdle connState = iota
	stateExported
	stateStreaming
	stateClosed
)

type Option func(*Conn)

func WithCommandTimeout(d time.Duration) Option { return func(c *Conn) { c.cmdTimeout = d } }

func WithCreateSlotTimeout(d time.Duration) Option { return func(c *Conn) { c.slotTimeout = d } }

func WithStatusTimeout(d time.Duration) Option { return func(c *Conn) { c.statusTimeout = d } }

func WithRuntimeParam(name, value string) Option {
	return func(c *Conn) {
		if c.params == nil {
			c.params = map[string]string{}
		}
		c.params[name] = value
	}
}

func WithLogger(l *slog.Logger) Option { return func(c *Conn) { c.logger = l } }

// Conn is a Postgres replication connection.
type Conn struct {
	cfg           *pgconn.Config
	params        map[string]string
	cmdTimeout    time.Duration
	slotTimeout   time.Duration
	statusTimeout time.Duration
	logger        *slog.Logger

	mu    sync.Mutex
	conn  *pgconn.PgConn
	state connState

	writeMu sync.Mutex
}

// New returns a Conn for the given DSN.
func New(dsn string, opts ...Option) (*Conn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: parse dsn: %w", ErrInvalidArgument, err)
	}
	c := &Conn{
		cfg: cfg, cmdTimeout: DefaultCommandTimeout, slotTimeout: DefaultCreateSlotTimeout,
		statusTimeout: DefaultStatusTimeout,
	}
	for _, o := range opts {
		o(c)
	}
	if c.cmdTimeout <= 0 || c.slotTimeout <= 0 || c.statusTimeout <= 0 {
		return nil, fmt.Errorf("%w: timeouts must be positive", ErrInvalidArgument)
	}
	if c.logger == nil {
		c.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	maps.Copy(cfg.RuntimeParams, c.params)
	maps.Copy(cfg.RuntimeParams, sessionParams)
	cfg.BuildContextWatcherHandler = func(pc *pgconn.PgConn) ctxwatch.Handler {
		return &readDeadlineHandler{conn: pc}
	}
	return c, nil
}

func (c *Conn) User() string { return c.cfg.User }

type readDeadlineHandler struct {
	conn *pgconn.PgConn
}

func (h *readDeadlineHandler) HandleCancel(context.Context) {
	_ = h.conn.Conn().SetReadDeadline(time.Now()) //nolint:errcheck // best effort; the read fails anyway
}

func (h *readDeadlineHandler) HandleUnwatchAfterCancel() {
	_ = h.conn.Conn().SetReadDeadline(time.Time{}) //nolint:errcheck // a broken conn fails the next read
}

func (c *Conn) command(ctx context.Context) (*pgconn.PgConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case stateStreaming:
		return nil, ErrStreaming
	case stateClosed:
		return nil, ErrClosed
	case stateIdle, stateExported:
	}
	c.state = stateIdle
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.cmdTimeout)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, c.cfg)
	if err != nil {
		return nil, c.mapConnectError(err)
	}
	c.conn = conn
	return conn, nil
}

func (c *Conn) mapConnectError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return privilegeError(c.cfg.User, err)
		case "53300":
			return noFreeWALSenderError(err)
		}
	}
	return fmt.Errorf("pgrepl: connect: %w", err)
}

func (c *Conn) setState(s connState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != stateClosed {
		c.state = s
	}
}

func (c *Conn) streamingConn() (*pgconn.PgConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case stateStreaming:
		return c.conn, nil
	case stateClosed:
		return nil, ErrClosed
	case stateIdle, stateExported:
	}
	return nil, ErrNotStreaming
}

func (c *Conn) breakStream(conn *pgconn.PgConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.state = stateClosed
	}
	_ = conn.Conn().Close() //nolint:errcheck // the stream is already broken
}

// Close closes the connection.
func (c *Conn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = stateClosed
	if c.conn == nil {
		return nil
	}
	conn := c.conn
	c.conn = nil
	ctx, cancel := context.WithTimeout(ctx, c.cmdTimeout)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("pgrepl: close: %w", err)
	}
	return nil
}

var (
	_ pipeline.SlotManager       = (*Conn)(nil)
	_ pipeline.ReplicationSource = (*Conn)(nil)
)
