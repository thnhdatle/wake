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

// Default timeouts.
const (
	// DefaultCommandTimeout bounds IDENTIFY_SYSTEM, DROP_REPLICATION_SLOT,
	// START_REPLICATION and connecting.
	DefaultCommandTimeout = 30 * time.Second
	// DefaultCreateSlotTimeout bounds CREATE_REPLICATION_SLOT, which waits
	// for the transactions running at that moment to finish.
	DefaultCreateSlotTimeout = 10 * time.Minute
	// DefaultStatusTimeout bounds one standby status write.
	DefaultStatusTimeout = 10 * time.Second
)

// sessionParams are the startup parameters schema.AppendJSON relies on;
// they override the DSN and options.
var sessionParams = map[string]string{
	"replication":        "database",
	"DateStyle":          "ISO",
	"IntervalStyle":      "postgres",
	"client_encoding":    "UTF8",
	"TimeZone":           "UTC",
	"extra_float_digits": "1",
}

// connState is the lifecycle of the replication connection.
type connState uint8

const (
	stateIdle      connState = iota // no command pending, not streaming
	stateExported                   // a slot with an exported snapshot was just created
	stateStreaming                  // START_REPLICATION succeeded
	stateClosed                     // Close was called or the stream broke
)

// Option configures a Conn.
type Option func(*Conn)

// WithCommandTimeout bounds connecting and every command except
// CREATE_REPLICATION_SLOT (default DefaultCommandTimeout).
func WithCommandTimeout(d time.Duration) Option { return func(c *Conn) { c.cmdTimeout = d } }

// WithCreateSlotTimeout bounds CREATE_REPLICATION_SLOT (default
// DefaultCreateSlotTimeout).
func WithCreateSlotTimeout(d time.Duration) Option { return func(c *Conn) { c.slotTimeout = d } }

// WithStatusTimeout bounds one standby status write (default
// DefaultStatusTimeout).
func WithStatusTimeout(d time.Duration) Option { return func(c *Conn) { c.statusTimeout = d } }

// WithRuntimeParam sets a session parameter of the replication connection,
// for example application_name or wal_sender_timeout. The parameters of
// the package documentation cannot be overridden.
func WithRuntimeParam(name, value string) Option {
	return func(c *Conn) {
		if c.params == nil {
			c.params = map[string]string{}
		}
		c.params[name] = value
	}
}

// WithLogger sets the logger (default: discard).
func WithLogger(l *slog.Logger) Option { return func(c *Conn) { c.logger = l } }

// Conn is a replication connection. It implements pipeline.SlotManager and
// pipeline.ReplicationSource; see the package documentation for the
// concurrency rules.
type Conn struct {
	cfg           *pgconn.Config
	params        map[string]string
	cmdTimeout    time.Duration
	slotTimeout   time.Duration
	statusTimeout time.Duration
	logger        *slog.Logger

	mu    sync.Mutex // protects conn and state; never held while Receive blocks
	conn  *pgconn.PgConn
	state connState

	writeMu sync.Mutex // serializes standby status writes
}

// New parses the DSN and validates the options. It does not connect: the
// first command opens the connection.
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

// User returns the role name of the connection.
func (c *Conn) User() string { return c.cfg.User }

// readDeadlineHandler interrupts a blocked read when its context ends by
// setting a read deadline. pgconn's default handler sends a cancel request
// (which would end the walsender) and sets a read and write deadline (which
// would break a concurrent status write).
type readDeadlineHandler struct {
	conn *pgconn.PgConn
}

// HandleCancel implements ctxwatch.Handler.
func (h *readDeadlineHandler) HandleCancel(context.Context) {
	_ = h.conn.Conn().SetReadDeadline(time.Now()) //nolint:errcheck // best effort; the read fails anyway
}

// HandleUnwatchAfterCancel implements ctxwatch.Handler.
func (h *readDeadlineHandler) HandleUnwatchAfterCancel() {
	_ = h.conn.Conn().SetReadDeadline(time.Time{}) //nolint:errcheck // a broken conn fails the next read
}

// command returns a connection for a command, connecting when needed. It
// fails while streaming or after Close and ends an exported snapshot.
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

// mapConnectError translates a failed connection attempt.
func (c *Conn) mapConnectError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501": // insufficient_privilege: no REPLICATION attribute
			return privilegeError(c.cfg.User, err)
		case "53300": // too_many_connections: max_wal_senders reached
			return noFreeWALSenderError(err)
		}
	}
	return fmt.Errorf("pgrepl: connect: %w", err)
}

// setState records the state after a command.
func (c *Conn) setState(s connState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != stateClosed {
		c.state = s
	}
}

// streamingConn returns the connection of a started stream.
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

// breakStream closes the socket of a broken stream; Receive then fails.
func (c *Conn) breakStream(conn *pgconn.PgConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.state = stateClosed
	}
	_ = conn.Conn().Close() //nolint:errcheck // the stream is already broken
}

// Close ends the connection. It must not run concurrently with Receive.
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

// Compile-time checks of the ports Conn implements.
var (
	_ pipeline.SlotManager       = (*Conn)(nil)
	_ pipeline.ReplicationSource = (*Conn)(nil)
)
