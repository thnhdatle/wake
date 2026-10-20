// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/thnhdatle/wake/pkg/event"
)

// pgTimeout bounds one helper statement.
const pgTimeout = 2 * time.Minute

// PingPostgres connects once and runs SELECT 1.
func PingPostgres(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, PostgresDSN)
	if err != nil {
		return fmt.Errorf("stack: connect postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("stack: ping postgres: %w", err)
	}
	return nil
}

// PG is a small fixed pool of connections to the compose Postgres with test
// helpers (pgxpool is not a dependency of the module).
type PG struct {
	conns chan *pgx.Conn
	all   []*pgx.Conn
}

// ConnectPG opens n connections (at least 2), closed at cleanup.
func ConnectPG(tb testing.TB, n int) *PG {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), pgTimeout)
	defer cancel()
	p, err := openPG(ctx, n)
	if err != nil {
		tb.Fatalf("stack: %v", err)
	}
	tb.Cleanup(func() { p.Close(context.Background()) })
	return p
}

func openPG(ctx context.Context, n int) (*PG, error) {
	n = max(n, 2)
	p := &PG{conns: make(chan *pgx.Conn, n)}
	for range n {
		c, err := pgx.Connect(ctx, PostgresDSN)
		if err != nil {
			p.Close(ctx)
			return nil, fmt.Errorf("stack: connect postgres: %w", err)
		}
		p.all = append(p.all, c)
		p.conns <- c
	}
	return p, nil
}

// Close closes every connection.
func (p *PG) Close(ctx context.Context) {
	for _, c := range p.all {
		_ = c.Close(context.WithoutCancel(ctx))
	}
	p.all = nil
}

// With runs fn with a connection taken from the pool.
func (p *PG) With(ctx context.Context, fn func(*pgx.Conn) error) error {
	var c *pgx.Conn
	select {
	case c = <-p.conns:
	case <-ctx.Done():
		return fmt.Errorf("stack: wait for a connection: %w", ctx.Err())
	}
	defer func() { p.conns <- c }()
	return fn(c)
}

// ExecContext runs one statement (several when args is empty).
func (p *PG) ExecContext(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var tag pgconn.CommandTag
	err := p.With(ctx, func(c *pgx.Conn) error {
		var err error
		tag, err = c.Exec(ctx, sql, args...)
		return err
	})
	if err != nil {
		return tag, fmt.Errorf("stack: exec %q: %w", abbreviate(sql), err)
	}
	return tag, nil
}

// QueryRow runs a single-row query and scans it into dest.
func (p *PG) QueryRow(ctx context.Context, sql string, args []any, dest ...any) error {
	return p.With(ctx, func(c *pgx.Conn) error {
		if err := c.QueryRow(ctx, sql, args...).Scan(dest...); err != nil {
			return fmt.Errorf("stack: query %q: %w", abbreviate(sql), err)
		}
		return nil
	})
}

// Exec runs sql (several statements allowed when args is empty) and fails
// the test on error.
func (p *PG) Exec(tb testing.TB, sql string, args ...any) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), pgTimeout)
	defer cancel()
	if _, err := p.ExecContext(ctx, sql, args...); err != nil {
		tb.Fatal(err)
	}
}

func abbreviate(s string) string {
	const limit = 200
	if len(s) > limit {
		return s[:limit] + "..."
	}
	return s
}

// CurrentWALLSN returns pg_current_wal_lsn() as a number.
func (p *PG) CurrentWALLSN(ctx context.Context) (uint64, error) {
	var s string
	if err := p.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text", nil, &s); err != nil {
		return 0, err
	}
	return event.ParseLSN(s)
}

// SlotInfo is the row of pg_replication_slots for one slot.
type SlotInfo struct {
	Exists       bool
	Active       bool
	ConfirmedLSN uint64
	LagBytes     int64
}

// Slot reads the slot's confirmed_flush_lsn and lag
// (pg_current_wal_lsn() - confirmed_flush_lsn).
func (p *PG) Slot(ctx context.Context, name string) (SlotInfo, error) {
	var (
		info      SlotInfo
		confirmed *string
		lag       *int64
	)
	err := p.QueryRow(ctx, `SELECT active, confirmed_flush_lsn::text,
		(pg_current_wal_lsn() - confirmed_flush_lsn)::bigint
		FROM pg_replication_slots WHERE slot_name = $1`, []any{name}, &info.Active, &confirmed, &lag)
	if errors.Is(err, pgx.ErrNoRows) {
		return info, nil
	}
	if err != nil {
		return info, fmt.Errorf("stack: read slot %s: %w", name, err)
	}
	info.Exists = true
	if confirmed != nil {
		if info.ConfirmedLSN, err = event.ParseLSN(*confirmed); err != nil {
			return info, fmt.Errorf("stack: slot %s: %w", name, err)
		}
	}
	if lag != nil {
		info.LagBytes = *lag
	}
	return info, nil
}

// SlotName returns the replication slot of a Wake source (wake_<name>).
func SlotName(source string) string { return "wake_" + source }

// WaitConfirmed polls the slot until its confirmed_flush_lsn reaches target
// or ctx ends: the M2 "Wake confirmed the final LSN" step. Wake confirms an
// idle source through heartbeats (heartbeat.interval), so target may be a
// pg_current_wal_lsn() read after the writers stopped.
func (p *PG) WaitConfirmed(ctx context.Context, slot string, target uint64) error {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	var last SlotInfo
	for {
		info, err := p.Slot(ctx, slot)
		if err == nil {
			last = info
			if info.Exists && info.ConfirmedLSN >= target {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("stack: slot %s confirmed %s, want >= %s: %w", slot,
				event.FormatLSN(last.ConfirmedLSN), event.FormatLSN(target), ctx.Err())
		case <-tick.C:
		}
	}
}

// WaitQuiesced reads pg_current_wal_lsn() now and waits until the slot
// confirmed it; it returns the target LSN.
func (p *PG) WaitQuiesced(ctx context.Context, slot string) (uint64, error) {
	target, err := p.CurrentWALLSN(ctx)
	if err != nil {
		return 0, err
	}
	return target, p.WaitConfirmed(ctx, slot, target)
}

// DropSource drops the slot and the managed publications of a Wake source
// (wake_<name>, wake_<name>_insert), waiting for an active walsender to go.
func (p *PG) DropSource(ctx context.Context, source string) error {
	slot := SlotName(source)
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := p.ExecContext(ctx, `SELECT pg_drop_replication_slot(slot_name)
			FROM pg_replication_slots WHERE slot_name = $1`, slot)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("stack: drop slot %s: %w", slot, err)
		}
		_, _ = p.ExecContext(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
			WHERE slot_name = $1 AND active_pid IS NOT NULL`, slot)
		time.Sleep(500 * time.Millisecond)
	}
	for _, pub := range []string{slot, slot + "_insert"} {
		if _, err := p.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{pub}.Sanitize()); err != nil {
			return err
		}
	}
	return nil
}
