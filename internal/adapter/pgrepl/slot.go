// SPDX-License-Identifier: Apache-2.0

package pgrepl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// IdentifySystem implements pipeline.SlotManager. It opens the replication
// connection if needed; its failure is the replication privilege check
// (code replication_privilege).
func (c *Conn) IdentifySystem(ctx context.Context) (pipeline.SystemInfo, error) {
	conn, err := c.command(ctx)
	if err != nil {
		return pipeline.SystemInfo{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.cmdTimeout)
	defer cancel()
	res, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			return pipeline.SystemInfo{}, privilegeError(c.cfg.User, err)
		}
		return pipeline.SystemInfo{}, fmt.Errorf("pgrepl: identify system: %w", err)
	}
	return pipeline.SystemInfo{
		SystemID: res.SystemID, Timeline: res.Timeline, XLogPos: lsn.LSN(res.XLogPos), DBName: res.DBName,
	}, nil
}

// CreateSlot implements pipeline.SlotManager: CREATE_REPLICATION_SLOT
// <name> LOGICAL pgoutput EXPORT_SNAPSHOT. The exported snapshot stays
// importable until this connection runs its next command, which must be
// StartReplication (see the package documentation).
func (c *Conn) CreateSlot(ctx context.Context, name string) (pipeline.SlotInfo, error) {
	if err := ValidateSlotName(name); err != nil {
		return pipeline.SlotInfo{}, err
	}
	conn, err := c.command(ctx)
	if err != nil {
		return pipeline.SlotInfo{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.slotTimeout)
	defer cancel()
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, name, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		return pipeline.SlotInfo{}, c.mapCreateError(name, err)
	}
	cp, err := lsn.Parse(res.ConsistentPoint)
	if err != nil {
		return pipeline.SlotInfo{}, fmt.Errorf("pgrepl: create slot %s: consistent point: %w", name, err)
	}
	c.setState(stateExported)
	c.logger.LogAttrs(ctx, slog.LevelInfo, "replication slot created", slog.String("component", "pgrepl"),
		slog.String("operation", "create_slot"), slog.String("lsn", cp.String()))
	return pipeline.SlotInfo{Name: res.SlotName, ConsistentPoint: cp, SnapshotName: res.SnapshotName}, nil
}

func (c *Conn) mapCreateError(slot string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42710": // duplicate_object
			return fmt.Errorf("%w: %s: %w", ErrSlotExists, slot, err)
		case "53400": // configuration_limit_exceeded
			return noFreeSlotError(slot, err)
		case "42501":
			return privilegeError(c.cfg.User, err)
		}
	}
	return fmt.Errorf("pgrepl: create slot %s: %w", slot, err)
}

// DropSlot implements pipeline.SlotManager: DROP_REPLICATION_SLOT without
// WAIT. An absent slot is not an error; an active one is slot_in_use.
func (c *Conn) DropSlot(ctx context.Context, name string) error {
	if err := ValidateSlotName(name); err != nil {
		return err
	}
	conn, err := c.command(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.cmdTimeout)
	defer cancel()
	err = pglogrepl.DropReplicationSlot(ctx, conn, name, pglogrepl.DropReplicationSlotOptions{})
	if err == nil {
		c.logger.LogAttrs(ctx, slog.LevelInfo, "replication slot dropped", slog.String("component", "pgrepl"),
			slog.String("operation", "drop_slot"))
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42704": // undefined_object
			return nil
		case "55006": // object_in_use
			st, _ := lookupSlot(ctx, conn, name) //nolint:errcheck // the PID is best effort
			return slotInUseError(name, st.activePID, err)
		}
	}
	return fmt.Errorf("pgrepl: drop slot %s: %w", name, err)
}

// slotRow is the part of a pg_replication_slots row used to explain errors.
type slotRow struct {
	found     bool
	activePID int32
	walStatus string
}

// lookupSlot reads the slot's row over the replication connection, which
// accepts simple-protocol SQL (replication=database). name is validated.
func lookupSlot(ctx context.Context, conn *pgconn.PgConn, name string) (slotRow, error) {
	if err := ValidateSlotName(name); err != nil {
		return slotRow{}, err
	}
	sql := "SELECT active_pid, wal_status FROM pg_catalog.pg_replication_slots WHERE slot_name = '" + name + "'"
	res, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return slotRow{}, fmt.Errorf("pgrepl: look up slot %s: %w", name, err)
	}
	return parseSlotRow(res)
}

func parseSlotRow(res []*pgconn.Result) (slotRow, error) {
	if len(res) != 1 || len(res[0].Rows) == 0 {
		return slotRow{}, nil
	}
	row := res[0].Rows[0]
	if len(row) != 2 {
		return slotRow{}, fmt.Errorf("%w: slot row has %d columns", ErrProtocol, len(row))
	}
	out := slotRow{found: true, walStatus: string(row[1])}
	if row[0] != nil {
		pid, err := strconv.ParseInt(string(row[0]), 10, 32)
		if err != nil {
			return slotRow{}, fmt.Errorf("%w: active_pid %q", ErrProtocol, row[0])
		}
		out.activePID = int32(pid)
	}
	return out, nil
}
