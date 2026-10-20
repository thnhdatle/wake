// SPDX-License-Identifier: Apache-2.0

package pgrepl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Replication protocol message bytes (CopyData payloads).
const (
	msgXLogData        = 'w'
	msgKeepalive       = 'k'
	msgStandbyStatus   = 'r'
	xlogHeaderLen      = 24
	keepaliveLen       = 17
	statusPayloadLen   = 34
	copyDataHeaderLen  = 5
	pgEpochUnixSeconds = 946_684_800
)

// StartReplication implements pipeline.ReplicationSource. It must be the
// next command after CreateSlot when the exported snapshot is in use. A slot
// with an active consumer is slot_in_use (naming its PID); an invalidated
// slot is slot_invalidated.
func (c *Conn) StartReplication(ctx context.Context, opts pipeline.StartOptions) error {
	sql, err := startCommand(&opts)
	if err != nil {
		return err
	}
	conn, err := c.command(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.cmdTimeout)
	defer cancel()
	if err := startReplication(ctx, conn, sql); err != nil {
		return c.mapStartError(ctx, conn, opts.Slot, err)
	}
	c.setState(stateStreaming)
	c.logger.LogAttrs(ctx, slog.LevelInfo, "replication started", slog.String("component", "pgrepl"),
		slog.String("operation", "start_replication"), slog.String("lsn", opts.Start.String()))
	return nil
}

// startReplication sends the command and waits for CopyBothResponse. On an
// ErrorResponse it reads up to ReadyForQuery so the connection stays usable.
func startReplication(ctx context.Context, conn *pgconn.PgConn, sql string) error {
	conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	if err := conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("pgrepl: send START_REPLICATION: %w", err)
	}
	var pgErr error
	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("pgrepl: START_REPLICATION: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CopyBothResponse:
			return nil
		case *pgproto3.ErrorResponse:
			pgErr = pgconn.ErrorResponseToPgError(m)
		case *pgproto3.ReadyForQuery:
			if pgErr == nil {
				return fmt.Errorf("%w: START_REPLICATION ended without CopyBothResponse", ErrProtocol)
			}
			return fmt.Errorf("pgrepl: START_REPLICATION: %w", pgErr)
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		default:
			return fmt.Errorf("%w: unexpected %T after START_REPLICATION", ErrProtocol, msg)
		}
	}
}

// mapStartError translates a START_REPLICATION failure, reading the slot
// row to name the active PID or confirm the invalidation.
func (c *Conn) mapStartError(ctx context.Context, conn *pgconn.PgConn, slot string, err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch pgErr.Code {
	case "55006": // object_in_use: "replication slot is active for PID n"
		row, _ := lookupSlot(ctx, conn, slot) //nolint:errcheck // the PID is best effort
		return slotInUseError(slot, row.activePID, err)
	case "55000": // object_not_in_prerequisite_state, e.g. an invalidated slot
		if row, lerr := lookupSlot(ctx, conn, slot); lerr == nil && row.walStatus == "lost" {
			return slotInvalidatedError(slot, err)
		}
	case "42704": // undefined_object
		return fmt.Errorf("%w: %s: %w", ErrSlotNotFound, slot, err)
	}
	return fmt.Errorf("pgrepl: start replication on slot %s: %w", slot, err)
}

// Receive implements pipeline.ReplicationSource. XLogData.Data aliases the
// connection's read buffer and is valid until the next Receive. When ctx
// ends, Receive returns its error and the connection stays usable.
func (c *Conn) Receive(ctx context.Context) (pipeline.ReplicationMessage, error) {
	conn, err := c.streamingConn()
	if err != nil {
		return pipeline.ReplicationMessage{}, err
	}
	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil && !conn.IsClosed() {
				return pipeline.ReplicationMessage{}, fmt.Errorf("pgrepl: receive: %w", cerr)
			}
			c.breakStream(conn)
			return pipeline.ReplicationMessage{}, fmt.Errorf("pgrepl: receive: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			out, err := parseCopyData(m.Data)
			if err != nil {
				c.breakStream(conn)
			}
			return out, err
		case *pgproto3.ErrorResponse:
			c.breakStream(conn)
			return pipeline.ReplicationMessage{}, fmt.Errorf("pgrepl: stream: %w", pgconn.ErrorResponseToPgError(m))
		case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		default:
			c.breakStream(conn)
			return pipeline.ReplicationMessage{}, fmt.Errorf("%w: unexpected %T while streaming", ErrProtocol, msg)
		}
	}
}

// parseCopyData decodes an XLogData or primary keepalive message.
func parseCopyData(data []byte) (pipeline.ReplicationMessage, error) {
	if len(data) == 0 {
		return pipeline.ReplicationMessage{}, fmt.Errorf("%w: empty CopyData", ErrProtocol)
	}
	body := data[1:]
	switch data[0] {
	case msgXLogData:
		if len(body) < xlogHeaderLen {
			return pipeline.ReplicationMessage{}, fmt.Errorf("%w: XLogData of %d bytes", ErrProtocol, len(body))
		}
		return pipeline.ReplicationMessage{Kind: pipeline.ReplicationMessageXLogData, XLogData: pipeline.XLogData{
			WALStart:     lsn.LSN(binary.BigEndian.Uint64(body)),
			ServerWALEnd: lsn.LSN(binary.BigEndian.Uint64(body[8:])),
			ServerTime:   pgTime(int64(binary.BigEndian.Uint64(body[16:]))), //nolint:gosec // G115: wire int64
			Data:         body[xlogHeaderLen:],
		}}, nil
	case msgKeepalive:
		if len(body) != keepaliveLen {
			return pipeline.ReplicationMessage{}, fmt.Errorf("%w: keepalive of %d bytes", ErrProtocol, len(body))
		}
		return pipeline.ReplicationMessage{Kind: pipeline.ReplicationMessageKeepalive, Keepalive: pipeline.Keepalive{
			ServerWALEnd:   lsn.LSN(binary.BigEndian.Uint64(body)),
			ServerTime:     pgTime(int64(binary.BigEndian.Uint64(body[8:]))), //nolint:gosec // G115: wire int64
			ReplyRequested: body[16] != 0,
		}}, nil
	}
	return pipeline.ReplicationMessage{}, fmt.Errorf("%w: CopyData type %q", ErrProtocol, data[0])
}

// SendStandbyStatus implements pipeline.StandbyStatusSender. It may run
// while another goroutine is blocked in Receive. Flushed is also reported
// as the applied position; a zero ClientTime means now.
func (c *Conn) SendStandbyStatus(ctx context.Context, st pipeline.StandbyStatus) error {
	conn, err := c.streamingConn()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pgrepl: send standby status: %w", err)
	}
	if st.ClientTime.IsZero() {
		st.ClientTime = time.Now()
	}
	buf := encodeStatus(&st)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	nc := conn.Conn()
	deadline := time.Now().Add(c.statusTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := nc.SetWriteDeadline(deadline); err != nil {
		c.breakStream(conn)
		return fmt.Errorf("pgrepl: send standby status: %w", err)
	}
	if _, err := nc.Write(buf); err != nil {
		// A partial or timed-out write leaves the protocol stream (and TLS
		// state) corrupt: give up the connection.
		c.breakStream(conn)
		return fmt.Errorf("pgrepl: send standby status: %w", err)
	}
	if err := nc.SetWriteDeadline(time.Time{}); err != nil {
		c.breakStream(conn)
		return fmt.Errorf("pgrepl: send standby status: %w", err)
	}
	return nil
}

// encodeStatus encodes a standby status update as a CopyData message:
// 'd', length, 'r', written, flushed, applied (= flushed), client time
// (microseconds since 2000-01-01) and the reply flag.
func encodeStatus(st *pipeline.StandbyStatus) []byte {
	buf := make([]byte, 0, copyDataHeaderLen+statusPayloadLen)
	buf = append(buf, 'd')
	buf = binary.BigEndian.AppendUint32(buf, uint32(4+statusPayloadLen))
	buf = append(buf, msgStandbyStatus)
	buf = binary.BigEndian.AppendUint64(buf, uint64(st.Written))
	buf = binary.BigEndian.AppendUint64(buf, uint64(st.Flushed))
	buf = binary.BigEndian.AppendUint64(buf, uint64(st.Flushed))
	buf = binary.BigEndian.AppendUint64(buf, uint64(pgMicros(st.ClientTime))) //nolint:gosec // G115: wire int64
	if st.ReplyRequested {
		return append(buf, 1)
	}
	return append(buf, 0)
}

// pgTime converts microseconds since 2000-01-01 UTC.
func pgTime(us int64) time.Time {
	return time.Unix(pgEpochUnixSeconds, 0).Add(time.Duration(us) * time.Microsecond).UTC()
}

// pgMicros converts to microseconds since 2000-01-01 UTC.
func pgMicros(t time.Time) int64 {
	return t.Sub(time.Unix(pgEpochUnixSeconds, 0)).Microseconds()
}
