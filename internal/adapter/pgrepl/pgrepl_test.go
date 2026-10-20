// SPDX-License-Identifier: Apache-2.0

package pgrepl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestValidateSlotName_AcceptsSlotRule(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		slot string
		ok   bool
	}{
		{name: "simple", slot: "wake_app", ok: true},
		{name: "digits", slot: "wake_01", ok: true},
		{name: "max length", slot: string(make63('a')), ok: true},
		{name: "empty", slot: ""},
		{name: "too long", slot: string(make63('a')) + "a"},
		{name: "upper case", slot: "Wake"},
		{name: "dash", slot: "wake-app"},
		{name: "quote", slot: "wake'app"},
		{name: "space", slot: "wake app"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateSlotName(tc.slot)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidArgument)
			}
		})
	}
}

func make63(c byte) []byte {
	b := make([]byte, 63)
	for i := range b {
		b[i] = c
	}
	return b
}

func TestStartCommand_BuildsPgoutputOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		opts pipeline.StartOptions
		want string
		err  bool
	}{
		{
			name: "two publications with messages",
			opts: pipeline.StartOptions{
				Slot: "wake_app", Start: lsn.LSN(0x1_00000AB0), ProtoVersion: 1,
				Publications: []string{"wake_app", "wake_app_insert"}, Messages: true,
			},
			want: `START_REPLICATION SLOT wake_app LOGICAL 1/AB0 (proto_version '1', ` +
				`publication_names '"wake_app","wake_app_insert"', messages 'true')`,
		},
		{
			name: "quotes in names are escaped",
			opts: pipeline.StartOptions{Slot: "s", ProtoVersion: 1, Publications: []string{`My"Pub's`}},
			want: `START_REPLICATION SLOT s LOGICAL 0/0 (proto_version '1', publication_names '"My""Pub''s"')`,
		},
		{name: "bad slot", opts: pipeline.StartOptions{Slot: "S", ProtoVersion: 1, Publications: []string{"p"}}, err: true},
		{name: "no publication", opts: pipeline.StartOptions{Slot: "s", ProtoVersion: 1}, err: true},
		{
			name: "empty publication", err: true,
			opts: pipeline.StartOptions{Slot: "s", ProtoVersion: 1, Publications: []string{""}},
		},
		{name: "no proto version", opts: pipeline.StartOptions{Slot: "s", Publications: []string{"p"}}, err: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := startCommand(&tc.opts)
			if tc.err {
				require.ErrorIs(t, err, ErrInvalidArgument)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseCopyData_XLogDataAndKeepalive(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 18, 12, 0, 0, 123456000, time.UTC)
	xlog := []byte{'w'}
	xlog = binary.BigEndian.AppendUint64(xlog, 0x10)
	xlog = binary.BigEndian.AppendUint64(xlog, 0x20)
	xlog = binary.BigEndian.AppendUint64(xlog, uint64(pgMicros(at)))
	xlog = append(xlog, 'B', 1, 2)
	msg, err := parseCopyData(xlog)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ReplicationMessageXLogData, msg.Kind)
	assert.Equal(t, lsn.LSN(0x10), msg.XLogData.WALStart)
	assert.Equal(t, lsn.LSN(0x20), msg.XLogData.ServerWALEnd)
	assert.True(t, at.Equal(msg.XLogData.ServerTime))
	assert.Equal(t, []byte{'B', 1, 2}, msg.XLogData.Data)

	ka := []byte{'k'}
	ka = binary.BigEndian.AppendUint64(ka, 0x30)
	ka = binary.BigEndian.AppendUint64(ka, uint64(pgMicros(at)))
	ka = append(ka, 1)
	msg, err = parseCopyData(ka)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ReplicationMessageKeepalive, msg.Kind)
	assert.Equal(t, lsn.LSN(0x30), msg.Keepalive.ServerWALEnd)
	assert.True(t, msg.Keepalive.ReplyRequested)
	assert.True(t, at.Equal(msg.Keepalive.ServerTime))
}

func TestParseCopyData_RejectsMalformed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "short xlog", data: append([]byte{'w'}, make([]byte, 23)...)},
		{name: "short keepalive", data: append([]byte{'k'}, make([]byte, 16)...)},
		{name: "long keepalive", data: append([]byte{'k'}, make([]byte, 18)...)},
		{name: "unknown type", data: []byte{'x', 0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseCopyData(tc.data)
			require.ErrorIs(t, err, ErrProtocol)
		})
	}
}

func TestEncodeStatus_ReportsFlushedAsApplied(t *testing.T) {
	t.Parallel()
	at := time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC)
	buf := encodeStatus(&pipeline.StandbyStatus{
		Written: 0x200, Flushed: 0x100, ClientTime: at, ReplyRequested: true,
	})
	require.Len(t, buf, 39)
	assert.Equal(t, byte('d'), buf[0])
	assert.Equal(t, uint32(38), binary.BigEndian.Uint32(buf[1:]))
	assert.Equal(t, byte('r'), buf[5])
	assert.Equal(t, uint64(0x200), binary.BigEndian.Uint64(buf[6:]))
	assert.Equal(t, uint64(0x100), binary.BigEndian.Uint64(buf[14:]), "flushed is the confirmed LSN")
	assert.Equal(t, uint64(0x100), binary.BigEndian.Uint64(buf[22:]), "applied equals flushed")
	assert.Equal(t, uint64(1_000_000), binary.BigEndian.Uint64(buf[30:]))
	assert.Equal(t, byte(1), buf[38])

	buf = encodeStatus(&pipeline.StandbyStatus{Written: 0x200, ClientTime: at})
	assert.Zero(t, binary.BigEndian.Uint64(buf[14:]), "nothing confirmed is never reported as written")
	assert.Equal(t, byte(0), buf[38])
}

func TestPgTime_RoundTrips(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 18, 1, 2, 3, 456789000, time.UTC)
	assert.True(t, at.Equal(pgTime(pgMicros(at))))
	assert.True(t, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Equal(pgTime(0)))
}

func TestError_MatchesSentinelAndCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("server said no")
	tests := []struct {
		name string
		err  *Error
		kind error
		code string
		text string
	}{
		{
			name: "privilege", err: privilegeError("app", cause), kind: ErrReplicationPrivilege,
			code: CodeReplicationPrivilege, text: "ALTER ROLE app REPLICATION",
		},
		{
			name: "privilege without role", err: privilegeError("", nil), kind: ErrReplicationPrivilege,
			code: CodeReplicationPrivilege, text: "GRANT rds_replication TO <role>",
		},
		{
			name: "in use with pid", err: slotInUseError("s", 42, cause), kind: ErrSlotInUse, code: CodeSlotInUse,
			text: "used by pid 42",
		},
		{
			name: "in use without pid", err: slotInUseError("s", 0, nil), kind: ErrSlotInUse, code: CodeSlotInUse,
			text: "another consumer",
		},
		{
			name: "invalidated", err: slotInvalidatedError("s", cause), kind: ErrSlotInvalidated,
			code: CodeSlotInvalidated, text: "wake slot drop",
		},
		{
			name: "senders", err: noFreeWALSenderError(cause), kind: ErrNoFreeWALSender, code: CodeNoFreeWALSender,
			text: "max_wal_senders",
		},
		{
			name: "slots", err: noFreeSlotError("s", cause), kind: ErrNoFreeSlot, code: CodeNoFreeSlot,
			text: "max_replication_slots",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wrapped := fmt.Errorf("outer: %w", tc.err)
			require.ErrorIs(t, wrapped, tc.kind)
			if tc.err.cause != nil {
				require.ErrorIs(t, wrapped, cause)
				assert.Contains(t, tc.err.Error(), cause.Error())
			}
			var coded interface{ ErrorCode() string }
			require.ErrorAs(t, wrapped, &coded)
			assert.Equal(t, tc.code, coded.ErrorCode())
			assert.Contains(t, tc.err.Error(), tc.text)
			assert.Contains(t, tc.err.Error(), "("+tc.code+")")
		})
	}
}

func TestNew_ValidatesArguments(t *testing.T) {
	t.Parallel()
	_, err := New("postgres://%zz")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = New("postgres://u@localhost/db", WithCommandTimeout(0))
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = New("postgres://u@localhost/db", WithStatusTimeout(-time.Second))
	require.ErrorIs(t, err, ErrInvalidArgument)
	c, err := New("postgres://u@localhost/db?application_name=x&DateStyle=German",
		WithRuntimeParam("DateStyle", "SQL"), WithRuntimeParam("wal_sender_timeout", "5s"),
		WithCreateSlotTimeout(time.Minute), WithLogger(nil))
	require.NoError(t, err)
	assert.Equal(t, "u", c.User())
	params := c.cfg.RuntimeParams
	assert.Equal(t, "ISO", params["DateStyle"], "session settings cannot be overridden")
	assert.Equal(t, "database", params["replication"])
	assert.Equal(t, "postgres", params["IntervalStyle"])
	assert.Equal(t, "UTF8", params["client_encoding"])
	assert.Equal(t, "5s", params["wal_sender_timeout"])
	assert.Equal(t, "x", params["application_name"])
}

func TestConn_MapErrors_TranslatesSQLStates(t *testing.T) {
	t.Parallel()
	c, err := New("postgres://app@localhost/db")
	require.NoError(t, err)
	pg := func(code string) error { return fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code}) }
	require.ErrorIs(t, c.mapConnectError(pg("42501")), ErrReplicationPrivilege)
	require.ErrorIs(t, c.mapConnectError(pg("53300")), ErrNoFreeWALSender)
	other := c.mapConnectError(pg("28P01"))
	require.NotErrorIs(t, other, ErrReplicationPrivilege)
	assert.Contains(t, other.Error(), "pgrepl: connect")
	require.ErrorIs(t, c.mapCreateError("s", pg("42710")), ErrSlotExists)
	require.ErrorIs(t, c.mapCreateError("s", pg("53400")), ErrNoFreeSlot)
	require.ErrorIs(t, c.mapCreateError("s", pg("42501")), ErrReplicationPrivilege)
	assert.Contains(t, c.mapCreateError("s", errors.New("boom")).Error(), "create slot s")
	require.ErrorIs(t, c.mapStartError(t.Context(), nil, "s", pg("42704")), ErrSlotNotFound)
	plain := errors.New("network")
	require.ErrorIs(t, c.mapStartError(t.Context(), nil, "s", plain), plain)
	assert.Contains(t, c.mapStartError(t.Context(), nil, "s", pg("XX000")).Error(), "start replication on slot s")
}

func TestParseSlotRow_ReadsPIDAndStatus(t *testing.T) {
	t.Parallel()
	row, err := parseSlotRow([]*pgconn.Result{{Rows: [][][]byte{{[]byte("123"), []byte("lost")}}}})
	require.NoError(t, err)
	assert.Equal(t, slotRow{found: true, activePID: 123, walStatus: "lost"}, row)
	row, err = parseSlotRow([]*pgconn.Result{{Rows: [][][]byte{{nil, []byte("reserved")}}}})
	require.NoError(t, err)
	assert.Equal(t, slotRow{found: true, walStatus: "reserved"}, row)
	row, err = parseSlotRow([]*pgconn.Result{{}})
	require.NoError(t, err)
	assert.False(t, row.found)
	_, err = parseSlotRow([]*pgconn.Result{{Rows: [][][]byte{{[]byte("x"), nil}}}})
	require.ErrorIs(t, err, ErrProtocol)
	_, err = parseSlotRow([]*pgconn.Result{{Rows: [][][]byte{{nil}}}})
	require.ErrorIs(t, err, ErrProtocol)
}
