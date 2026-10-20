// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"strings"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// Wake-internal logical message prefixes. They are
// consumed by Wake and never forwarded to sinks.
const (
	InternalPrefix    = "wake."
	PrefixHeartbeat   = "wake.heartbeat"
	PrefixWatermark   = "wake.watermark"
	PrefixSnapshot    = "wake.snapshot"
	messageFlagTxnBit = 1
)

// LogicalMessage is a message written with pg_logical_emit_message.
type LogicalMessage struct {
	Flags uint8
	// LSN is the message's own WAL position; transactional messages are
	// identified by their transaction's commit LSN instead (Begin.FinalLSN).
	LSN     lsn.LSN
	Prefix  string
	Content []byte // aliases the buffer passed to Parse or Decode
}

// MessageType implements Message.
func (*LogicalMessage) MessageType() MessageType { return MessageLogical }

// Transactional reports whether the message was emitted as part of a
// transaction (delivered between its Begin and Commit) rather than
// immediately.
func (m *LogicalMessage) Transactional() bool { return m.Flags&messageFlagTxnBit != 0 }

// IsInternal reports whether the message has a Wake-internal prefix.
func (m *LogicalMessage) IsInternal() bool { return IsInternal(m.Prefix) }

// IsInternal reports whether prefix is reserved for Wake ("wake." and
// anything below it).
func IsInternal(prefix string) bool { return strings.HasPrefix(prefix, InternalPrefix) }

func parseLogicalMessage(r *reader) *LogicalMessage {
	m := &LogicalMessage{Flags: r.u8("flags")}
	m.LSN = lsn.LSN(r.u64("message lsn"))
	m.Prefix = r.cstring("prefix")
	size := int32(r.u32("content length")) //nolint:gosec // G115: wire Int32 sent as uint32
	if r.err == nil && size < 0 {
		r.fail("negative content length %d", size)
		return nil
	}
	m.Content = r.take(int(size), "content")
	return m
}
