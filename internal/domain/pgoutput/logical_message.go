// SPDX-License-Identifier: Apache-2.0

package pgoutput

import (
	"strings"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

const (
	InternalPrefix    = "wake."
	PrefixHeartbeat   = "wake.heartbeat"
	PrefixWatermark   = "wake.watermark"
	PrefixSnapshot    = "wake.snapshot"
	messageFlagTxnBit = 1
)

type LogicalMessage struct {
	Flags   uint8
	LSN     lsn.LSN
	Prefix  string
	Content []byte
}

func (*LogicalMessage) MessageType() MessageType { return MessageLogical }

func (m *LogicalMessage) Transactional() bool { return m.Flags&messageFlagTxnBit != 0 }

func (m *LogicalMessage) IsInternal() bool { return IsInternal(m.Prefix) }

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
