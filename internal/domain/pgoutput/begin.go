// SPDX-License-Identifier: Apache-2.0

package pgoutput

import "github.com/thnhdatle/wake/internal/domain/lsn"

type Begin struct {
	FinalLSN lsn.LSN
	TsMs     int64
	XID      uint32
}

func (*Begin) MessageType() MessageType { return MessageBegin }

func parseBegin(r *reader) *Begin {
	m := &Begin{FinalLSN: lsn.LSN(r.u64("final lsn"))}
	m.TsMs = pgTimeToUnixMs(int64(r.u64("commit timestamp"))) //nolint:gosec // G115: wire int64 sent as uint64
	m.XID = r.u32("xid")
	return m
}
