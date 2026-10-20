// SPDX-License-Identifier: Apache-2.0

package pgoutput

import "github.com/thnhdatle/wake/internal/domain/lsn"

// Begin starts a transaction. FinalLSN is the LSN of the commit record,
// i.e. the CDC event commit LSN of every event of the transaction.
type Begin struct {
	FinalLSN lsn.LSN
	// TsMs is the commit time in Unix milliseconds.
	TsMs int64
	XID  uint32
}

// MessageType implements Message.
func (*Begin) MessageType() MessageType { return MessageBegin }

func parseBegin(r *reader) *Begin {
	m := &Begin{FinalLSN: lsn.LSN(r.u64("final lsn"))}
	m.TsMs = pgTimeToUnixMs(int64(r.u64("commit timestamp"))) //nolint:gosec // G115: wire int64 sent as uint64
	m.XID = r.u32("xid")
	return m
}
