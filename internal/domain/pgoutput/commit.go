// SPDX-License-Identifier: Apache-2.0

package pgoutput

import "github.com/thnhdatle/wake/internal/domain/lsn"

// Commit ends a transaction.
type Commit struct {
	// Flags is currently unused by Postgres (always 0).
	Flags     uint8
	CommitLSN lsn.LSN
	// EndLSN is the end of the commit record: the LSN to confirm as flushed
	// once the transaction is acknowledged.
	EndLSN lsn.LSN
	// TsMs is the commit time in Unix milliseconds.
	TsMs int64
}

// MessageType implements Message.
func (*Commit) MessageType() MessageType { return MessageCommit }

func parseCommit(r *reader) *Commit {
	m := &Commit{Flags: r.u8("flags")}
	m.CommitLSN = lsn.LSN(r.u64("commit lsn"))
	m.EndLSN = lsn.LSN(r.u64("end lsn"))
	m.TsMs = pgTimeToUnixMs(int64(r.u64("commit timestamp"))) //nolint:gosec // G115: wire int64 sent as uint64
	return m
}
