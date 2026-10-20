// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

// TxBatch is an assembled transaction released at Commit, or the
// single-record transaction of a forwarded non-transactional message.
type TxBatch struct {
	// XID is the Postgres transaction id; zero for a non-transactional
	// message.
	XID uint32
	// CommitLSN is the LSN of the commit record (the message LSN for a
	// non-transactional message); events carry it as their lsn.
	CommitLSN lsn.LSN
	// EndLSN is the end of the commit record; it becomes the confirmed LSN
	// once this and every earlier transaction are acknowledged.
	EndLSN lsn.LSN
	// CommitTime is the commit timestamp (events' ts_ms).
	CommitTime time.Time
	// Records are the captured records in seq order, before routing. A
	// transaction without records (heartbeat, LOW watermark, snapshot
	// trigger) still advances the confirmed LSN.
	Records []Record
}
