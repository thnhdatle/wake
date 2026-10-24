// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"time"

	"github.com/thnhdatle/wake/internal/domain/lsn"
)

type TxBatch struct {
	XID        uint32
	CommitLSN  lsn.LSN
	EndLSN     lsn.LSN
	CommitTime time.Time
	Records    []Record
}
