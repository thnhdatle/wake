// SPDX-License-Identifier: Apache-2.0

package snapshot

import "errors"

var ErrSlotMissing = errors.New("snapshot: replication slot missing for existing state file")

var ErrSlotInvalidated = errors.New("snapshot: replication slot invalidated (wal_status lost)")

var ErrSlotInUse = errors.New("snapshot: replication slot in use")

var ErrInvalidOption = errors.New("snapshot: invalid option")

var ErrUnknownRun = errors.New("snapshot: unknown snapshot run or table")

var ErrChunkAttempts = errors.New("snapshot: chunk attempts exhausted")
