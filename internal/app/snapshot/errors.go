// SPDX-License-Identifier: Apache-2.0

package snapshot

import "errors"

// ErrSlotMissing reports a state file whose slot does not exist (error code
// slot_missing); --reinit-state creates the slot as on first start.
var ErrSlotMissing = errors.New("snapshot: replication slot missing for existing state file")

// ErrSlotInvalidated reports a slot whose WAL was removed (wal_status lost,
// error code slot_invalidated); it must be dropped and recreated.
var ErrSlotInvalidated = errors.New("snapshot: replication slot invalidated (wal_status lost)")

// ErrSlotInUse reports a slot with an active consumer (error code
// slot_in_use).
var ErrSlotInUse = errors.New("snapshot: replication slot in use")

// ErrInvalidOption reports an invalid constructor argument or option.
var ErrInvalidOption = errors.New("snapshot: invalid option")

// ErrUnknownRun reports a state update for a snapshot run or table that is
// not in the state.
var ErrUnknownRun = errors.New("snapshot: unknown snapshot run or table")

// ErrChunkAttempts reports a chunk that failed snapshot.max chunk attempts
// in a row.
var ErrChunkAttempts = errors.New("snapshot: chunk attempts exhausted")
