// SPDX-License-Identifier: Apache-2.0

package stream

import "errors"

// ErrOutOfOrder reports a transaction registered out of commit order: its
// commit LSN is before the previous transaction's end LSN (or the tracker's
// start), or its end LSN is before its commit LSN. Postgres does not resend
// transactions at or before the confirmed LSN, so the reader skips them.
var ErrOutOfOrder = errors.New("stream: transaction out of commit order")

// ErrUnknownTx reports a handle of a transaction that is not open.
var ErrUnknownTx = errors.New("stream: unknown or completed transaction")

// ErrSealed reports a delivery added to a sealed transaction or group, or a
// second Seal.
var ErrSealed = errors.New("stream: transaction or group already sealed")

// ErrUnknownGroup reports a group that is not open or belongs to another
// transaction.
var ErrUnknownGroup = errors.New("stream: unknown or completed group")

// ErrGroupOpen reports sealing a transaction that still has an open group.
var ErrGroupOpen = errors.New("stream: transaction has an open group")

// ErrUnknownDelivery reports an acknowledgement of a delivery that is not
// pending (acknowledged twice, or never added).
var ErrUnknownDelivery = errors.New("stream: unknown or already acknowledged delivery")

// ErrStatusInterval reports a stream.status_interval that is not below half
// of the server's wal_sender_timeout.
var ErrStatusInterval = errors.New("stream: status_interval must be below half of wal_sender_timeout")

// ErrInvalidOption reports an invalid constructor argument or option.
var ErrInvalidOption = errors.New("stream: invalid option")

// ErrSpillCorrupt reports a spilled record that cannot be decoded.
var ErrSpillCorrupt = errors.New("stream: corrupt spill record")

// ErrProtocol reports a replication stream that violates the pgoutput
// transaction structure: a row or transactional message outside Begin and
// Commit, nested transactions, a Commit that does not match its Begin, or a
// non-transactional message inside a transaction.
var ErrProtocol = errors.New("stream: unexpected replication message")
