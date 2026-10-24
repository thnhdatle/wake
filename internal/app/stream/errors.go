// SPDX-License-Identifier: Apache-2.0

package stream

import "errors"

var ErrOutOfOrder = errors.New("stream: transaction out of commit order")

var ErrUnknownTx = errors.New("stream: unknown or completed transaction")

var ErrSealed = errors.New("stream: transaction or group already sealed")

var ErrUnknownGroup = errors.New("stream: unknown or completed group")

var ErrGroupOpen = errors.New("stream: transaction has an open group")

var ErrUnknownDelivery = errors.New("stream: unknown or already acknowledged delivery")

var ErrStatusInterval = errors.New("stream: status_interval must be below half of wal_sender_timeout")

var ErrInvalidOption = errors.New("stream: invalid option")

var ErrSpillCorrupt = errors.New("stream: corrupt spill record")

var ErrProtocol = errors.New("stream: unexpected replication message")
