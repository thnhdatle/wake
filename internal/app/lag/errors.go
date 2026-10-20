// SPDX-License-Identifier: Apache-2.0

package lag

import "errors"

// ErrInvalidOption reports an invalid constructor argument or option.
var ErrInvalidOption = errors.New("lag: invalid option")

// ErrSlotNotFound reports a lag poll that found no row for the slot in
// pg_replication_slots. It is reported to the Observer like any other poll
// error and does not stop the monitor.
var ErrSlotNotFound = errors.New("lag: replication slot not found")
