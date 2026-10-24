// SPDX-License-Identifier: Apache-2.0

package lag

import "errors"

var ErrInvalidOption = errors.New("lag: invalid option")

var ErrSlotNotFound = errors.New("lag: replication slot not found")
