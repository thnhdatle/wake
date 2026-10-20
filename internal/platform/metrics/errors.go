// SPDX-License-Identifier: Apache-2.0

package metrics

import "errors"

// ErrNilRegisterer reports a constructor called without a registerer.
var ErrNilRegisterer = errors.New("metrics: nil registerer")
