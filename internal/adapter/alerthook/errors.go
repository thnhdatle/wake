// SPDX-License-Identifier: Apache-2.0

package alerthook

import (
	"errors"
	"strconv"
)

// ErrInvalidConfig reports a URL or option New cannot use.
var ErrInvalidConfig = errors.New("alerthook: invalid configuration")

// ErrEncode reports an alert that cannot be encoded as JSON (a NaN or
// infinite value).
var ErrEncode = errors.New("alerthook: encode alert")

// StatusError reports a non-2xx response of the alert receiver.
type StatusError struct {
	StatusCode int
}

// Error implements error.
func (e *StatusError) Error() string {
	return "alerthook: receiver returned HTTP " + strconv.Itoa(e.StatusCode)
}
