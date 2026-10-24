// SPDX-License-Identifier: Apache-2.0

package alerthook

import (
	"errors"
	"strconv"
)

var ErrInvalidConfig = errors.New("alerthook: invalid configuration")

var ErrEncode = errors.New("alerthook: encode alert")

type StatusError struct {
	StatusCode int
}

func (e *StatusError) Error() string {
	return "alerthook: receiver returned HTTP " + strconv.Itoa(e.StatusCode)
}
