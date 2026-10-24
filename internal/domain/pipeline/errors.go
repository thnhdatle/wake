// SPDX-License-Identifier: Apache-2.0

package pipeline

import "errors"

var ErrStateNotFound = errors.New("pipeline: state not found")

var ErrStateMismatch = errors.New("pipeline: state belongs to another slot or system")

var ErrBadTablePattern = errors.New("pipeline: bad table pattern")

var ErrLanesFull = errors.New("pipeline: lane scheduler full")

var ErrUnknownBatch = errors.New("pipeline: batch not in flight")

var ErrInvalidLanes = errors.New("pipeline: invalid lane configuration")

var ErrInvalidTablePolicy = errors.New("pipeline: invalid table policy")
