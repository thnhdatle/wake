// SPDX-License-Identifier: Apache-2.0

package pipeline

import "errors"

// ErrStateNotFound reports that no state file exists yet (StateStore.Load).
var ErrStateNotFound = errors.New("pipeline: state not found")

// ErrStateMismatch reports a state file that belongs to another slot or
// Postgres system (error code state_mismatch).
var ErrStateMismatch = errors.New("pipeline: state belongs to another slot or system")

// ErrBadTablePattern reports a malformed `tables` glob.
var ErrBadTablePattern = errors.New("pipeline: bad table pattern")

// ErrLanesFull reports that a LaneScheduler holds its capacity of queued
// records; the caller waits for a Complete before pushing again.
var ErrLanesFull = errors.New("pipeline: lane scheduler full")

// ErrUnknownBatch reports a Complete for a batch that is not in flight.
var ErrUnknownBatch = errors.New("pipeline: batch not in flight")

// ErrInvalidLanes reports a LaneScheduler configuration with a non-positive
// lane count or capacity.
var ErrInvalidLanes = errors.New("pipeline: invalid lane configuration")

// ErrInvalidTablePolicy reports a TablePolicy configuration with a
// malformed tables.name_map entry.
var ErrInvalidTablePolicy = errors.New("pipeline: invalid table policy")
