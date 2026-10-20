// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"

	"github.com/thnhdatle/wake/internal/adapter/kafkasink"
	"github.com/thnhdatle/wake/internal/adapter/natssink"
	"github.com/thnhdatle/wake/internal/adapter/statefile"
	"github.com/thnhdatle/wake/internal/adapter/stdoutsink"
	"github.com/thnhdatle/wake/internal/adapter/webhooksink"
	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/app/stream"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Process exit codes.
const (
	// exitFailure: a runtime failure, a failed pre-flight check or an error
	// reported by Postgres or a sink.
	exitFailure = 1
	// exitConfig: the configuration (file, env vars, flags, missing DSN or
	// secrets) is invalid, or a destructive command lacks --yes.
	exitConfig = 2
)

// Error codes produced by the command layer (docs/guides/errors.md); the
// adapters' own codes come through their ErrorCode methods.
const (
	codeConfigInvalid         = "config_invalid"
	codeConfirmationRequired  = "confirmation_required"
	codeStateMismatch         = "state_mismatch"
	codeSlotMissing           = "slot_missing"
	codeSlotInUse             = "slot_in_use"
	codeSlotInvalidated       = "slot_invalidated"
	codeStatusInterval        = "status_interval_invalid"
	codeStateCorrupt          = "state_corrupt"
	codeIdentifierInvalid     = "identifier_invalid"
	codeChunkAttempts         = "snapshot_chunk_attempts"
	codeReplicationNotStarted = "replication_not_streaming"
)

// codedError attaches an error code to an error of the command layer.
type codedError struct {
	code string
	err  error
}

func (e *codedError) Error() string     { return e.err.Error() }
func (e *codedError) Unwrap() error     { return e.err }
func (e *codedError) ErrorCode() string { return e.code }

// withCode returns err with code attached; nil stays nil.
func withCode(code string, err error) error {
	if err == nil {
		return nil
	}
	return &codedError{code: code, err: err}
}

// sentinelCodes maps the app layer's and adapters' sentinel errors without
// an ErrorCode method to their stable codes. It is a constant table.
var sentinelCodes = []struct {
	err  error
	code string
}{
	{snapshot.ErrSlotMissing, codeSlotMissing},
	{snapshot.ErrSlotInUse, codeSlotInUse},
	{snapshot.ErrSlotInvalidated, codeSlotInvalidated},
	{pipeline.ErrStateMismatch, codeStateMismatch},
	{stream.ErrStatusInterval, codeStatusInterval},
	{statefile.ErrCorrupt, codeStateCorrupt},
	{statefile.ErrUnsupportedVersion, codeStateCorrupt},
	{snapshot.ErrChunkAttempts, codeChunkAttempts},
	{config.ErrInvalid, codeConfigInvalid},
	{kafkasink.ErrInvalidConfig, codeConfigInvalid},
	{kafkasink.ErrMissingCredentials, codeConfigInvalid},
	{natssink.ErrInvalidConfig, codeConfigInvalid},
	{webhooksink.ErrInvalidConfig, codeConfigInvalid},
	{webhooksink.ErrMissingSecret, codeConfigInvalid},
	{stdoutsink.ErrInvalidConfig, codeConfigInvalid},
}

// errorCode returns the stable error code err carries: the code of the
// first error in its tree with an ErrorCode method, else the code of a known
// sentinel, else "".
func errorCode(err error) string {
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		return coded.ErrorCode()
	}
	for _, s := range sentinelCodes {
		if errors.Is(err, s.err) {
			return s.code
		}
	}
	return ""
}

// describe renders err for the terminal, prefixed with its code.
func describe(err error) string {
	if code := errorCode(err); code != "" {
		return fmt.Sprintf("[%s] %v", code, err)
	}
	return err.Error()
}

// exitCode returns the process exit code for err.
func exitCode(err error) int {
	var ce *exitError
	if errors.As(err, &ce) {
		return ce.code
	}
	if errorCode(err) == codeConfigInvalid {
		return exitConfig
	}
	return exitFailure
}
