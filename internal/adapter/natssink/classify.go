// SPDX-License-Identifier: Apache-2.0

package natssink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"io/fs"
	"net"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// jsErrMessageExceedsLimit is the JetStream API error code of a message
// above the stream's max_msg_size (nats-server errors.json).
const jsErrMessageExceedsLimit jetstream.ErrorCode = 10054

// verdict is the classification of a delivery error. Every
// error is retried; a fatal one also makes Health report SinkStateFatal with
// code until the cause is fixed.
type verdict struct {
	fatal  bool
	code   string // pipeline.CodeEventTooLarge or pipeline.CodeSinkFatal when fatal
	reason string // RetryReason* label
}

func fatal(reason string) verdict {
	return verdict{fatal: true, code: pipeline.CodeSinkFatal, reason: reason}
}

var authErrors = []error{
	nats.ErrAuthorization, nats.ErrAuthExpired, nats.ErrAuthRevoked, nats.ErrAccountAuthExpired,
	nats.ErrPermissionViolation, nats.ErrNkeysNotSupported, nats.ErrSecureConnRequired,
	nats.ErrSecureConnWanted, nats.ErrConnectionNotTLS, nats.ErrTLS, fs.ErrNotExist, fs.ErrPermission,
}

var networkErrors = []error{
	nats.ErrTimeout, nats.ErrDisconnected, nats.ErrConnectionClosed, nats.ErrConnectionReconnecting,
	nats.ErrReconnectBufExceeded, nats.ErrNoServers, nats.ErrStaleConnection, nats.ErrConnectionDraining,
	jetstream.ErrAsyncPublishTimeout, jetstream.ErrTooManyStalledMsgs, ErrNotConnected,
	context.DeadlineExceeded, context.Canceled, io.EOF, io.ErrUnexpectedEOF, net.ErrClosed,
}

// classify sorts err into retriable and non-retriable: message too
// large, authentication or permission failures, a missing stream with
// nats.create_stream off and other rejections by JetStream are
// non-retriable; disconnections, timeouts and no responders are retriable.
func classify(err error) verdict {
	switch {
	case errors.Is(err, ErrEventTooLarge):
		return verdict{fatal: true, code: pipeline.CodeEventTooLarge, reason: RetryReasonEventTooLarge}
	case errors.Is(err, ErrMessageTooLarge), errors.Is(err, nats.ErrMaxPayload),
		apiCode(err) == jsErrMessageExceedsLimit:
		return fatal(RetryReasonMessageTooLarge)
	case errors.Is(err, ErrUnknownStream), errors.Is(err, jetstream.ErrStreamNotFound):
		return fatal(RetryReasonUnknownStream)
	case isAuthError(err):
		return fatal(RetryReasonAuth)
	case errors.Is(err, jetstream.ErrJetStreamNotEnabled), errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return fatal(RetryReasonRejected)
	case isAny(err, networkErrors):
		return verdict{reason: RetryReasonNetwork}
	case errors.Is(err, nats.ErrNoResponders), errors.Is(err, jetstream.ErrNoStreamResponse):
		return verdict{reason: RetryReasonServer}
	}
	if api := apiError(err); api != nil {
		if api.Code == 408 || api.Code == 429 || api.Code >= 500 {
			return verdict{reason: RetryReasonServer}
		}
		return fatal(RetryReasonRejected)
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return verdict{reason: RetryReasonNetwork}
	}
	return verdict{reason: RetryReasonOther}
}

func isAny(err error, targets []error) bool {
	for _, t := range targets {
		if errors.Is(err, t) {
			return true
		}
	}
	return false
}

func isAuthError(err error) bool {
	if isAny(err, authErrors) {
		return true
	}
	var (
		unknownAuthority x509.UnknownAuthorityError
		hostname         x509.HostnameError
		invalid          x509.CertificateInvalidError
		verify           *tls.CertificateVerificationError
		alert            tls.AlertError
		header           tls.RecordHeaderError
	)
	return errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalid) ||
		errors.As(err, &verify) || errors.As(err, &alert) || errors.As(err, &header)
}

// apiError extracts the JetStream API error of a publish or stream request.
func apiError(err error) *jetstream.APIError {
	var api *jetstream.APIError
	if errors.As(err, &api) {
		return api
	}
	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		return jsErr.APIError()
	}
	return nil
}

func apiCode(err error) jetstream.ErrorCode {
	if api := apiError(err); api != nil {
		return api.ErrorCode
	}
	return 0
}
