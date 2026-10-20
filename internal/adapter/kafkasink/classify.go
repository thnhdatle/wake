// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// verdict is the classification of a delivery error. Every
// error is retried; a fatal one also makes Health report SinkStateFatal with
// code until the cause is fixed.
type verdict struct {
	fatal  bool
	code   string // pipeline.CodeEventTooLarge or pipeline.CodeSinkFatal when fatal
	reason string // RetryReason* label
}

func fatalVerdict(reason string) verdict {
	return verdict{fatal: true, code: pipeline.CodeSinkFatal, reason: reason}
}

// authErrors are the Kafka error codes of authentication and authorization
// failures.
var authErrors = []*kerr.Error{
	kerr.SaslAuthenticationFailed,
	kerr.IllegalSaslState,
	kerr.UnsupportedSaslMechanism,
	kerr.TopicAuthorizationFailed,
	kerr.ClusterAuthorizationFailed,
	kerr.TransactionalIDAuthorizationFailed,
	kerr.DelegationTokenAuthorizationFailed,
}

// classify sorts err into retriable and non-retriable. An unknown topic
// is non-retriable only while kafka.auto_create_topics is off.
func classify(err error, autoCreate bool) verdict {
	switch {
	case errors.Is(err, ErrEventTooLarge):
		return verdict{fatal: true, code: pipeline.CodeEventTooLarge, reason: RetryReasonEventTooLarge}
	case errors.Is(err, ErrMessageTooLarge), errors.Is(err, kerr.MessageTooLarge),
		errors.Is(err, kerr.RecordListTooLarge):
		return fatalVerdict(RetryReasonMessageTooLarge)
	case errors.Is(err, ErrUnknownTopic), errors.Is(err, kerr.UnknownTopicOrPartition),
		errors.Is(err, kerr.UnknownTopicID):
		if autoCreate {
			return verdict{reason: RetryReasonUnknownTopic}
		}
		return fatalVerdict(RetryReasonUnknownTopic)
	case isAuthError(err):
		return fatalVerdict(RetryReasonAuth)
	case isNetworkError(err):
		return verdict{reason: RetryReasonNetwork}
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		if ke.Retriable {
			return verdict{reason: RetryReasonBroker}
		}
		return fatalVerdict(RetryReasonRejected)
	}
	if errors.Is(err, kgo.ErrRecordTimeout) || errors.Is(err, kgo.ErrRecordRetries) {
		return verdict{reason: RetryReasonBroker}
	}
	return verdict{reason: RetryReasonOther}
}

// isAuthError reports SASL, ACL and certificate failures. A broker that
// closes the connection right after the first request (kgo.ErrFirstReadEOF)
// almost always means a TLS or SASL mismatch.
func isAuthError(err error) bool {
	for _, ae := range authErrors {
		if errors.Is(err, ae) {
			return true
		}
	}
	var (
		unknownAuthority x509.UnknownAuthorityError
		hostname         x509.HostnameError
		invalid          x509.CertificateInvalidError
		verify           *tls.CertificateVerificationError
		alert            tls.AlertError
		header           tls.RecordHeaderError
		firstRead        *kgo.ErrFirstReadEOF
	)
	return errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalid) ||
		errors.As(err, &verify) || errors.As(err, &alert) || errors.As(err, &header) ||
		errors.As(err, &firstRead)
}

// isNetworkError reports transport failures and timeouts.
func isNetworkError(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, kgo.ErrClientClosed)
}
