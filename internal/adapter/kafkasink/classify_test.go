// SPDX-License-Identifier: Apache-2.0

package kafkasink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

func TestClassify(t *testing.T) {
	t.Parallel()
	retry := func(reason string) verdict { return verdict{reason: reason} }
	tests := []struct {
		name       string
		err        error
		autoCreate bool
		want       verdict
	}{
		{
			"event too large", fmt.Errorf("%w: x", ErrEventTooLarge), false,
			verdict{fatal: true, code: pipeline.CodeEventTooLarge, reason: RetryReasonEventTooLarge},
		},
		{"topic limit", fmt.Errorf("%w: x", ErrMessageTooLarge), false, fatalVerdict(RetryReasonMessageTooLarge)},
		{"broker message too large", kerr.MessageTooLarge, false, fatalVerdict(RetryReasonMessageTooLarge)},
		{"record list too large", kerr.RecordListTooLarge, false, fatalVerdict(RetryReasonMessageTooLarge)},
		{
			"unknown topic, no auto-create", fmt.Errorf("%w: t", ErrUnknownTopic), false,
			fatalVerdict(RetryReasonUnknownTopic),
		},
		{
			"broker unknown topic, no auto-create", kerr.UnknownTopicOrPartition, false,
			fatalVerdict(RetryReasonUnknownTopic),
		},
		{"unknown topic id", kerr.UnknownTopicID, false, fatalVerdict(RetryReasonUnknownTopic)},
		{"unknown topic, auto-create", kerr.UnknownTopicOrPartition, true, retry(RetryReasonUnknownTopic)},
		{"sasl", kerr.SaslAuthenticationFailed, false, fatalVerdict(RetryReasonAuth)},
		{"topic acl", fmt.Errorf("produce: %w", kerr.TopicAuthorizationFailed), false, fatalVerdict(RetryReasonAuth)},
		{"cluster acl", kerr.ClusterAuthorizationFailed, false, fatalVerdict(RetryReasonAuth)},
		{"unknown CA", x509.UnknownAuthorityError{}, false, fatalVerdict(RetryReasonAuth)},
		{"hostname", x509.HostnameError{}, false, fatalVerdict(RetryReasonAuth)},
		{"verification", &tls.CertificateVerificationError{Err: io.EOF}, false, fatalVerdict(RetryReasonAuth)},
		{"plain broker, tls client", tls.RecordHeaderError{}, false, fatalVerdict(RetryReasonAuth)},
		{"first read eof", &kgo.ErrFirstReadEOF{}, false, fatalVerdict(RetryReasonAuth)},
		{"invalid record", kerr.InvalidRecord, false, fatalVerdict(RetryReasonRejected)},
		{"policy", kerr.PolicyViolation, false, fatalVerdict(RetryReasonRejected)},
		{"not leader", kerr.NotLeaderForPartition, false, retry(RetryReasonBroker)},
		{"not enough replicas", kerr.NotEnoughReplicas, false, retry(RetryReasonBroker)},
		{"request timed out", kerr.RequestTimedOut, false, retry(RetryReasonBroker)},
		{"record timeout", kgo.ErrRecordTimeout, false, retry(RetryReasonBroker)},
		{"record retries", kgo.ErrRecordRetries, false, retry(RetryReasonBroker)},
		{"refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, false, retry(RetryReasonNetwork)},
		{"reset", syscall.ECONNRESET, false, retry(RetryReasonNetwork)},
		{"eof", io.EOF, false, retry(RetryReasonNetwork)},
		{"deadline", context.DeadlineExceeded, false, retry(RetryReasonNetwork)},
		{"closed", kgo.ErrClientClosed, false, retry(RetryReasonNetwork)},
		{"other", errInjected, false, retry(RetryReasonOther)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, classify(tc.err, tc.autoCreate))
		})
	}
}

func TestMurmur2_MatchesKafka(t *testing.T) {
	t.Parallel()
	// Vectors from Kafka's UtilsTest.testMurmur2 (Java int results).
	tests := []struct {
		in   string
		want int32
	}{
		{"21", -973932308},
		{"foobar", -790332482},
		{"a-little-bit-long-string", -985981536},
		{"a-little-bit-longer-string", -1486304829},
		{"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971},
		{"abc", 479470107},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, int32(murmur2([]byte(tc.in))), tc.in) //nolint:gosec // G115: Java int semantics
	}
}

func TestPartitionFor(t *testing.T) {
	t.Parallel()
	key := []byte(`{"id":1}`)
	assert.Equal(t, keyPartition(key, 6), partitionFor(&kgo.Record{Key: key, Partition: hashPartition}, 6))
	assert.Equal(t, 4, partitionFor(&kgo.Record{Key: key, Partition: 4}, 6), "an explicit partition is kept")
	assert.Equal(t, keyPartition(key, 3), partitionFor(&kgo.Record{Key: key, Partition: 5}, 3),
		"an explicit partition beyond the count falls back to the hash")
	id := []byte("0/1-m0")
	r := &kgo.Record{Partition: hashPartition, Headers: []kgo.RecordHeader{{Key: HeaderEventID, Value: id}}}
	assert.Equal(t, keyPartition(id, 6), partitionFor(r, 6), "keyless records hash their id")
	assert.Equal(t, 0, keyPartition(key, 1))
	for n := 1; n < 20; n++ {
		p := keyPartition([]byte(strconv.Itoa(n)), n)
		assert.True(t, p >= 0 && p < n)
	}
	assert.NotNil(t, newPartitioner().ForTopic("t"))
}

func TestHealthTracker(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(100, 0)
	var h healthTracker
	tr := h.set(faultStall, &fault{v: verdict{reason: RetryReasonNetwork}, reason: "down"}, t0)
	assert.True(t, tr.changed)
	assert.Equal(t, pipeline.SinkStateRetrying, h.current.State)
	assert.Empty(t, h.current.Code)

	tr = h.set(faultWrite, &fault{v: fatalVerdict(RetryReasonAuth), reason: "denied"}, t0.Add(time.Second))
	assert.True(t, tr.changed)
	assert.Equal(t, pipeline.SinkHealth{
		State: pipeline.SinkStateFatal, Code: pipeline.CodeSinkFatal, Reason: "denied", Since: t0.Add(time.Second),
	}, h.current)

	tr = h.set(faultWrite, &fault{v: fatalVerdict(RetryReasonAuth), reason: "denied again"}, t0.Add(2*time.Second))
	assert.False(t, tr.changed)
	assert.Equal(t, t0.Add(time.Second), h.current.Since, "Since changes with the state only")
	assert.Equal(t, "denied again", h.current.Reason)

	assert.False(t, h.clear(t0, faultBlock).changed, "clearing an empty slot changes nothing")
	tr = h.clear(t0.Add(3*time.Second), faultWrite, faultStall)
	assert.True(t, tr.changed)
	assert.True(t, h.current.IsHealthy())
}
