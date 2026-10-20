// SPDX-License-Identifier: Apache-2.0

//go:build integration

package kafkatest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
)

func produce(t *testing.T, cl *kgo.Client, topic, value string, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(value)}).FirstErr()
}

func TestKafka_Plain_OutageAndRestart(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t)
	k.CreateTopic(t, "smoke", 2, nil)
	cl := k.Client(t)
	require.NoError(t, produce(t, cl, "smoke", "one", 30*time.Second))

	k.Outage()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	_, err := k.Admin(t).ListBrokers(ctx)
	cancel()
	require.Error(t, err, "no connection during the outage")
	k.Restore()
	k.Restart(t)
	require.NoError(t, produce(t, cl, "smoke", "two", 60*time.Second), "the client reconnects after a restart")

	recs := k.Consume(t, "smoke", 2, time.Minute)
	values := make([]string, 0, len(recs))
	for _, r := range recs {
		values = append(values, string(r.Value))
	}
	assert.Subset(t, values, []string{"one", "two"})
}

func TestKafka_TLSAndSCRAM(t *testing.T) {
	t.Parallel()
	k := kafkatest.Start(t, kafkatest.WithTLSAndSCRAM(kafkatest.User{Name: "wake", Password: "wakesecret"}))
	require.NotNil(t, k.TLS)
	k.CreateTopic(t, "secure", 1, nil)
	require.NoError(t, produce(t, k.Client(t), "secure", "hello", 30*time.Second))
	recs := k.Consume(t, "secure", 1, time.Minute)
	require.Len(t, recs, 1)
	assert.Equal(t, "hello", string(recs[0].Value))
}
