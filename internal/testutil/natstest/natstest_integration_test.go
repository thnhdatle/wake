// SPDX-License-Identifier: Apache-2.0

//go:build integration

package natstest_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/testutil/natstest"
)

func publishOne(t *testing.T, js jetstream.JetStream, subject string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "smoke", Subjects: []string{"smoke.>"}})
	require.NoError(t, err)
	_, err = js.Publish(ctx, subject, []byte("hi"))
	require.NoError(t, err)
}

func TestNATS_TLSAndCredentials(t *testing.T) {
	t.Parallel()
	n := natstest.Start(t, natstest.WithTLS(), natstest.WithCredentials(), natstest.WithMaxPayload(4096))
	require.NotEmpty(t, n.CredsFile)
	nc := n.Connect(t)
	assert.Equal(t, int64(4096), nc.MaxPayload())
	assert.True(t, nc.TLSRequired())
	publishOne(t, n.JetStream(t), "smoke.a")

	other := n.NewCredsFile(t, "other")
	nc2, err := nats.Connect(n.URL, nats.Secure(n.TLS.ClientTLS(t)), nats.UserCredentials(other))
	require.NoError(t, err)
	nc2.Close()

	foreign := n.ForeignCredsFile(t, "intruder")
	_, err = nats.Connect(n.URL, nats.Secure(n.TLS.ClientTLS(t)), nats.UserCredentials(foreign))
	require.ErrorIs(t, err, nats.ErrAuthorization)
}

func TestNATS_Plain_OutageAndRestart(t *testing.T) {
	t.Parallel()
	n := natstest.Start(t)
	js := n.JetStream(t)
	publishOne(t, js, "smoke.a")

	n.Outage()
	_, err := nats.Connect(n.URL, nats.Timeout(time.Second))
	require.Error(t, err, "no connection during the outage")
	n.Restore()
	n.Restart(t)

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	st, err := n.JetStream(t).Stream(ctx, "smoke")
	require.NoError(t, err, "the stream survives the restart")
	assert.Equal(t, uint64(1), st.CachedInfo().State.Msgs)
}
