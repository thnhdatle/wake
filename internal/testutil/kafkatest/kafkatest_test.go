// SPDX-License-Identifier: Apache-2.0

package kafkatest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/thnhdatle/wake/internal/testutil/tcpproxy"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStartCommand_BootstrapsSCRAMUsers(t *testing.T) {
	t.Parallel()
	cmd := startCommand([]User{{Name: "wake", Password: "pw"}})
	assert.Contains(t, cmd, "kafka-storage.sh format --ignore-formatted -t "+clusterID)
	assert.Contains(t, cmd, "--add-scram 'SCRAM-SHA-256=[name=wake,password=pw]'")
	assert.Contains(t, cmd, "--add-scram 'SCRAM-SHA-512=[name=wake,password=pw]'")
	assert.True(t, strings.HasSuffix(cmd, "exec /opt/kafka/bin/kafka-server-start.sh "+propsFile+"\n"))
	assert.NotContains(t, startCommand(nil), "--add-scram")
}

func TestProperties(t *testing.T) {
	t.Parallel()
	k := &Kafka{proxy: tcpproxy.Start(t, "")}
	plain := k.properties(options{})
	assert.Contains(t, plain, "advertised.listeners=EXTERNAL://"+k.proxy.Addr()+",BROKER://localhost:9092\n")
	assert.Contains(t, plain, "EXTERNAL:PLAINTEXT")
	assert.Contains(t, plain, "auto.create.topics.enable=false\n")
	assert.NotContains(t, plain, "ssl.")

	secure := k.properties(options{tls: true})
	assert.Contains(t, secure, "EXTERNAL:SASL_SSL")
	assert.Contains(t, secure, "sasl.enabled.mechanisms=SCRAM-SHA-256,SCRAM-SHA-512\n")
	assert.Contains(t, secure, "ssl.keystore.type=PEM\n")
	assert.Equal(t, DefaultImage, image())
}
