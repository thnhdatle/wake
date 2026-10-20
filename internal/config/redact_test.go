// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/config"
)

func TestConfig_Redacted(t *testing.T) {
	t.Parallel()

	cfg, err := parse(t, `
source: {name: app}
tables: {include: ["*.*"]}
lag: {alert_webhook: "https://ops:hunter2@alerts.example/hook"}
sinks:
  - {name: n, type: nats, nats: {url: "nats://u:s3cret@a:4222,nats://b:4222", subject_prefix: cdc}}
  - {name: w, type: webhook, webhook: {url: "https://u:topsecret@h/x", secret_env: S}}
  - {name: s, type: stdout}
cache_invalidator: {nats: {url: "nats://u:cipw@c:4222"}}
`, config.Options{SkipValidate: true})
	require.NoError(t, err)

	out, err := cfg.Redacted().YAML()
	require.NoError(t, err)

	for _, secret := range []string{"hunter2", "s3cret", "topsecret", "cipw"} {
		assert.NotContains(t, string(out), secret)
	}
	assert.Contains(t, string(out), "nats://u:xxxxx@a:4222,nats://b:4222")
	assert.Equal(t, "https://u:topsecret@h/x", cfg.Sinks[1].Webhook.URL, "the original is untouched")
}
