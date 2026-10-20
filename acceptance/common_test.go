// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/thnhdatle/wake/acceptance/internal/harness"
)

// wakeConfig builds a Wake configuration for the compose service: the
// source named name (slot wake_<name>), the included table globs, fast
// status updates and heartbeats so that WaitConfirmed converges quickly, and
// the given sinks section (YAML list items indented by two spaces).
func wakeConfig(name string, include []string, sinks string, extra string) string {
	inc := make([]string, len(include))
	for i, g := range include {
		inc[i] = fmt.Sprintf("%q", g)
	}
	return fmt.Sprintf(`source:
  name: %s
  dsn_env: WAKE_SOURCE_DSN
tables:
  include: [%s]
  refresh_interval: 5s
state:
  path: /var/lib/wake/wake-state.json
stream:
  spill_dir: /var/lib/wake/spill
  status_interval: 1s
  min_confirm_bytes: 0B
heartbeat:
  interval: 1s
admin:
  listen: 0.0.0.0:9093
sinks:
%s
%s`, name, strings.Join(inc, ", "), sinks, extra)
}

// kafkaSink is a sinks item for the compose Kafka.
func kafkaSink(prefix string) string {
	return fmt.Sprintf(`  - name: kafka-main
    type: kafka
    kafka:
      brokers: ["kafka:29092"]
      topic_prefix: %s
      auto_create_topics: true`, prefix)
}

// kafkaSinkPartitions is kafkaSink with created topics of the given
// partition count, so per-key order is checked across partitions.
func kafkaSinkPartitions(prefix string, partitions int) string {
	return kafkaSink(prefix) + fmt.Sprintf("\n      partitions: %d", partitions)
}

// webhookSink is a sinks item posting to url with the secret in secretEnv.
func webhookSink(url, secretEnv string) string {
	return fmt.Sprintf(`  - name: hook
    type: webhook
    webhook:
      url: %q
      secret_env: %s`, url, secretEnv)
}

// withTimeout returns a context bounded by d, canceled at cleanup.
func withTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// jsonEqual compares two JSON texts token by token: object member order,
// number literals (digits exactly as written) and decoded string values must
// match; insignificant whitespace and string escaping do not matter.
func jsonEqual(a, b []byte) (bool, error) {
	ta, err := jsonTokens(a)
	if err != nil {
		return false, err
	}
	tb, err := jsonTokens(b)
	if err != nil {
		return false, err
	}
	if len(ta) != len(tb) {
		return false, nil
	}
	for i := range ta {
		if ta[i] != tb[i] {
			return false, nil
		}
	}
	return true, nil
}

func jsonTokens(data []byte) ([]any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var out []any
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("json %q: %w", truncate(string(data), 80), err)
		}
		if d, ok := tok.(json.Delim); ok {
			out = append(out, string(d))
			continue
		}
		out = append(out, tok)
	}
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// failIf records a failing result with the error and stops the test.
func failIf(tb testing.TB, id string, err error, what string) {
	tb.Helper()
	if err != nil {
		harness.Failf(tb, id, "%s: %v", what, err)
	}
}

// diffCounts splits m2.Diff lines into rows missing or different at the sink
// and extra rows at the sink.
func diffCounts(diff []string) (missing, extra int) {
	for _, d := range diff {
		if strings.HasPrefix(d, "extra at sink: ") {
			extra++
		} else {
			missing++
		}
	}
	return missing, extra
}
