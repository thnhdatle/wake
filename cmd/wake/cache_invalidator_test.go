// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/pkg/cache/cdcsource"
)

func invalClosedAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func invalWriteFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func invalKafkaYAML(broker, redisAddr string) string {
	return fmt.Sprintf(`cache_invalidator:
  source: kafka
  kafka:
    brokers: [%q]
    topic_prefix: cdc
  redis:
    addrs: [%q]
  namespace: users
  templates:
    public.users: ["user:{id}", "user:email:{email}"]
`, broker, redisAddr)
}

func invalNATSYAML(url, redisAddr string) string {
	return fmt.Sprintf(`cache_invalidator:
  source: nats
  nats:
    url: %q
    subject_prefix: cdc
    stream: wake_app
  redis:
    addrs: [%q]
  namespace: users
  templates:
    public.users: ["user:{id}"]
`, url, redisAddr)
}

func invalGetenv(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

type invalLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *invalLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *invalLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestCacheInvalidator_ConfigErrors(t *testing.T) {
	t.Parallel()
	notPEM := invalWriteFile(t, "ca.pem", "not a certificate")
	broker, redisAddr := "127.0.0.1:9092", "127.0.0.1:6379"
	tests := []struct {
		name string
		yaml string
		code string
		want string
	}{
		{
			"missing namespace", strings.Replace(invalKafkaYAML(broker, redisAddr), "  namespace: users\n", "", 1),
			"config_invalid", "cache_invalidator.namespace",
		},
		{
			"template the parser rejects", strings.Replace(invalKafkaYAML(broker, redisAddr), "{id}", "{ id }", 1),
			cdcsource.CodeTemplateInvalid, "white space",
		},
		{
			"redis tls ca not pem", strings.Replace(invalKafkaYAML(broker, redisAddr), "    addrs:",
				"    tls: {enabled: true, ca_file: "+notPEM+"}\n    addrs:", 1),
			"config_invalid", "cache_invalidator.redis.tls.ca_file",
		},
		{
			"kafka sasl env unset", strings.Replace(invalKafkaYAML(broker, redisAddr), "    topic_prefix: cdc\n",
				"    topic_prefix: cdc\n    sasl: {mechanism: SCRAM-SHA-256, username_env: WAKE_T_U, password_env: WAKE_T_P}\n", 1),
			"config_invalid", "WAKE_T_U and WAKE_T_P must be set",
		},
		{
			"kafka tls ca missing", strings.Replace(invalKafkaYAML(broker, redisAddr), "    topic_prefix: cdc\n",
				"    topic_prefix: cdc\n    tls: {enabled: true, ca_file: /nonexistent/ca.pem}\n", 1),
			"config_invalid", "cache_invalidator.kafka.tls.ca_file",
		},
		{
			"nats tls client pair missing", strings.Replace(invalNATSYAML("nats://127.0.0.1:4222", redisAddr),
				"    stream: wake_app\n",
				"    stream: wake_app\n    tls: {enabled: true, cert_file: /nx/c.pem, key_file: /nx/k.pem}\n", 1),
			"config_invalid", "cache_invalidator.nats.tls cert_file/key_file",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := invalWriteFile(t, "wake.yaml", tc.yaml)

			err := runCacheInvalidator(t.Context(), path, invalEnv{getenv: invalGetenv(nil), log: slog.New(slog.DiscardHandler)})

			var ee *exitError
			require.ErrorAs(t, err, &ee)
			assert.Equal(t, exitConfig, ee.code)
			assert.Contains(t, err.Error(), tc.want)
			if tc.code != "" {
				assert.Equal(t, tc.code, errorCode(err))
			}
		})
	}
}

func TestCacheInvalidator_RedisPasswordEnv(t *testing.T) {
	t.Parallel()
	yaml := strings.Replace(invalKafkaYAML("127.0.0.1:9092", "127.0.0.1:6379"), "    addrs:",
		"    password_env: WAKE_T_REDIS_PW\n    addrs:", 1)
	path := invalWriteFile(t, "wake.yaml", yaml)

	err := runCacheInvalidator(t.Context(), path, invalEnv{getenv: invalGetenv(nil), log: slog.New(slog.DiscardHandler)})

	var ee *exitError
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, exitConfig, ee.code)
	assert.Contains(t, err.Error(), "environment variable WAKE_T_REDIS_PW is empty")
}

func TestCacheInvalidatorCmd_MissingConfig(t *testing.T) {
	t.Parallel()
	path := "/nonexistent/wake.yaml"
	cmd := newCacheInvalidatorCmd(&path)
	cmd.SetArgs(nil)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.ExecuteContext(t.Context())

	var ee *exitError
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, exitConfig, ee.code)
}

func TestInvalidatorRedisOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		redis config.Redis
		check func(t *testing.T, c redis.UniversalClient, o *redis.UniversalOptions)
	}{
		{
			"single node",
			config.Redis{Addrs: []string{"a:6379"}},
			func(t *testing.T, c redis.UniversalClient, _ *redis.UniversalOptions) {
				t.Helper()
				assert.IsType(t, &redis.Client{}, c)
			},
		},
		{
			"cluster",
			config.Redis{Addrs: []string{"a:7000", "b:7001"}},
			func(t *testing.T, c redis.UniversalClient, _ *redis.UniversalOptions) {
				t.Helper()
				assert.IsType(t, &redis.ClusterClient{}, c)
			},
		},
		{
			"sentinel with password and TLS",
			config.Redis{
				Addrs: []string{"s1:26379", "s2:26379"}, MasterName: "mymaster", PasswordEnv: "PW",
				TLS: config.TLS{Enabled: true},
			},
			func(t *testing.T, c redis.UniversalClient, o *redis.UniversalOptions) {
				t.Helper()
				assert.IsType(t, &redis.Client{}, c, "failover client")
				assert.Equal(t, "mymaster", o.Failover().MasterName)
				assert.Equal(t, "s3cret", o.Password)
				require.NotNil(t, o.TLSConfig)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o, err := invalidatorRedisOptions(&tc.redis, invalGetenv(map[string]string{"PW": "s3cret"}))

			require.NoError(t, err)
			c := redis.NewUniversalClient(o)
			defer func() { _ = c.Close() }()
			tc.check(t, c, o)
		})
	}
}

func invalAdminGet(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+path, http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func invalStart(t *testing.T, yaml string, vars map[string]string, logs io.Writer) (addr string, stop func() error) {
	t.Helper()
	path := invalWriteFile(t, "wake.yaml", yaml)
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	env := invalEnv{getenv: invalGetenv(vars), log: slog.New(slog.NewJSONHandler(logs, nil)), listener: ln}
	go func() { done <- runCacheInvalidator(ctx, path, env) }()
	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			runErr = <-done
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return ln.Addr().String(), stop
}

func invalReadyz(t *testing.T, addr string) (int, map[string]string) {
	t.Helper()
	status, body := invalAdminGet(t, addr, "/readyz")
	failing := map[string]string{}
	if status != http.StatusServiceUnavailable {
		return status, failing
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		Checks []httpadmin.Check `json:"checks"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &env), body)
	assert.Equal(t, httpadmin.CodeNotReady, env.Error.Code)
	for _, c := range env.Checks {
		if !c.OK {
			failing[c.Name] = c.Code
		}
	}
	return status, failing
}

func TestCacheInvalidator_RunsDegraded(t *testing.T) {
	t.Parallel()
	redisAddr := invalClosedAddr(t)
	tests := []struct {
		name string
		yaml string
	}{
		{"kafka", invalKafkaYAML(invalClosedAddr(t), redisAddr)},
		{"nats", invalNATSYAML("nats://"+invalClosedAddr(t), redisAddr)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logs := &invalLogBuffer{}

			addr, stop := invalStart(t, tc.yaml, nil, logs)

			require.Eventually(t, func() bool {
				status, _ := invalAdminGet(t, addr, "/healthz")
				return status == http.StatusOK
			}, 10*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				status, failing := invalReadyz(t, addr)
				return status == http.StatusServiceUnavailable &&
					failing[invalCheckRedis] == codeInvalRedisUnavailable &&
					failing[invalCheckSource] == codeInvalSourceUnavailable
			}, 10*time.Second, 10*time.Millisecond)
			status, body := invalAdminGet(t, addr, "/metrics")
			assert.Equal(t, http.StatusOK, status)
			for _, name := range []string{"wake_build_info", "wake_cdcsource_events_total", "wake_cache_breaker_state"} {
				assert.Contains(t, body, name)
			}
			require.NoError(t, stop())
			assert.Contains(t, logs.String(), "replica identity is checked at runtime only",
				"validate_dsn_env defaults to WAKE_SOURCE_DSN, unset here")
			assert.Contains(t, logs.String(), "cache invalidator stopped")
		})
	}
}

type invalFakeSource struct{ batch [][]byte }

func (s invalFakeSource) Consume(ctx context.Context, handle cdcsource.Handler) error {
	if err := handle(ctx, s.batch); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func (invalFakeSource) Check(context.Context) error { return nil }

type invalNopDeleter struct{ calls chan []string }

func (d invalNopDeleter) Delete(_ context.Context, keys ...string) error {
	d.calls <- keys
	return nil
}

func TestInvalidatorReadiness_FullIdentity(t *testing.T) {
	t.Parallel()
	m, err := cdcsource.NewMapping(map[string][]string{"public.users": {"user:{id}", "user:email:{email}"}})
	require.NoError(t, err)
	update := []byte(`{"id":"0/16B3800-0","source":{"db":"app","schema":"public","table":"users"},"op":"u",` +
		`"ts_ms":1760600001000,"lsn":"0/16B3800","tx_id":7813,"key":{"id":42},"before":{"id":42},` +
		`"after":{"id":42,"email":"b@example.com"},"schema_version":3}`)
	del := invalNopDeleter{calls: make(chan []string, 1)}
	inv, err := cdcsource.New(invalFakeSource{batch: [][]byte{update}}, m, del)
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: invalClosedAddr(t), MaxRetries: -1})
	defer func() { _ = rdb.Close() }()
	ready := invalidatorReadiness(rdb, inv)
	checks := ready(t.Context())
	require.Len(t, checks, 3)
	assert.True(t, checks[1].OK)
	assert.True(t, checks[2].OK)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)

	go func() { done <- inv.Run(ctx) }()
	assert.Equal(t, []string{"user:42", "user:email:b@example.com"}, <-del.calls)
	checks = ready(t.Context())
	cancel()
	require.NoError(t, <-done)

	assert.Equal(t, httpadmin.Check{Name: invalCheckRedis, Code: codeInvalRedisUnavailable, Reason: checks[0].Reason},
		checks[0])
	assert.True(t, checks[1].OK, "the source is healthy")
	assert.Equal(t, invalCheckTemplates, checks[2].Name)
	assert.False(t, checks[2].OK)
	assert.Equal(t, cdcsource.CodeTemplateRequiresFullIdentity, checks[2].Code)
	assert.Contains(t, checks[2].Reason, "table public.users: templates reference non-key columns [email]")
}

func TestCheckInvalidatorTemplates_UnreachableDatabase(t *testing.T) {
	t.Parallel()
	m, err := cdcsource.NewMapping(map[string][]string{"public.users": {"user:{id}"}})
	require.NoError(t, err)
	cfg := config.Default()
	dsn := "postgres://wake:s3cret@" + invalClosedAddr(t) + "/app?connect_timeout=2"
	env := invalEnv{getenv: invalGetenv(map[string]string{config.DefaultDSNEnv: dsn}), log: slog.New(slog.DiscardHandler)}

	err = checkInvalidatorTemplates(t.Context(), &cfg, m, env)

	require.Error(t, err)
	var ee *exitError
	assert.NotErrorAs(t, err, &ee, "an unreachable database is not a configuration error")
	assert.Contains(t, err.Error(), "cache_invalidator.validate_dsn_env WAKE_SOURCE_DSN: connect")
	assert.NotContains(t, err.Error(), "s3cret")

	cfg.CacheInvalidator.ValidateDSNEnv = ""
	require.NoError(t, checkInvalidatorTemplates(t.Context(), &cfg, m, env), "disabled")
}

func TestInvalSplit(t *testing.T) {
	t.Parallel()
	a, b := errors.New("a"), errors.New("b")

	assert.Nil(t, invalSplit(nil))
	assert.Equal(t, []error{a}, invalSplit(a))
	assert.Equal(t, []error{a, b}, invalSplit(errors.Join(a, b)))
}
