// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/testutil/kafkatest"
	"github.com/thnhdatle/wake/internal/testutil/natstest"
	"github.com/thnhdatle/wake/internal/testutil/pgtest"
	"github.com/thnhdatle/wake/internal/testutil/redistest"
	"github.com/thnhdatle/wake/pkg/cache"
	"github.com/thnhdatle/wake/pkg/cache/cdcsource"
)

// `wake cache-invalidator` end to end, in process: CDC events written to
// Kafka (TLS + SCRAM) or NATS → the command → Redis (TLS with client
// certificates for Kafka) → the keys disappear from the L1 of three cache
// instances. The validate_dsn_env check runs against a real Postgres.

const (
	invalITWait = 2 * time.Minute
	invalITPoll = 50 * time.Millisecond
)

// invalTestLog writes log records to the test log.
func invalTestLog(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(invalTestWriter{t}, nil))
}

type invalTestWriter struct{ t *testing.T }

func (w invalTestWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// invalUserEvent returns a CDC event of public.users.
func invalUserEvent(op string, id int, before, after string) []byte {
	return fmt.Appendf(nil, `{"id":"0/16B3800-%d","source":{"db":"app","schema":"public","table":"users"},`+
		`"op":%q,"ts_ms":1760600001000,"lsn":"0/16B3800","tx_id":7813,"key":{"id":%d},"before":%s,"after":%s,`+
		`"schema_version":1}`, id, op, id, before, after)
}

// invalReaders starts three cache instances of namespace users, plus a probe
// instance used to prove that every reader's bus subscription is live.
func invalReaders(t *testing.T, client func() redis.UniversalClient) []cache.Cache[string] {
	t.Helper()
	open := func() cache.Cache[string] {
		c, err := cache.New[string]("users", cache.JSONCodec[string](), cache.WithL2(client()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		return c
	}
	probe := open()
	readers := []cache.Cache[string]{open(), open(), open()}
	require.Eventually(t, func() bool {
		invalWarm(t, readers, "probe")
		require.NoError(t, probe.Delete(t.Context(), "probe"))
		return !invalCachedAny(readers, "probe")
	}, invalITWait, invalITPoll, "readers observe a probe invalidation")
	return readers
}

func invalWarm(t *testing.T, readers []cache.Cache[string], keys ...string) {
	t.Helper()
	for _, r := range readers {
		for _, k := range keys {
			_, err := r.GetOrLoad(t.Context(), k, func(context.Context) (string, error) { return "v", nil })
			require.NoError(t, err)
		}
	}
}

func invalCachedAny(readers []cache.Cache[string], keys ...string) bool {
	for _, r := range readers {
		for _, k := range keys {
			if _, ok, _ := r.Get(context.Background(), k); ok {
				return true
			}
		}
	}
	return false
}

func invalProduce(t *testing.T, cl *kgo.Client, payloads ...[]byte) {
	t.Helper()
	for _, p := range payloads {
		rec := &kgo.Record{Topic: "cdc.public.users", Key: []byte(`{"id":1}`), Value: p}
		require.NoError(t, cl.ProduceSync(t.Context(), rec).FirstErr())
	}
}

//nolint:paralleltest // starts the largest containers (Kafka, Postgres); one at a time
func TestCacheInvalidator_IntegrationKafkaTLSRedisTLS(t *testing.T) {
	user := kafkatest.User{Name: "inval", Password: "secret42"}
	k := kafkatest.Start(t, kafkatest.WithTLSAndSCRAM(user))
	k.CreateTopic(t, "cdc.public.users", 3, nil)
	rs := redistest.StartTLS(t)
	pg, err := pgtest.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Terminate() })
	db := pg.NewDatabase(t)
	db.Exec(t, "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)")
	b := rs.Bundle()
	yaml := fmt.Sprintf(`cache_invalidator:
  source: kafka
  kafka:
    brokers: [%q]
    topic_prefix: cdc
    tls: {enabled: true, ca_file: %q, cert_file: %q, key_file: %q}
    sasl: {mechanism: SCRAM-SHA-512, username_env: WAKE_IT_KAFKA_USER, password_env: WAKE_IT_KAFKA_PASSWORD}
  redis:
    addrs: [%q]
    tls: {enabled: true, ca_file: %q, cert_file: %q, key_file: %q}
  namespace: users
  validate_dsn_env: WAKE_IT_DSN
  templates:
    public.users: ["user:{id}", "user:email:{email}"]
`, k.Brokers[0], k.TLS.CAFile, k.TLS.ClientCertFile, k.TLS.ClientKeyFile,
		rs.Addr(), b.CAFile, b.ClientCertFile, b.ClientKeyFile)
	vars := map[string]string{
		"WAKE_IT_KAFKA_USER": user.Name, "WAKE_IT_KAFKA_PASSWORD": user.Password, "WAKE_IT_DSN": db.DSN,
	}

	// Default replica identity: the e-mail template is rejected at startup.
	path := invalWriteFile(t, "wake.yaml", yaml)
	err = runCacheInvalidator(t.Context(), path, invalEnv{getenv: invalGetenv(vars), log: invalTestLog(t)})
	var ee *exitError
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, exitConfig, ee.code)
	assert.Equal(t, cdcsource.CodeTemplateRequiresFullIdentity, errorCode(err))
	assert.Contains(t, err.Error(), "table public.users: templates reference non-key columns [email]")

	db.Exec(t, "ALTER TABLE users REPLICA IDENTITY FULL")
	readers := invalReaders(t, func() redis.UniversalClient { return rs.Client(t) })
	invalWarm(t, readers, "user:1", "user:email:a@example.com", "user:email:b@example.com", "user:2")
	logs := &invalLogBuffer{}
	addr, stop := invalStart(t, yaml, vars, logs)
	cl := k.Client(t)

	invalProduce(t, cl,
		invalUserEvent("u", 1, `{"id":1,"email":"a@example.com"}`, `{"id":1,"email":"b@example.com"}`),
		nil, // a tombstone is skipped
		invalUserEvent("d", 2, `{"id":2,"email":"c@example.com"}`, "null"))

	require.Eventually(t, func() bool {
		return !invalCachedAny(readers, "user:1", "user:email:a@example.com", "user:email:b@example.com", "user:2")
	}, invalITWait, invalITPoll, "keys gone from every reader")
	require.Eventually(t, func() bool {
		status, _ := invalReadyz(t, addr)
		return status == http.StatusOK
	}, invalITWait, invalITPoll, "ready")

	// A before image without the e-mail (the identity was changed back):
	// processing continues and /readyz reports the condition.
	invalProduce(t, cl, invalUserEvent("u", 3, `{"id":3}`, `{"id":3,"email":"d@example.com"}`))
	require.Eventually(t, func() bool {
		status, failing := invalReadyz(t, addr)
		return status == http.StatusServiceUnavailable &&
			failing[invalCheckTemplates] == cdcsource.CodeTemplateRequiresFullIdentity
	}, invalITWait, invalITPoll, "not ready: template_requires_full_identity")
	require.NoError(t, stop())
	assert.Equal(t, 1, bytes.Count([]byte(logs.String()), []byte(`"msg":"old cache keys of updates and deletes`)))
}

//nolint:paralleltest // see TestCacheInvalidator_IntegrationKafkaTLSRedisTLS
func TestCacheInvalidator_IntegrationCatalogChecks(t *testing.T) {
	pg, err := pgtest.Start(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Terminate() })
	db := pg.NewDatabase(t)
	db.Exec(t, `CREATE SCHEMA app;
		CREATE TABLE app.accounts (id bigint, tenant text NOT NULL, email text,
			upper_email text GENERATED ALWAYS AS (upper(email)) STORED, CONSTRAINT accounts_pk PRIMARY KEY (id));
		CREATE UNIQUE INDEX accounts_tenant ON app.accounts (tenant);
		ALTER TABLE app.accounts REPLICA IDENTITY USING INDEX accounts_tenant;
		CREATE TABLE app.full_t (id int PRIMARY KEY, v text);
		ALTER TABLE app.full_t REPLICA IDENTITY FULL;`)
	m, err := cdcsource.NewMapping(map[string][]string{
		// Event names come through tables.name_map: public.accts is app.accounts.
		"public.accts":  {"acct:{tenant}", "acct:{id}", "acct:upper:{upper_email}"},
		"public.full_t": {"f:{v}"},
		"public.gone":   {"g:{id}"},
	})
	require.NoError(t, err)
	cfg := config.Default()
	cfg.Tables.NameMap = map[string]string{"app.accounts": "public.accts", "app.full_t": "public.full_t"}
	env := invalEnv{getenv: invalGetenv(map[string]string{config.DefaultDSNEnv: db.DSN}), log: invalTestLog(t)}

	err = checkInvalidatorTemplates(t.Context(), &cfg, m, env)

	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "table public.accts: templates reference non-key columns [id]",
		"USING INDEX: the index columns are the key")
	assert.Contains(t, msg, "table public.accts has no published column [upper_email]", "generated columns")
	assert.Contains(t, msg, "table public.gone not found")
	assert.NotContains(t, msg, "full_t", "FULL identity accepts every column")
}

func TestCacheInvalidator_IntegrationNATS(t *testing.T) {
	t.Parallel()
	n := natstest.Start(t)
	rs := redistest.StartSingle(t)
	js := n.JetStream(t)
	_, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "wake_app", Subjects: []string{"cdc.>"}})
	require.NoError(t, err)
	readers := invalReaders(t, func() redis.UniversalClient { return rs.Client(t) })
	invalWarm(t, readers, "user:1", "user:2")
	addr, stop := invalStart(t, fmt.Sprintf(`cache_invalidator:
  source: nats
  nats:
    url: %q
    subject_prefix: cdc
    stream: wake_app
  redis:
    addrs: [%q]
  namespace: users
  validate_dsn_env: ""
  templates:
    public.users: ["user:{id}"]
`, n.URL, rs.Addr()), nil, &invalLogBuffer{})

	for i, p := range [][]byte{
		invalUserEvent("c", 1, "null", `{"id":1,"email":"a@example.com"}`),
		invalUserEvent("d", 2, `{"id":2}`, "null"),
	} {
		_, perr := js.Publish(t.Context(), "cdc.public.users", p, jetstream.WithMsgID(fmt.Sprintf("0/1-%d", i)))
		require.NoError(t, perr)
	}

	require.Eventually(t, func() bool { return !invalCachedAny(readers, "user:1", "user:2") },
		invalITWait, invalITPoll, "keys gone from every reader")
	require.Eventually(t, func() bool {
		status, _ := invalReadyz(t, addr)
		return status == http.StatusOK
	}, invalITWait, invalITPoll, "ready")
	require.NoError(t, stop())
}
