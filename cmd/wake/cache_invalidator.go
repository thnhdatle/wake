// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/thnhdatle/wake/internal/adapter/httpadmin"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/platform/buildinfo"
	"github.com/thnhdatle/wake/internal/platform/metrics"
	"github.com/thnhdatle/wake/pkg/cache"
	"github.com/thnhdatle/wake/pkg/cache/cdcsource"
)

// Readiness checks and their error codes (docs/guides/errors.md) of `wake
// cache-invalidator`.
const (
	invalCheckRedis     = "redis"
	invalCheckSource    = "source"
	invalCheckTemplates = "templates"

	codeInvalRedisUnavailable  = "redis_unavailable"
	codeInvalSourceUnavailable = "source_unavailable"
)

// Timeouts of the cache invalidator's external calls.
const (
	invalCatalogTimeout = 30 * time.Second
	invalRedisDial      = 5 * time.Second
	invalRedisIO        = 3 * time.Second
)

// invalTableQuery reads what the validate_dsn_env check needs about one
// table: replica identity, published (non-generated) columns and the replica
// identity key columns (primary key, or the REPLICA IDENTITY USING INDEX
// index).
const invalTableQuery = `
SELECT c.relreplident::text,
       ARRAY(SELECT a.attname::text FROM pg_attribute a
             WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
             ORDER BY a.attnum),
       ARRAY(SELECT a.attname::text FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
             WHERE i.indrelid = c.oid
               AND ((c.relreplident = 'd' AND i.indisprimary) OR (c.relreplident = 'i' AND i.indisreplident))
             ORDER BY a.attnum)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')`

// invalEnv holds the process dependencies of the command; tests replace them.
type invalEnv struct {
	getenv func(string) string
	log    *slog.Logger
	// listener, when set, serves the admin endpoints instead of listening on
	// cache_invalidator.admin_listen.
	listener net.Listener
}

// invalCodedError attaches a stable error code to an error.
type invalCodedError struct {
	code string
	err  error
}

func (e *invalCodedError) Error() string     { return e.err.Error() }
func (e *invalCodedError) Unwrap() error     { return e.err }
func (e *invalCodedError) ErrorCode() string { return e.code }

// invalConfigError marks err as a configuration error: exit code 2 and
// the cdcsource code when there is one, else config_invalid.
func invalConfigError(err error) error {
	code := cdcsource.ErrorCode(err)
	if code == "" || code == cdcsource.CodeInvalidConfig {
		code = codeConfigInvalid
	}
	return &exitError{code: exitConfig, err: &invalCodedError{code: code, err: err}}
}

func newCacheInvalidatorCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "cache-invalidator",
		Short: "Run the CDC-driven cache invalidator",
		Long: "Consume Wake's change events from Kafka or NATS and delete the cache keys rendered from\n" +
			"cache_invalidator.templates in every pkg/cache instance of cache_invalidator.namespace.\n" +
			"Serves /healthz, /readyz and /metrics on cache_invalidator.admin_listen.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCacheInvalidator(cmd.Context(), *cfgPath, invalEnv{getenv: os.Getenv, log: newLogger()})
		},
	}
}

// loadInvalidatorConfig loads the configuration, checks the
// cache_invalidator section only (the CDC sections may be absent) and
// parses the templates. Every failure is a configuration error.
func loadInvalidatorConfig(cfgPath string) (config.Config, *cdcsource.Mapping, error) {
	cfg, err := loadConfig(cfgPath, true)
	if err != nil {
		return config.Config{}, nil, err
	}
	if verr := cfg.ValidateCacheInvalidator(); verr != nil {
		return config.Config{}, nil, invalConfigError(verr)
	}
	m, err := cdcsource.NewMapping(cfg.CacheInvalidator.Templates)
	if err != nil {
		return config.Config{}, nil, invalConfigError(fmt.Errorf("cache_invalidator.templates: %w", err))
	}
	return cfg, m, nil
}

// invalRuntime is the wired invalidator.
type invalRuntime struct {
	rdb     redis.UniversalClient
	deleter cache.Cache[[]byte]
	inv     *cdcsource.Invalidator
	admin   *httpadmin.Server
}

func (r *invalRuntime) close() {
	if r.deleter != nil {
		_ = r.deleter.Close() // only stops the bus subscriber
	}
	if r.rdb != nil {
		_ = r.rdb.Close()
	}
}

// runCacheInvalidator runs `wake cache-invalidator` until ctx ends.
func runCacheInvalidator(ctx context.Context, cfgPath string, env invalEnv) error {
	cfg, mapping, err := loadInvalidatorConfig(cfgPath)
	if err != nil {
		return err
	}
	if err = checkInvalidatorTemplates(ctx, &cfg, mapping, env); err != nil {
		return err
	}
	rt := &invalRuntime{}
	defer rt.close()
	// cache.New takes no context: its bus subscriber lives until Close.
	if err = rt.build(&cfg.CacheInvalidator, mapping, env); err != nil { //nolint:contextcheck // see above
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if env.listener != nil {
			return rt.admin.Serve(gctx, env.listener)
		}
		return rt.admin.Run(gctx)
	})
	g.Go(func() error { return rt.inv.Run(gctx) })
	ci := &cfg.CacheInvalidator
	env.log.Info("cache invalidator started", "component", "main", "provider", string(ci.Source),
		"count", len(mapping.Tables()))
	err = g.Wait()
	env.log.Info("cache invalidator stopped", "component", "main")
	return err
}

// build connects nothing yet: clients connect lazily, so the invalidator
// starts (degraded, not ready) while Redis or the broker is unreachable.
func (r *invalRuntime) build(ci *config.CacheInvalidator, m *cdcsource.Mapping, env invalEnv) error {
	opts, err := invalidatorRedisOptions(&ci.Redis, env.getenv)
	if err != nil {
		return invalConfigError(err)
	}
	r.rdb = redis.NewUniversalClient(opts)
	src, err := newInvalidatorSource(ci, m, env)
	if err != nil {
		return invalConfigError(err)
	}
	reg, err := metrics.NewRegistry(buildinfo.Get())
	if err != nil {
		return fmt.Errorf("metrics registry: %w", err)
	}
	r.deleter, err = cache.New[[]byte](ci.Namespace, invalBytesCodec{}, cache.WithL2(r.rdb),
		cache.WithInvalidationBus(ci.BusChannel), cache.WithMetrics(reg))
	if err != nil {
		return invalConfigError(err)
	}
	r.inv, err = cdcsource.New(src, m, r.deleter, cdcsource.WithLogger(env.log), cdcsource.WithMetrics(reg),
		cdcsource.WithInvalidateOnSnapshot(ci.InvalidateOnSnapshot))
	if err != nil {
		return invalConfigError(err)
	}
	r.admin, err = httpadmin.New(ci.AdminListen, buildinfo.Get(), env.log, httpadmin.WithRegistry(reg, reg),
		httpadmin.WithReadiness(invalidatorReadiness(r.rdb, r.inv)))
	if err != nil {
		return invalConfigError(err)
	}
	return nil
}

// invalBytesCodec stores raw bytes; the invalidator only deletes, so its
// cache never encodes a value.
type invalBytesCodec struct{}

func (invalBytesCodec) Marshal(v []byte) ([]byte, error)      { return v, nil }
func (invalBytesCodec) Unmarshal(data []byte) ([]byte, error) { return data, nil }

// invalidatorReadiness reports Redis, the event source and the
// template_requires_full_identity condition.
func invalidatorReadiness(rdb redis.UniversalClient, inv *cdcsource.Invalidator) httpadmin.ReadinessFunc {
	return func(ctx context.Context) []httpadmin.Check {
		redisCheck := httpadmin.Check{Name: invalCheckRedis, OK: true}
		if err := rdb.Ping(ctx).Err(); err != nil {
			redisCheck = invalFailedCheck(invalCheckRedis, codeInvalRedisUnavailable, err)
		}
		var identity, other []error
		for _, err := range invalSplit(inv.Ready(ctx)) {
			if errors.Is(err, cdcsource.ErrRequiresFullIdentity) {
				identity = append(identity, err)
			} else {
				other = append(other, err)
			}
		}
		sourceCheck := httpadmin.Check{Name: invalCheckSource, OK: true}
		if len(other) > 0 {
			sourceCheck = invalFailedCheck(invalCheckSource, codeInvalSourceUnavailable, errors.Join(other...))
		}
		tplCheck := httpadmin.Check{Name: invalCheckTemplates, OK: true}
		if len(identity) > 0 {
			tplCheck = invalFailedCheck(invalCheckTemplates, cdcsource.CodeTemplateRequiresFullIdentity,
				errors.Join(identity...))
		}
		return []httpadmin.Check{redisCheck, sourceCheck, tplCheck}
	}
}

func invalFailedCheck(name, code string, err error) httpadmin.Check {
	return httpadmin.Check{Name: name, Code: code, Reason: strings.ReplaceAll(err.Error(), "\n", "; ")}
}

// invalSplit returns the errors of a joined error, err itself otherwise.
func invalSplit(err error) []error {
	if err == nil {
		return nil
	}
	if j, ok := err.(interface{ Unwrap() []error }); ok { //nolint:errorlint // only the top-level join is split
		return j.Unwrap()
	}
	return []error{err}
}

// invalidatorRedisOptions builds the go-redis options of
// cache_invalidator.redis: several addresses select Cluster and master_name
// selects Sentinel (redis.NewUniversalClient); the password comes from the
// password_env variable.
func invalidatorRedisOptions(r *config.Redis, getenv func(string) string) (*redis.UniversalOptions, error) {
	opts := &redis.UniversalOptions{
		Addrs:        r.Addrs,
		MasterName:   r.MasterName,
		DialTimeout:  invalRedisDial,
		ReadTimeout:  invalRedisIO,
		WriteTimeout: invalRedisIO,
	}
	if r.PasswordEnv != "" {
		opts.Password = getenv(r.PasswordEnv)
		if opts.Password == "" {
			return nil, fmt.Errorf("cache_invalidator.redis.password_env: environment variable %s is empty", r.PasswordEnv)
		}
	}
	if r.TLS.Enabled {
		t, err := loadInvalidatorTLS(r.TLS, "cache_invalidator.redis.tls")
		if err != nil {
			return nil, err
		}
		opts.TLSConfig = t
	}
	return opts, nil
}

// loadInvalidatorTLS builds a client TLS configuration from PEM files:
// ca_file replaces the system roots, cert_file and key_file present a client
// certificate.
func loadInvalidatorTLS(c config.TLS, key string) (*tls.Config, error) {
	out := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%s.ca_file: %w", key, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s.ca_file %s holds no PEM certificate", key, c.CAFile)
		}
		out.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%s cert_file/key_file: %w", key, err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	return out, nil
}

// newInvalidatorSource builds the Kafka or NATS source of the section.
func newInvalidatorSource(ci *config.CacheInvalidator, m *cdcsource.Mapping, env invalEnv) (cdcsource.Source, error) {
	switch ci.Source {
	case config.SourceNATS:
		n := &ci.NATS
		nc := cdcsource.NATSConfig{
			URL: n.URL, SubjectPrefix: n.SubjectPrefix, Stream: n.Stream, Durable: n.Durable,
			CredentialsFile: n.CredentialsFile,
		}
		if n.TLS.Enabled {
			t, err := loadInvalidatorTLS(n.TLS, "cache_invalidator.nats.tls")
			if err != nil {
				return nil, err
			}
			nc.TLS = t
		}
		return cdcsource.NewNATSSource(nc, m, cdcsource.WithSourceLogger(env.log))
	case config.SourceKafka:
	}
	k := &ci.Kafka
	kc := cdcsource.KafkaConfig{Brokers: k.Brokers, TopicPrefix: k.TopicPrefix, Group: k.Group}
	if k.TLS.Enabled {
		t, err := loadInvalidatorTLS(k.TLS, "cache_invalidator.kafka.tls")
		if err != nil {
			return nil, err
		}
		kc.TLS = t
	}
	if k.SASL.Mechanism != config.SASLNone {
		kc.SASL = cdcsource.SASL{
			Mechanism: string(k.SASL.Mechanism),
			User:      env.getenv(k.SASL.UsernameEnv),
			Password:  env.getenv(k.SASL.PasswordEnv),
		}
		if kc.SASL.User == "" || kc.SASL.Password == "" {
			return nil, fmt.Errorf("cache_invalidator.kafka.sasl: environment variables %s and %s must be set",
				k.SASL.UsernameEnv, k.SASL.PasswordEnv)
		}
	}
	return cdcsource.NewKafkaSource(kc, m, cdcsource.WithSourceLogger(env.log))
}

// checkInvalidatorTemplates is the validate_dsn_env check: with
// the DSN from that environment variable it rejects templates that reference
// non-key columns of a table whose replica identity is not FULL, columns a
// table does not publish and tables that do not exist. An empty
// validate_dsn_env disables it; an unset variable skips it with a warning
// (the runtime check still applies). A rejection is a configuration error
// (exit 2); an unreachable database is a runtime failure.
func checkInvalidatorTemplates(ctx context.Context, cfg *config.Config, m *cdcsource.Mapping, env invalEnv) error {
	name := cfg.CacheInvalidator.ValidateDSNEnv
	if name == "" {
		return nil
	}
	dsn := env.getenv(name)
	if dsn == "" {
		env.log.Warn("validate_dsn_env names an empty environment variable: replica identity is checked "+
			"at runtime only", "component", "main", "status", name)
		return nil
	}
	tables, err := readInvalidatorTables(ctx, dsn, &cfg.Tables, m.Tables())
	if err != nil {
		return fmt.Errorf("cache_invalidator.validate_dsn_env %s: %w", name, err)
	}
	if err := m.CheckReplicaIdentity(tables); err != nil {
		return invalConfigError(fmt.Errorf("cache_invalidator.templates: %w", err))
	}
	return nil
}

// readInvalidatorTables reads the metadata of the mapped tables, named as in
// the events; tables.name_map is applied in reverse to find the Postgres
// table. Missing tables are left out (CheckReplicaIdentity reports them).
func readInvalidatorTables(ctx context.Context, dsn string, tables *config.Tables, names []string,
) ([]cdcsource.TableInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, invalCatalogTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	pgName := make(map[string]string, len(tables.NameMap))
	for from, to := range tables.NameMap {
		pgName[to] = from
	}
	infos := make([]cdcsource.TableInfo, 0, len(names))
	for _, name := range names {
		qualified := name
		if from, ok := pgName[name]; ok {
			qualified = from
		}
		schema, table, _ := config.SplitQualified(qualified)
		var ident string
		var cols, keys []string
		err := conn.QueryRow(ctx, invalTableQuery, schema, table).Scan(&ident, &cols, &keys)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read table %s: %w", qualified, err)
		}
		evSchema, evTable, _ := config.SplitQualified(name)
		infos = append(infos, cdcsource.TableInfo{
			Schema: evSchema, Table: evTable, Columns: cols, IdentityColumns: keys, IdentityFull: ident == "f",
		})
	}
	return infos, nil
}
