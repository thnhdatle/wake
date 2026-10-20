// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/wake/internal/adapter/pgrepl"
	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	// appName is the application_name of Wake's connections.
	appName = "wake"
	// extraConns are the pool connections beyond snapshot.parallel_tables:
	// table snapshot chunk reads, watermark and heartbeat emission, the lag
	// monitor and the table refresh, plus one spare (pgsql.WithMaxConns).
	extraConns = 4
	// closeTimeout bounds closing the source connections.
	closeTimeout = 10 * time.Second
)

// source holds the connections to the source database: the bounded SQL
// pool with its catalog and publication management, and the replication
// connection used from slot creation through START_REPLICATION.
type source struct {
	pool    *pgsql.Pool
	catalog *pgsql.Catalog
	pubs    *pgsql.Publications
	conn    *pgrepl.Conn
	log     *slog.Logger
}

// sourceDSN reads the DSN from the environment variable source.dsn_env.
func sourceDSN(cfg *config.Config) (string, error) {
	dsn := os.Getenv(cfg.Source.DSNEnv)
	if dsn == "" {
		return "", &exitError{code: exitConfig, err: withCode(codeConfigInvalid,
			fmt.Errorf("source.dsn_env: environment variable %s is not set", cfg.Source.DSNEnv))}
	}
	return dsn, nil
}

// openSource builds the source adapters. Nothing connects yet; ctx bounds
// only the cleanup of a failed construction.
func openSource(ctx context.Context, cfg *config.Config, dsn string, log *slog.Logger) (*source, error) {
	configErr := func(err error) error {
		return &exitError{code: exitConfig, err: withCode(codeConfigInvalid, err)}
	}
	pool, err := pgsql.NewPool(dsn, pgsql.WithMaxConns(cfg.Snapshot.ParallelTables+extraConns),
		pgsql.WithApplicationName(appName))
	if err != nil {
		return nil, configErr(err)
	}
	s := &source{pool: pool, log: log}
	if s.catalog, err = pgsql.NewCatalog(pool, pgsql.WithLogger(log)); err != nil {
		return nil, errors.Join(err, s.close(ctx))
	}
	if s.pubs, err = pgsql.NewPublications(pool, pgsql.WithLogger(log)); err != nil {
		return nil, errors.Join(err, s.close(ctx))
	}
	conn, err := pgrepl.New(dsn, pgrepl.WithLogger(log), pgrepl.WithRuntimeParam("application_name", appName))
	if err != nil {
		return nil, errors.Join(configErr(err), s.close(ctx))
	}
	s.conn = conn
	return s, nil
}

// close ends the replication connection, then the pool, within
// closeTimeout even when ctx is already canceled. The caller must have
// stopped every Receive first.
func (s *source) close(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	var err error
	if s.conn != nil {
		err = s.conn.Close(ctx)
	}
	s.pool.Close(ctx)
	return err
}

// newResolver builds the table resolver from the configuration: the
// tables.include/exclude globs, tables.name_map and the keyless rule
// (pipeline.TablePolicy), plus the Kafka topic length limit.
func newResolver(cfg *config.Config) (*topicResolver, error) {
	policy, err := pipeline.NewTablePolicy(pipeline.TablePolicyConfig{
		DB: cfg.Source.Name, NameMap: cfg.Tables.NameMap, EmitUnkeyed: cfg.Tables.EmitUnkeyed,
		Include: cfg.Tables.Include, Exclude: cfg.Tables.Exclude,
	})
	if err != nil {
		return nil, withCode(codeConfigInvalid, err)
	}
	r := &topicResolver{policy: policy}
	for i := range cfg.Sinks {
		sc := &cfg.Sinks[i]
		if sc.Type != config.SinkKafka || sc.Kafka == nil {
			continue
		}
		f, err := pipeline.NewTableFilter(sc.Tables)
		if err != nil {
			return nil, withCode(codeConfigInvalid, fmt.Errorf("sinks[%d].tables: %w", i, err))
		}
		r.topics = append(r.topics, kafkaTopics{prefix: sc.Kafka.TopicPrefix, filter: f})
	}
	return r, nil
}

// kafkaTopics is one Kafka sink's topic prefix and table globs.
type kafkaTopics struct {
	prefix string
	filter pipeline.TableFilter
}

// topicResolver is the TablePolicy plus the topic name rule: a table
// whose topic `<prefix>.<schema>.<table>` would exceed 249 characters on a
// Kafka sink that receives it is skipped like an unmapped invalid identifier
// (identifier_invalid), so startup and `wake check` reject it and a table
// detected at runtime is skipped until mapped.
type topicResolver struct {
	policy *pipeline.TablePolicy
	topics []kafkaTopics
}

// Resolve implements pipeline.TableResolver.
func (r *topicResolver) Resolve(t pipeline.TableInfo) pipeline.TableDecision {
	d := r.policy.Resolve(t)
	if !d.Capture {
		return d
	}
	mapped := d.Source.QualifiedName()
	for _, k := range r.topics {
		if !k.filter.Match(mapped) {
			continue
		}
		if _, err := config.TopicName(k.prefix, d.Source.Schema, d.Source.Table); err != nil {
			return pipeline.TableDecision{Skip: pipeline.SkipIdentifierInvalid}
		}
	}
	return d
}

// liveResolver is the resolver every component shares; SIGHUP swaps in the
// one built from the reloaded tables section.
type liveResolver struct {
	cur atomic.Pointer[topicResolver]
}

func newLiveResolver(r *topicResolver) *liveResolver {
	l := &liveResolver{}
	l.cur.Store(r)
	return l
}

// Resolve implements pipeline.TableResolver.
func (l *liveResolver) Resolve(t pipeline.TableInfo) pipeline.TableDecision {
	return l.cur.Load().Resolve(t)
}

func (l *liveResolver) swap(r *topicResolver) { l.cur.Store(r) }

// checkConfig is the configuration of the pre-flight checks.
func checkConfig(cfg *config.Config, r pipeline.TableResolver) *pgsql.CheckConfig {
	return &pgsql.CheckConfig{
		SlotName:    slotName(cfg),
		Managed:     cfg.Source.Publication.Mode == config.PublicationManaged,
		Source:      cfg.Source.Name,
		Publication: cfg.Source.Publication.Name,
		EmitUnkeyed: cfg.Tables.EmitUnkeyed,
		SkipUnkeyed: cfg.Tables.SkipUnkeyed,
		Resolver:    r,
	}
}

// slotName is the replication slot wake_<source.name>.
func slotName(cfg *config.Config) string { return "wake_" + cfg.Source.Name }
