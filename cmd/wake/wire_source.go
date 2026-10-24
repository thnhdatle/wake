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
	appName      = "wake"
	extraConns   = 4
	closeTimeout = 10 * time.Second
)

type source struct {
	pool    *pgsql.Pool
	catalog *pgsql.Catalog
	pubs    *pgsql.Publications
	conn    *pgrepl.Conn
	log     *slog.Logger
}

func sourceDSN(cfg *config.Config) (string, error) {
	dsn := os.Getenv(cfg.Source.DSNEnv)
	if dsn == "" {
		return "", &exitError{code: exitConfig, err: withCode(codeConfigInvalid,
			fmt.Errorf("source.dsn_env: environment variable %s is not set", cfg.Source.DSNEnv))}
	}
	return dsn, nil
}

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

type kafkaTopics struct {
	prefix string
	filter pipeline.TableFilter
}

type topicResolver struct {
	policy *pipeline.TablePolicy
	topics []kafkaTopics
}

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

type liveResolver struct {
	cur atomic.Pointer[topicResolver]
}

func newLiveResolver(r *topicResolver) *liveResolver {
	l := &liveResolver{}
	l.cur.Store(r)
	return l
}

func (l *liveResolver) Resolve(t pipeline.TableInfo) pipeline.TableDecision {
	return l.cur.Load().Resolve(t)
}

func (l *liveResolver) swap(r *topicResolver) { l.cur.Store(r) }

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

func slotName(cfg *config.Config) string { return "wake_" + cfg.Source.Name }
