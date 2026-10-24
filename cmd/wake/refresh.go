// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/platform/clock"
)

const refreshTimeout = time.Minute

type tableLister interface {
	captured(ctx context.Context, r pipeline.TableResolver) (names []string, skipped map[string]string, err error)
}

type tableQueue interface {
	AddTables(ctx context.Context, tables []string) error
}

type refresher struct {
	lister   tableLister
	queue    tableQueue
	resolver *liveResolver
	cfgPath  string
	cfg      *config.Config
	added    bool
	interval time.Duration
	hup      <-chan os.Signal
	clk      clock.Clock
	log      *slog.Logger
	known    map[string]bool
	skipped  map[string]bool
}

func newRefresher(s *service, w *wiring, resolver *liveResolver, known []string) *refresher {
	r := &refresher{
		queue: s.snap, resolver: resolver, cfgPath: w.cfgPath, cfg: s.cfg,
		added: s.cfg.Snapshot.AddedTables, interval: s.cfg.Tables.RefreshInterval.Std(), hup: w.hup,
		clk: s.clk, log: s.log.With("component", "refresh"),
		known: make(map[string]bool, len(known)), skipped: map[string]bool{},
	}
	if s.cfg.Source.Publication.Mode == config.PublicationExisting {
		r.lister = existingLister{pubs: s.src.pubs, name: s.cfg.Source.Publication.Name}
	} else {
		r.lister = managedLister{pubs: s.src.pubs, source: s.cfg.Source.Name, emitUnkeyed: s.cfg.Tables.EmitUnkeyed}
	}
	for _, t := range known {
		r.known[t] = true
	}
	return r
}

// Run refreshes the table list every interval and on SIGHUP until ctx ends.
func (r *refresher) Run(ctx context.Context) error {
	ticker := r.clk.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
		case <-r.hup:
			r.reload()
		}
		r.refresh(ctx)
	}
}

func (r *refresher) reload() {
	cfg, err := loadConfig(r.cfgPath, false)
	if err != nil {
		r.log.Error("SIGHUP: configuration not reloaded", "error", err)
		return
	}
	res, err := newResolver(&cfg)
	if err != nil {
		r.log.Error("SIGHUP: configuration not reloaded", "error", err)
		return
	}
	r.resolver.swap(res)
	r.log.Info("SIGHUP: reloaded tables.include, tables.exclude and tables.name_map; other changes need a restart")
}

func (r *refresher) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	names, skipped, err := r.lister.captured(ctx, r.resolver)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("table refresh failed; retrying at the next interval", "error", err)
		}
		return
	}
	r.logSkipped(skipped)
	current := make(map[string]bool, len(names))
	var added []string
	for _, n := range names {
		current[n] = true
		if !r.known[n] {
			added = append(added, n)
		}
	}
	for n := range r.known {
		if !current[n] {
			r.log.Info("table no longer captured", "table", n)
		}
	}
	r.known = current
	if len(added) == 0 {
		return
	}
	slices.Sort(added)
	r.log.Info("tables detected: "+strings.Join(added, ", "), "count", len(added))
	if !r.added {
		return
	}
	if err := r.queue.AddTables(ctx, added); err != nil {
		r.log.Error("queue snapshots of detected tables", "error", err, "table", strings.Join(added, ","))
	}
}

func (r *refresher) logSkipped(skipped map[string]string) {
	for t, reason := range skipped {
		if r.skipped[t] {
			continue
		}
		r.skipped[t] = true
		r.log.Error("table skipped: "+skipHint(reason), "table", t, "error", reason)
	}
	for t := range r.skipped {
		if _, ok := skipped[t]; !ok {
			delete(r.skipped, t)
		}
	}
}

func skipHint(reason string) string {
	switch pipeline.SkipReason(reason) {
	case pipeline.SkipKeyless:
		return "keyless; add a key or set tables.emit_unkeyed"
	case pipeline.SkipIdentifierInvalid:
		return "name is not a valid event identifier or its topic is too long; map it in tables.name_map and send SIGHUP"
	case pipeline.SkipNone, pipeline.SkipNotCaptured:
		return reason
	default:
		return reason
	}
}

type managedLister struct {
	pubs        *pgsql.Publications
	source      string
	emitUnkeyed bool
}

func (m managedLister) captured(ctx context.Context, r pipeline.TableResolver) ([]string, map[string]string, error) {
	res, err := m.pubs.EnsureManaged(ctx, pgsql.ManagedConfig{
		Source: m.source, EmitUnkeyed: m.emitUnkeyed,
		Resolver: r,
	})
	if err != nil {
		return nil, nil, err
	}
	skipped := make(map[string]string, len(res.Skipped))
	for _, s := range res.Skipped {
		skipped[s.Table.QualifiedName()] = string(s.Reason)
	}
	return syncedNames(&res), skipped, nil
}

type existingLister struct {
	pubs *pgsql.Publications
	name string
}

func (e existingLister) captured(ctx context.Context, r pipeline.TableResolver) ([]string, map[string]string, error) {
	p, err := e.pubs.InspectExisting(ctx, e.name)
	if err != nil {
		return nil, nil, err
	}
	var names []string
	skipped := map[string]string{}
	for i := range p.Tables {
		t := &p.Tables[i]
		switch d := r.Resolve(t.Info()); {
		case d.Capture:
			names = append(names, t.QualifiedName())
		case d.Skip != pipeline.SkipNotCaptured:
			skipped[t.QualifiedName()] = string(d.Skip)
		}
	}
	return names, skipped, nil
}

var (
	_ tableQueue  = (*snapshot.Snapshotter)(nil)
	_ tableLister = managedLister{}
	_ tableLister = existingLister{}
)
