// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/app/snapshot"
	"github.com/thnhdatle/wake/internal/app/stream"
	"github.com/thnhdatle/wake/internal/config"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Names of the checks the command layer adds to pgsql.RunCheck.
const (
	checkStatusInterval = "status_interval"
	checkNameCollisions = "name_collisions"
)

// runChecks runs the pre-flight checks (pgsql.RunCheck, with the
// replication connection as the IDENTIFY_SYSTEM prober) plus the
// stream.status_interval rule. Used by startup and `wake check`.
func runChecks(ctx context.Context, src *source, cfg *config.Config, r pipeline.TableResolver) (pgsql.Report, error) {
	report, err := pgsql.RunCheck(ctx, src.catalog, src.conn, checkConfig(cfg, r))
	if err != nil {
		return pgsql.Report{}, fmt.Errorf("pre-flight checks: %w", err)
	}
	settings, err := src.catalog.ServerSettings(ctx)
	if err != nil {
		return pgsql.Report{}, fmt.Errorf("pre-flight checks: %w", err)
	}
	report.Findings = append(report.Findings, statusIntervalFinding(cfg, settings))
	return report, nil
}

// statusIntervalFinding is the startup rule for stream.status_interval: with wal_sender_timeout > 0,
// stream.status_interval must be below half of it.
func statusIntervalFinding(cfg *config.Config, s pipeline.ServerSettings) pgsql.Finding {
	interval := cfg.Stream.StatusInterval.Std()
	if err := stream.CheckStatusInterval(interval, s.WALSenderTimeout); err != nil {
		return pgsql.Finding{
			Check: checkStatusInterval, Status: pgsql.StatusFail, Code: codeStatusInterval,
			Message: fmt.Sprintf("stream.status_interval %s is not below half of wal_sender_timeout %s; lower it",
				interval, s.WALSenderTimeout),
		}
	}
	return pgsql.Finding{
		Check: checkStatusInterval, Status: pgsql.StatusOK,
		Message: fmt.Sprintf("stream.status_interval %s, wal_sender_timeout %s", interval, s.WALSenderTimeout),
	}
}

// findingsError reports failed pre-flight checks; its code is the first
// failure's.
type findingsError struct {
	failed []pgsql.Finding
}

func (e *findingsError) Error() string {
	parts := make([]string, len(e.failed))
	for i, f := range e.failed {
		parts[i] = fmt.Sprintf("%s [%s]: %s", f.Check, f.Code, f.Message)
		if len(f.Tables) > 0 {
			parts[i] += " (" + strings.Join(f.Tables, ", ") + ")"
		}
	}
	return "pre-flight checks failed: " + strings.Join(parts, "; ")
}

// ErrorCode returns the code of the first failed check.
func (e *findingsError) ErrorCode() string { return e.failed[0].Code }

// judge logs the passed and warning findings of a startup report and
// returns the failed ones as a findingsError (reported once, by the caller).
func judge(ctx context.Context, log *slog.Logger, report *pgsql.Report) error {
	var failed []pgsql.Finding
	for _, f := range report.Findings {
		level := slog.LevelDebug
		switch f.Status {
		case pgsql.StatusFail:
			failed = append(failed, f)
			continue
		case pgsql.StatusWarn:
			level = slog.LevelWarn
		case pgsql.StatusOK:
		}
		log.Log(ctx, level, "pre-flight check "+f.Check+": "+f.Message, "component", "startup",
			"status", string(f.Status), "error", f.Code, "table", strings.Join(f.Tables, ","))
	}
	if len(failed) > 0 {
		return &findingsError{failed: failed}
	}
	return nil
}

// syncResult is the outcome of the startup publication step.
type syncResult struct {
	// publications are the publication_names of START_REPLICATION.
	publications []string
	// added are the tables managed mode published at this startup that
	// were not published before (detected while Wake was stopped).
	added []string
}

// syncPublications creates or alters the managed publications (managed
// mode) or uses the existing one, before the slot is created or replication
// starts (pgoutput needs the publication to exist).
func syncPublications(ctx context.Context, src *source, cfg *config.Config, r pipeline.TableResolver,
) (syncResult, error) {
	if cfg.Source.Publication.Mode == config.PublicationExisting {
		return syncResult{publications: []string{cfg.Source.Publication.Name}}, nil
	}
	names := pgsql.ManagedPublicationNames(cfg.Source.Name, cfg.Tables.EmitUnkeyed)
	before, err := src.catalog.PublicationTables(ctx, names)
	if err != nil {
		return syncResult{}, err
	}
	res, err := src.pubs.EnsureManaged(ctx, pgsql.ManagedConfig{
		Source: cfg.Source.Name, EmitUnkeyed: cfg.Tables.EmitUnkeyed, Resolver: r,
	})
	if err != nil {
		return syncResult{}, err
	}
	known := make(map[string]bool, len(before))
	for _, t := range before {
		known[t.Schema+"."+t.Table] = true
	}
	out := syncResult{publications: res.Publications}
	for _, name := range syncedNames(&res) {
		if !known[name] {
			out.added = append(out.added, name)
		}
	}
	return out, nil
}

// syncedNames lists the tables of both managed publications.
func syncedNames(res *pgsql.SyncResult) []string {
	out := make([]string, 0, len(res.Keyed)+len(res.Unkeyed))
	for i := range res.Keyed {
		out = append(out, res.Keyed[i].QualifiedName())
	}
	for i := range res.Unkeyed {
		out = append(out, res.Unkeyed[i].QualifiedName())
	}
	return out
}

// collisionFinding rejects two captured tables that map to the same
// "<schema>.<table>": their events would share a topic and a source.
func collisionFinding(tables []snapshot.Table) pgsql.Finding {
	seen := make(map[string]string, len(tables))
	var clashes []string
	for _, t := range tables {
		mapped := t.Source.QualifiedName()
		if other, dup := seen[mapped]; dup {
			clashes = append(clashes, fmt.Sprintf("%s and %s map to %s", other, t.Name, mapped))
			continue
		}
		seen[mapped] = t.Name
	}
	if len(clashes) > 0 {
		return pgsql.Finding{
			Check: checkNameCollisions, Status: pgsql.StatusFail, Code: codeIdentifierInvalid,
			Message: "captured tables share a name: " + strings.Join(clashes, "; ") + "; fix tables.name_map",
		}
	}
	return pgsql.Finding{Check: checkNameCollisions, Status: pgsql.StatusOK, Message: "captured table names are unique"}
}
