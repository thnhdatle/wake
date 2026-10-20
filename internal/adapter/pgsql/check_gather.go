// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// ReplicationProber runs IDENTIFY_SYSTEM on a replication connection; the
// replication adapter's connection implements it (pipeline.SlotManager).
type ReplicationProber interface {
	IdentifySystem(ctx context.Context) (pipeline.SystemInfo, error)
}

// RunCheck gathers the facts and evaluates the pre-flight checks of `wake
// check`. Failures of the checks are findings; the error reports
// only a failure to gather them over the SQL connection.
func RunCheck(ctx context.Context, cat *Catalog, prober ReplicationProber, cfg *CheckConfig) (Report, error) {
	f, err := GatherCheckFacts(ctx, cat, prober, cfg)
	if err != nil {
		return Report{}, err
	}
	return Evaluate(f, cfg), nil
}

// GatherCheckFacts reads the facts the checks evaluate. Usage is read
// before the replication probe so that the probe's own WAL sender is not
// counted.
func GatherCheckFacts(ctx context.Context, cat *Catalog, prober ReplicationProber, cfg *CheckConfig,
) (*CheckFacts, error) {
	if cat == nil || prober == nil || cfg == nil || cfg.Resolver == nil {
		return nil, fmt.Errorf("%w: check needs a catalog, a prober, a config and a resolver", ErrInvalidArgument)
	}
	f := &CheckFacts{}
	var err error
	if f.Settings, err = cat.ServerSettings(ctx); err != nil {
		return nil, err
	}
	if f.Usage, err = cat.Usage(ctx); err != nil {
		return nil, err
	}
	if f.Slot, err = cat.Slot(ctx, cfg.SlotName); err != nil {
		return nil, err
	}
	f.System, f.ReplicationErr = prober.IdentifySystem(ctx)
	err = cat.query(ctx, func(ctx context.Context, conn *pgx.Conn) error {
		return gatherPublicationFacts(ctx, conn, f, cfg)
	})
	if err != nil {
		return nil, err
	}
	return f, nil
}

func gatherPublicationFacts(ctx context.Context, conn *pgx.Conn, f *CheckFacts, cfg *CheckConfig) error {
	var err error
	if !cfg.Managed {
		f.Existing, err = inspectExisting(ctx, conn, cfg.Publication)
		return err
	}
	if err = validateSource(cfg.Source, cfg.EmitUnkeyed); err != nil {
		return err
	}
	if f.Candidates, err = candidateTables(ctx, conn); err != nil {
		return err
	}
	if f.Main, err = inspectExisting(ctx, conn, PublicationName(cfg.Source)); err != nil {
		return err
	}
	if f.Insert, err = inspectExisting(ctx, conn, InsertPublicationName(cfg.Source)); err != nil {
		return err
	}
	err = conn.QueryRow(ctx, `SELECT pg_catalog.has_database_privilege(pg_catalog.current_database(), 'CREATE')`).
		Scan(&f.CanCreate)
	if err != nil {
		return fmt.Errorf("pgsql: read database privilege: %w", err)
	}
	return nil
}
