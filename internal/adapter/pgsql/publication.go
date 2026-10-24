// SPDX-License-Identifier: Apache-2.0

package pgsql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	publicationPrefix = "wake_"
	insertSuffix      = "_insert"
	maxIdentifier     = 63
	publishAll        = "insert, update, delete, truncate"
	publishInsert     = "insert"
)

func PublicationName(source string) string { return publicationPrefix + source }

func InsertPublicationName(source string) string { return publicationPrefix + source + insertSuffix }

func ManagedPublicationNames(source string, emitUnkeyed bool) []string {
	if emitUnkeyed {
		return []string{PublicationName(source), InsertPublicationName(source)}
	}
	return []string{PublicationName(source)}
}

func validateSource(source string, emitUnkeyed bool) error {
	longest := PublicationName(source)
	if emitUnkeyed {
		longest = InsertPublicationName(source)
	}
	if source == "" || len(longest) > maxIdentifier {
		return fmt.Errorf("%w: source name %q too long for %q", ErrInvalidArgument, source, longest)
	}
	for _, ch := range source {
		if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '_' {
			return fmt.Errorf("%w: source name %q may only contain [a-z0-9_]", ErrInvalidArgument, source)
		}
	}
	return nil
}

// Publications manages the publications wake owns.
type Publications struct {
	pool *Pool
	o    options
}

// NewPublications returns a Publications that uses pool.
func NewPublications(pool *Pool, opts ...Option) (*Publications, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrInvalidArgument)
	}
	o, err := newOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Publications{pool: pool, o: o}, nil
}

type ManagedConfig struct {
	Source      string
	EmitUnkeyed bool
	Resolver    pipeline.TableResolver
}

type SkippedTable struct {
	Table  Table
	Reason pipeline.SkipReason
}

type SyncResult struct {
	Publications []string
	Keyed        []Table
	Unkeyed      []Table
	Skipped      []SkippedTable
	Changes      []string
}

type managedPlan struct {
	keyed, unkeyed []Table
	skipped        []SkippedTable
	main, insert   pubState
	canCreate      bool
}

// EnsureManaged creates or updates the managed publications to match cfg.
func (p *Publications) EnsureManaged(ctx context.Context, cfg ManagedConfig) (SyncResult, error) {
	if err := validateSource(cfg.Source, cfg.EmitUnkeyed); err != nil {
		return SyncResult{}, err
	}
	if cfg.Resolver == nil {
		return SyncResult{}, fmt.Errorf("%w: nil resolver", ErrInvalidArgument)
	}
	ctx, cancel := context.WithTimeout(ctx, p.o.queryTimeout)
	defer cancel()
	var res SyncResult
	err := p.pool.withConn(ctx, func(conn *pgx.Conn) error {
		return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			plan, err := loadManagedPlan(ctx, tx, &cfg)
			if err != nil {
				return err
			}
			if err := plan.check(cfg.EmitUnkeyed); err != nil {
				return err
			}
			res = SyncResult{
				Publications: ManagedPublicationNames(cfg.Source, cfg.EmitUnkeyed),
				Keyed:        plan.keyed, Unkeyed: plan.unkeyed, Skipped: plan.skipped,
			}
			for _, stmt := range plan.statements(cfg.EmitUnkeyed) {
				if _, err := tx.Exec(ctx, stmt.sql); err != nil {
					return mapDDLError(stmt.sql, err)
				}
				res.Changes = append(res.Changes, stmt.change)
			}
			return dropLostKeys(ctx, tx, PublicationName(cfg.Source), &res)
		})
	})
	if err != nil {
		return SyncResult{}, fmt.Errorf("pgsql: ensure managed publications: %w", err)
	}
	for _, c := range res.Changes {
		p.o.logger.LogAttrs(ctx, slog.LevelInfo, "publication changed", slog.String("component", "pgsql"),
			slog.String("operation", "publication"), slog.String("status", c))
	}
	return res, nil
}

func loadManagedPlan(ctx context.Context, tx pgx.Tx, cfg *ManagedConfig) (*managedPlan, error) {
	cands, err := candidateTables(ctx, tx)
	if err != nil {
		return nil, err
	}
	plan := &managedPlan{}
	plan.keyed, plan.unkeyed, plan.skipped = classify(cands, cfg.Resolver)
	if plan.main, err = loadPubState(ctx, tx, PublicationName(cfg.Source)); err != nil {
		return nil, err
	}
	if plan.insert, err = loadPubState(ctx, tx, InsertPublicationName(cfg.Source)); err != nil {
		return nil, err
	}
	err = tx.QueryRow(ctx, `SELECT pg_catalog.has_database_privilege(pg_catalog.current_database(), 'CREATE')`).
		Scan(&plan.canCreate)
	if err != nil {
		return nil, fmt.Errorf("pgsql: read database privilege: %w", err)
	}
	return plan, nil
}

func dropLostKeys(ctx context.Context, tx pgx.Tx, pub string, res *SyncResult) error {
	ps, err := loadPubState(ctx, tx, pub)
	if err != nil {
		return err
	}
	var lost []Table
	for _, t := range ps.members {
		if !t.HasKey {
			lost = append(lost, t)
		}
	}
	if len(lost) == 0 {
		return nil
	}
	sql := "ALTER PUBLICATION " + pgx.Identifier{pub}.Sanitize() + " DROP TABLE " + tableList(lost)
	if _, err := tx.Exec(ctx, sql); err != nil {
		return mapDDLError(sql, err)
	}
	res.Changes = append(res.Changes, "drop "+qualifiedNames(lost)+" from "+pub+" (key lost)")
	for _, t := range lost {
		res.Keyed = slices.DeleteFunc(res.Keyed, func(k Table) bool { return k.OID == t.OID })
		res.Skipped = append(res.Skipped, SkippedTable{Table: t, Reason: pipeline.SkipKeyless})
	}
	return nil
}

func classify(cands []Table, r pipeline.TableResolver) (keyed, unkeyed []Table, skipped []SkippedTable) {
	for _, t := range cands {
		d := r.Resolve(t.Info())
		switch {
		case d.Capture && t.HasKey:
			keyed = append(keyed, t)
		case d.Capture:
			unkeyed = append(unkeyed, t)
		case d.Skip == pipeline.SkipKeyless || d.Skip == pipeline.SkipIdentifierInvalid:
			skipped = append(skipped, SkippedTable{Table: t, Reason: d.Skip})
		}
	}
	return keyed, unkeyed, skipped
}

func (m *managedPlan) check(emitUnkeyed bool) error {
	pubs := []pubState{m.main}
	if emitUnkeyed {
		pubs = append(pubs, m.insert)
	}
	for _, ps := range pubs {
		switch {
		case !ps.exists && !m.canCreate:
			return codedError(CodePublicationPrivilege, ErrPublicationPrivilege, fmt.Sprintf(
				"creating publication %s needs CREATE on the database (GRANT CREATE ON DATABASE … TO …), "+
					"or use source.publication.mode existing with a DBA-created publication", ps.name))
		case ps.exists && !ps.owned:
			return codedError(CodePublicationPrivilege, ErrPublicationPrivilege, fmt.Sprintf(
				"publication %s is owned by another role; managed mode must own it", ps.name))
		case ps.exists && ps.allTables:
			return codedError(CodePublicationConflict, ErrPublicationConflict, fmt.Sprintf(
				"publication %s is FOR ALL TABLES; drop it or use source.publication.mode existing", ps.name))
		}
	}
	var notOwned []string
	for _, t := range slices.Concat(m.keyed, m.unkeyed) {
		if !t.Owned {
			notOwned = append(notOwned, t.QualifiedName())
		}
	}
	if len(notOwned) > 0 {
		return codedError(CodePublicationPrivilege, ErrPublicationPrivilege, fmt.Sprintf(
			"managed mode must own every matched table; not owned: %s (exclude them, change the owner "+
				"or use source.publication.mode existing)", strings.Join(notOwned, ", ")))
	}
	return nil
}

type ddl struct {
	sql    string
	change string
}

func (m *managedPlan) statements(emitUnkeyed bool) []ddl {
	var create, drops, adds []ddl
	type target struct {
		ps      pubState
		publish string
		want    []Table
	}
	targets := []target{{ps: m.main, publish: publishAll, want: m.keyed}}
	if emitUnkeyed {
		targets = append(targets, target{ps: m.insert, publish: publishInsert, want: m.unkeyed})
	}
	for _, tg := range targets {
		name := pgx.Identifier{tg.ps.name}.Sanitize()
		opts := "(publish = '" + tg.publish + "', publish_via_partition_root = true)"
		switch {
		case !tg.ps.exists:
			create = append(create, ddl{"CREATE PUBLICATION " + name + " WITH " + opts, "create " + tg.ps.name})
		case !tg.ps.hasOptions(tg.publish):
			create = append(create, ddl{"ALTER PUBLICATION " + name + " SET " + opts, "set options of " + tg.ps.name})
		}
		add, drop := diffTables(tg.ps.members, tg.want)
		if len(drop) > 0 {
			drops = append(drops, ddl{
				"ALTER PUBLICATION " + name + " DROP TABLE " + tableList(drop),
				"drop " + qualifiedNames(drop) + " from " + tg.ps.name,
			})
		}
		if len(add) > 0 {
			adds = append(adds, ddl{
				"ALTER PUBLICATION " + name + " ADD TABLE " + tableList(add),
				"add " + qualifiedNames(add) + " to " + tg.ps.name,
			})
		}
	}
	return slices.Concat(create, drops, adds)
}

func diffTables(have, want []Table) (add, drop []Table) {
	in := func(ts []Table, oid uint32) bool {
		return slices.ContainsFunc(ts, func(t Table) bool { return t.OID == oid })
	}
	for _, t := range want {
		if !in(have, t.OID) {
			add = append(add, t)
		}
	}
	for _, t := range have {
		if !in(want, t.OID) {
			drop = append(drop, t)
		}
	}
	return add, drop
}

func tableList(ts []Table) string {
	q := make([]string, len(ts))
	for i := range ts {
		q[i] = ts[i].sanitize()
	}
	return strings.Join(q, ", ")
}

func qualifiedNames(ts []Table) string {
	q := make([]string, len(ts))
	for i := range ts {
		q[i] = ts[i].QualifiedName()
	}
	return strings.Join(q, ", ")
}

func mapDDLError(sql string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42501" {
		e := codedError(CodePublicationPrivilege, ErrPublicationPrivilege, sql)
		e.cause = err
		return e
	}
	return fmt.Errorf("pgsql: %s: %w", sql, err)
}

func (p *Publications) Drop(ctx context.Context, source string) error {
	if err := validateSource(source, true); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, p.o.queryTimeout)
	defer cancel()
	sql := "DROP PUBLICATION IF EXISTS " + pgx.Identifier{PublicationName(source)}.Sanitize() + ", " +
		pgx.Identifier{InsertPublicationName(source)}.Sanitize()
	return p.pool.withConn(ctx, func(conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, sql); err != nil {
			return mapDDLError(sql, err)
		}
		return nil
	})
}
