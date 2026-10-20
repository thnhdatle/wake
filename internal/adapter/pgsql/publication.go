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

// Managed publication names and options.
const (
	publicationPrefix = "wake_"
	insertSuffix      = "_insert"
	maxIdentifier     = 63
	publishAll        = "insert, update, delete, truncate"
	publishInsert     = "insert"
)

// PublicationName returns the managed publication wake_<source>.
func PublicationName(source string) string { return publicationPrefix + source }

// InsertPublicationName returns the insert-only managed publication
// wake_<source>_insert of keyless tables (tables.emit_unkeyed).
func InsertPublicationName(source string) string { return publicationPrefix + source + insertSuffix }

// ManagedPublicationNames returns the publication_names of managed mode.
func ManagedPublicationNames(source string, emitUnkeyed bool) []string {
	if emitUnkeyed {
		return []string{PublicationName(source), InsertPublicationName(source)}
	}
	return []string{PublicationName(source)}
}

// validateSource checks source.name for the managed names: [a-z0-9_] and
// short enough that every managed name fits 63 bytes.
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

// Publications manages the managed publications and inspects existing ones.
// It is safe for concurrent use.
type Publications struct {
	pool *Pool
	o    options
}

// NewPublications validates the dependencies.
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

// ManagedConfig configures EnsureManaged.
type ManagedConfig struct {
	// Source is source.name.
	Source string
	// EmitUnkeyed is tables.emit_unkeyed: keyless tables go to
	// wake_<source>_insert.
	EmitUnkeyed bool
	// Resolver decides which tables are captured: the tables.include and
	// exclude globs, the identifier rule and the keyless rule
	// (pipeline.TablePolicy).
	Resolver pipeline.TableResolver
}

// SkippedTable is a table the globs match but managed mode does not
// publish.
type SkippedTable struct {
	Table  Table
	Reason pipeline.SkipReason
}

// SyncResult is the outcome of EnsureManaged.
type SyncResult struct {
	// Publications are the publication_names for START_REPLICATION.
	Publications []string
	// Keyed are the tables of wake_<source>, Unkeyed those of
	// wake_<source>_insert.
	Keyed   []Table
	Unkeyed []Table
	// Skipped are matched tables left out: keyless without emit_unkeyed
	// (table_keyless) and names that are not CDC event identifiers
	// (identifier_invalid).
	Skipped []SkippedTable
	// Changes lists the DDL changes made, e.g. "create wake_app" or
	// "add public.t to wake_app".
	Changes []string
}

// managedPlan is what EnsureManaged computes before changing anything.
type managedPlan struct {
	keyed, unkeyed []Table
	skipped        []SkippedTable
	main, insert   pubState
	canCreate      bool
}

// EnsureManaged creates or alters the managed publications so that
// wake_<source> holds exactly the captured keyed tables and, with
// EmitUnkeyed, wake_<source>_insert (publish = 'insert') the captured
// keyless ones, both with publish_via_partition_root = true. Keyless tables
// are never added to wake_<source>: the key check runs in the same
// transaction as the DDL and again after it, when the added tables are
// locked against key changes. It requires CREATE on the database when a
// publication must be created, ownership of existing managed publications
// and of every table to publish (publication_privilege).
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

// loadManagedPlan reads the candidates and both publications.
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

// dropLostKeys removes tables that lost their key after the candidates were
// read from wake_<source>, in the same transaction. ADD TABLE holds a SHARE
// UPDATE EXCLUSIVE lock on every added table until commit, which blocks the
// DDL that removes a key, so the re-read is final for them.
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

// classify splits the candidates by the resolver's decision: captured keyed
// tables, captured keyless tables (emit_unkeyed) and matched tables left
// out. Tables matching no glob are dropped silently.
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

// check verifies the privileges and that the publications can be managed.
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

// ddl is one statement with its change description.
type ddl struct {
	sql    string
	change string
}

// statements returns the DDL that brings the publications to the plan:
// creations and option fixes first, then removals from both publications,
// then additions (a table moving between them is never in both).
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

// diffTables returns the tables of want missing from have and the tables of
// have missing from want, by OID.
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

// mapDDLError translates a failed publication statement.
func mapDDLError(sql string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42501" {
		e := codedError(CodePublicationPrivilege, ErrPublicationPrivilege, sql)
		e.cause = err
		return e
	}
	return fmt.Errorf("pgsql: %s: %w", sql, err)
}

// Drop drops wake_<source> and wake_<source>_insert if they exist (`wake
// publication drop`).
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
