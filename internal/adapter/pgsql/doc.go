// SPDX-License-Identifier: Apache-2.0

// Package pgsql implements Wake's regular SQL access to the source database
// with pgx/v5:
//
//   - Pool: a bounded connection pool whose sessions use DateStyle=ISO,
//     IntervalStyle=postgres, client_encoding=UTF8, TimeZone=UTC and
//     extra_float_digits=1, like the replication connection, so chunk rows
//     and stream events render values identically.
//   - Catalog (pipeline.Catalog, stream.KeyColumnSource): server settings,
//     slot status with lag bytes, publication tables with key detection,
//     clock_timestamp(), primary key columns and replication usage.
//   - ChunkReader (pipeline.ChunkReader) and Session (pipeline.ChunkSession):
//     chunk reads by keyset pagination on the primary key (else the replica
//     identity index) or by ctid ranges (tables without either: FULL
//     without key and keyless tables; see ctid.go for the rewrite caveat).
//     Every column is read in text format and converted with
//     schema.AppendJSON using the catalog type OID (pgoutput's OID, also for
//     domains). ReadChunk runs in a READ ONLY REPEATABLE READ transaction:
//     pg_current_snapshot(), the chunk, then pg_xact_status of every xid of
//     the snapshot's xip list.
//     OpenExported imports the slot's exported snapshot with SET
//     TRANSACTION SNAPSHOT.
//   - Emitter (pipeline.MessageEmitter): pg_logical_emit_message.
//   - Publications: managed publications wake_<name> and
//     wake_<name>_insert (never adding a keyless table to wake_<name>),
//     existing-mode inspection and `wake publication drop`.
//   - RunCheck / Evaluate: the pre-flight checks of `wake check` as a
//     Report; the replication privilege is probed through the small
//     ReplicationProber interface so this package does not import the
//     replication adapter.
//
// SQL is parameterised; identifiers are quoted with pgx.Identifier. The only
// literal embedded in SQL is the exported snapshot name, which takes no
// parameter and is validated against the Postgres format first. No type
// starts goroutines.
//
//	pool, err := pgsql.NewPool(dsn, pgsql.WithMaxConns(8))
//	if err != nil { ... }
//	defer pool.Close(ctx)
//	chunks, _ := pgsql.NewChunkReader(pool)
//	chunk, err := chunks.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "orders", Limit: 10000})
package pgsql
