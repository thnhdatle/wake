// SPDX-License-Identifier: Apache-2.0

// Package pgtest runs one throw-away Postgres container per test package for
// integration tests (Testcontainers) and offers helpers on top of it: a fresh
// database per test, extra login roles, SQL execution and a raw logical
// replication client that decodes pgoutput with internal/domain/pgoutput.
//
// The image is postgres:<WAKE_TEST_PG> (default 17), started with
// wal_level=logical, max_wal_senders=20, max_replication_slots=20 and small
// memory settings, on a random host port. Run it from TestMain so that the
// container is terminated when the package's tests end:
//
//	var srv *pgtest.Server
//
//	func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &srv)) }
//
//	func TestX(t *testing.T) {
//		db := srv.NewDatabase(t)
//		db.Exec(t, "CREATE TABLE t (id int PRIMARY KEY)")
//	}
package pgtest
