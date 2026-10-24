// SPDX-License-Identifier: Apache-2.0

package pgtest

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	EnvVersion     = "WAKE_TEST_PG"
	DefaultVersion = "17"
	Password       = "wake"

	superuser      = "postgres"
	startupTimeout = 3 * time.Minute
	opTimeout      = time.Minute
)

var ErrBadVersion = errors.New("pgtest: bad postgres version")

func Image() (string, error) {
	return imageFor(os.Getenv(EnvVersion))
}

func imageFor(v string) (string, error) {
	if v == "" {
		v = DefaultVersion
	}
	if strings.ContainsAny(v, "/:@ ") || len(v) > 32 {
		return "", fmt.Errorf("%w: %q", ErrBadVersion, v)
	}
	return "postgres:" + v, nil
}

func Command() []string {
	return []string{
		"postgres",
		"-c", "wal_level=logical",
		"-c", "max_wal_senders=20",
		"-c", "max_replication_slots=20",
		"-c", "max_connections=80",
		"-c", "shared_buffers=32MB",
		"-c", "work_mem=4MB",
		"-c", "maintenance_work_mem=16MB",
		"-c", "fsync=off",
		"-c", "full_page_writes=off",
	}
}

// Server is a running Postgres container.
type Server struct {
	ctr      *tcpostgres.PostgresContainer
	hostPort string
	Image    string
	seq      atomic.Uint64
}

// Start runs the container and waits until it accepts connections.
func Start(ctx context.Context) (*Server, error) {
	img, err := Image()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	ctr, err := tcpostgres.Run(ctx, img,
		tcpostgres.WithUsername(superuser), tcpostgres.WithPassword(Password), tcpostgres.WithDatabase(superuser),
		testcontainers.WithCmd(Command()...),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		if ctr != nil {
			_ = testcontainers.TerminateContainer(ctr) //nolint:errcheck // reporting the start error
		}
		return nil, fmt.Errorf("pgtest: start %s: %w", img, err)
	}
	hostPort, err := ctr.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr) //nolint:errcheck // reporting the endpoint error
		return nil, fmt.Errorf("pgtest: endpoint: %w", err)
	}
	return &Server{ctr: ctr, hostPort: hostPort, Image: img}, nil
}

func (s *Server) Terminate() error {
	if err := testcontainers.TerminateContainer(s.ctr); err != nil {
		return fmt.Errorf("pgtest: terminate: %w", err)
	}
	return nil
}

// Main starts the server, runs the tests and terminates the container.
func Main(m *testing.M, srv **Server) int {
	s, err := Start(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	*srv = s
	code := m.Run()
	if err := s.Terminate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if code == 0 {
			code = 1
		}
	}
	return code
}

func (s *Server) DSN(database, role string) string {
	return dsn(s.hostPort, database, role)
}

func dsn(hostPort, database, role string) string {
	u := url.URL{
		Scheme: "postgres", User: url.UserPassword(role, Password), Host: hostPort, Path: "/" + database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

func (s *Server) unique(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, s.seq.Add(1))
}

// Database is a database created for one test.
type Database struct {
	srv  *Server
	Name string
	DSN  string
}

// NewDatabase creates a fresh database that is dropped when the test ends.
func (s *Server) NewDatabase(tb testing.TB) *Database {
	tb.Helper()
	name := s.unique("db")
	admin := s.DSN(superuser, superuser)
	execOn(tb, admin, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()
		conn, err := pgx.Connect(ctx, admin)
		if err != nil {
			tb.Errorf("pgtest: drop database %s: %v", name, err)
			return
		}
		defer conn.Close(ctx) //nolint:errcheck // cleanup
		_, err = conn.Exec(ctx, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
			WHERE database = $1 AND active_pid IS NOT NULL`, name)
		if err == nil {
			_, err = conn.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
				WHERE database = $1`, name)
		}
		if err == nil {
			_, err = conn.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		}
		if err != nil {
			tb.Logf("pgtest: drop database %s: %v", name, err)
		}
	})
	return &Database{srv: s, Name: name, DSN: s.DSN(name, superuser)}
}

func (d *Database) RoleDSN(role string) string { return d.srv.DSN(d.Name, role) }

func (d *Database) CreateRole(tb testing.TB, prefix, attrs string) string {
	tb.Helper()
	name := d.srv.unique(prefix)
	d.Exec(tb, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s' %s", pgx.Identifier{name}.Sanitize(), Password, attrs))
	return name
}

func (d *Database) Exec(tb testing.TB, sql string, args ...any) {
	tb.Helper()
	execOn(tb, d.DSN, sql, args...)
}

func (d *Database) Conn(tb testing.TB) *pgx.Conn {
	tb.Helper()
	return Connect(tb, d.DSN)
}

func Connect(tb testing.TB, dsn string) *pgx.Conn {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()
		_ = conn.Close(ctx) //nolint:errcheck // cleanup
	})
	return conn
}

func execOn(tb testing.TB, dsn, sql string, args ...any) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(tb.Context()), opTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(tb, err)
	defer conn.Close(ctx) //nolint:errcheck // short-lived connection
	_, err = conn.Exec(ctx, sql, args...)
	require.NoError(tb, err, sql)
}
