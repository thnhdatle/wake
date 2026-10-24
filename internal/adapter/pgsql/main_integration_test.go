// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgsql_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/wake/internal/adapter/pgsql"
	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/testutil/pgtest"
)

var srv *pgtest.Server

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &srv)) }

const waitTimeout = 30 * time.Second

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func newPool(t *testing.T, dsn string, opts ...pgsql.PoolOption) *pgsql.Pool {
	t.Helper()
	p, err := pgsql.NewPool(dsn, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { p.Close(context.Background()) })
	return p
}

func newCatalog(t *testing.T, p *pgsql.Pool) *pgsql.Catalog {
	t.Helper()
	c, err := pgsql.NewCatalog(p)
	require.NoError(t, err)
	return c
}

func newReader(t *testing.T, p *pgsql.Pool) *pgsql.ChunkReader {
	t.Helper()
	r, err := pgsql.NewChunkReader(p)
	require.NoError(t, err)
	return r
}

func newPublications(t *testing.T, p *pgsql.Pool) *pgsql.Publications {
	t.Helper()
	pubs, err := pgsql.NewPublications(p)
	require.NoError(t, err)
	return pubs
}

func rowJSON(r change.Row) string {
	if r == nil {
		return "null"
	}
	b := []byte{'{'}
	for i, c := range r {
		if i > 0 {
			b = append(b, ',')
		}
		b = change.AppendString(b, c.Name)
		b = append(b, ':')
		b = append(b, c.Value...)
	}
	return string(append(b, '}'))
}

func readAll(t *testing.T, read func(pipeline.ChunkRequest) (pipeline.Chunk, error), schema, table string,
	limit int,
) ([]string, []pipeline.Chunk) {
	t.Helper()
	req := pipeline.ChunkRequest{Schema: schema, Table: table, Limit: limit}
	var rows []string
	var chunks []pipeline.Chunk
	for range 100_000 {
		c, err := read(req)
		require.NoError(t, err)
		require.LessOrEqual(t, len(c.Rows), limit)
		chunks = append(chunks, c)
		for _, r := range c.Rows {
			rows = append(rows, rowJSON(r.After))
		}
		if c.Done {
			return rows, chunks
		}
		require.NotEmpty(t, c.Rows, "a chunk that is not done has rows")
		req.After = c.Last
	}
	require.FailNow(t, "table never done")
	return nil, nil
}
