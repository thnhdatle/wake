// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"fmt"

	"github.com/thnhdatle/wake/internal/domain/change"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Table is a captured table as the snapshotter needs it.
type Table struct {
	// Name is the Postgres "<schema>.<table>": the key of state progress,
	// watermark payloads and snapshot requests.
	Name string
	// Schema and Table are the Postgres names for chunk reads.
	Schema string
	Table  string
	// Source is the events' source (tables.name_map applied).
	Source change.Source
}

// TableSource lists the captured tables.
type TableSource interface {
	CapturedTables(ctx context.Context) ([]Table, error)
}

// PublicationLister lists the tables of publications; pipeline.Catalog
// implements it.
type PublicationLister interface {
	PublicationTables(ctx context.Context, publications []string) ([]pipeline.TableInfo, error)
}

// CatalogTables is the TableSource of the published tables the resolver
// captures.
type CatalogTables struct {
	lister       PublicationLister
	publications []string
	resolver     pipeline.TableResolver
}

// NewCatalogTables validates the dependencies.
func NewCatalogTables(l PublicationLister, publications []string, r pipeline.TableResolver) (*CatalogTables, error) {
	if l == nil || r == nil || len(publications) == 0 {
		return nil, fmt.Errorf("%w: catalog tables need a lister, a resolver and publications", ErrInvalidOption)
	}
	return &CatalogTables{lister: l, publications: publications, resolver: r}, nil
}

// CapturedTables implements TableSource.
func (c *CatalogTables) CapturedTables(ctx context.Context) ([]Table, error) {
	infos, err := c.lister.PublicationTables(ctx, c.publications)
	if err != nil {
		return nil, fmt.Errorf("snapshot: list publication tables: %w", err)
	}
	out := make([]Table, 0, len(infos))
	for _, info := range infos {
		d := c.resolver.Resolve(info)
		if !d.Capture {
			continue
		}
		out = append(out, Table{
			Name: info.Schema + "." + info.Table, Schema: info.Schema, Table: info.Table, Source: d.Source,
		})
	}
	return out, nil
}

func findTable(tables []Table, name string) (Table, bool) {
	for _, t := range tables {
		if t.Name == name {
			return t, true
		}
	}
	return Table{}, false
}
