// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"fmt"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/schema"
)

// Schemas is the schema version registry of the stream, safe for
// concurrent use: the reader observes Relation messages and the snapshotter
// reads the version its `r` events carry. Tables are keyed by their
// Postgres "<schema>.<table>" (schema.Relation.QualifiedName).
type Schemas struct {
	mu    sync.Mutex // protects every field below
	reg   *schema.Registry
	floor uint64
	seen  map[string]bool
}

// NewSchemas returns a registry seeded with the persisted state (already
// reinitialised for --reinit-state by the startup procedure).
func NewSchemas(state schema.RegistryState) (*Schemas, error) {
	reg := schema.NewRegistry()
	if err := reg.Seed(state); err != nil {
		return nil, fmt.Errorf("stream: seed schema versions: %w", err)
	}
	return &Schemas{reg: reg, floor: state.Floor, seen: map[string]bool{}}, nil
}

// Observe records a Relation message (schema.Registry.Observe). first
// reports the table's first observation in this process, after which its
// shape should be persisted even without a change.
func (s *Schemas) Observe(rel *schema.Relation) (obs schema.Observation, first bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obs = s.reg.Observe(rel)
	first = !s.seen[obs.Table]
	s.seen[obs.Table] = true
	return obs, first
}

// Version returns the table's current schema version, or the version a
// table not seen yet will receive (at least 1, at least the reinit floor).
func (s *Schemas) Version(table string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.reg.Version(table); ok {
		return max(v, 1)
	}
	return max(s.floor, 1)
}

// State exports the registry for the state file.
func (s *Schemas) State() schema.RegistryState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reg.State()
}
