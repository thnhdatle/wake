// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"fmt"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/schema"
)

// Schemas is the schema version registry of the stream.
type Schemas struct {
	mu    sync.Mutex
	reg   *schema.Registry
	floor uint64
	seen  map[string]bool
}

// NewSchemas returns a registry seeded with the persisted state.
func NewSchemas(state schema.RegistryState) (*Schemas, error) {
	reg := schema.NewRegistry()
	if err := reg.Seed(state); err != nil {
		return nil, fmt.Errorf("stream: seed schema versions: %w", err)
	}
	return &Schemas{reg: reg, floor: state.Floor, seen: map[string]bool{}}, nil
}

func (s *Schemas) Observe(rel *schema.Relation) (obs schema.Observation, first bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obs = s.reg.Observe(rel)
	first = !s.seen[obs.Table]
	s.seen[obs.Table] = true
	return obs, first
}

func (s *Schemas) Version(table string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.reg.Version(table); ok {
		return max(v, 1)
	}
	return max(s.floor, 1)
}

func (s *Schemas) State() schema.RegistryState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reg.State()
}
