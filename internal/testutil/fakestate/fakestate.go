// SPDX-License-Identifier: Apache-2.0

package fakestate

import (
	"context"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Store is the fake. It is safe for concurrent use.
type Store struct {
	mu      sync.Mutex // protects every field below
	state   pipeline.State
	has     bool
	saves   int
	loadErr error
	saveErr error
}

// New returns a store without state.
func New() *Store { return &Store{} }

// NewWith returns a store holding a copy of s.
func NewWith(s pipeline.State) *Store {
	return &Store{state: s.Clone(), has: true}
}

// Load returns a copy of the stored state, or pipeline.ErrStateNotFound.
func (st *Store) Load(_ context.Context) (pipeline.State, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.loadErr != nil {
		return pipeline.State{}, st.loadErr
	}
	if !st.has {
		return pipeline.State{}, pipeline.ErrStateNotFound
	}
	return st.state.Clone(), nil
}

// Save stores a copy of s.
func (st *Store) Save(_ context.Context, s pipeline.State) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.saveErr != nil {
		return st.saveErr
	}
	st.state, st.has = s.Clone(), true
	st.saves++
	return nil
}

// State returns a copy of the stored state and whether there is one.
func (st *Store) State() (pipeline.State, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.state.Clone(), st.has
}

// Saves returns the number of successful saves.
func (st *Store) Saves() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.saves
}

// FailLoad makes Load return err; nil restores it.
func (st *Store) FailLoad(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.loadErr = err
}

// FailSave makes Save return err; nil restores it.
func (st *Store) FailSave(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.saveErr = err
}
