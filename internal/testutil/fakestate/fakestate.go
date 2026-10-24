// SPDX-License-Identifier: Apache-2.0

package fakestate

import (
	"context"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

// Store is an in-memory state store.
type Store struct {
	mu      sync.Mutex
	state   pipeline.State
	has     bool
	saves   int
	loadErr error
	saveErr error
}

// New returns a store without state.
func New() *Store { return &Store{} }

func NewWith(s pipeline.State) *Store {
	return &Store{state: s.Clone(), has: true}
}

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

func (st *Store) State() (pipeline.State, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.state.Clone(), st.has
}

func (st *Store) Saves() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.saves
}

func (st *Store) FailLoad(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.loadErr = err
}

func (st *Store) FailSave(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.saveErr = err
}
