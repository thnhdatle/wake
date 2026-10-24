// SPDX-License-Identifier: Apache-2.0

package fakespill

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

var ErrRemoved = errors.New("fakespill: file removed")

// Store is an in-memory spill store.
type Store struct {
	mu         sync.Mutex
	files      map[string]*File
	created    int
	removed    int
	removeAlls int
	createErr  error
	appendErr  error
}

// New returns an empty store.
func New() *Store { return &Store{files: map[string]*File{}} }

func (s *Store) Create(ctx context.Context, name string) (pipeline.SpillFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return nil, s.createErr
	}
	f := &File{store: s, name: name}
	s.files[name] = f
	s.created++
	return f, nil
}

func (s *Store) RemoveAll(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeAlls++
	for _, f := range s.files {
		f.removed = true
	}
	clear(s.files)
	return nil
}

func (s *Store) Seed(name string, records ...[]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := &File{store: s, name: name}
	for _, r := range records {
		f.records = append(f.records, slices.Clone(r))
		f.size += int64(len(r))
	}
	s.files[name] = f
}

func (s *Store) Files() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.files))
}

func (s *Store) Counts() (created, removed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.created, s.removed
}

func (s *Store) RemoveAlls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removeAlls
}

func (s *Store) FailCreate(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createErr = err
}

func (s *Store) FailAppend(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendErr = err
}

type File struct {
	store   *Store
	name    string
	records [][]byte
	size    int64
	removed bool
}

func (f *File) Append(ctx context.Context, rec []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if f.removed {
		return ErrRemoved
	}
	if f.store.appendErr != nil {
		return f.store.appendErr
	}
	f.records = append(f.records, slices.Clone(rec))
	f.size += int64(len(rec))
	return nil
}

func (f *File) Replay(ctx context.Context, fn func(rec []byte) error) error {
	f.store.mu.Lock()
	if f.removed {
		f.store.mu.Unlock()
		return ErrRemoved
	}
	recs := slices.Clone(f.records)
	f.store.mu.Unlock()
	var scratch []byte
	for _, r := range recs {
		if err := ctx.Err(); err != nil {
			return err
		}
		scratch = append(scratch[:0], r...)
		if err := fn(scratch); err != nil {
			return err
		}
		clear(scratch)
	}
	return nil
}

func (f *File) Size() int64 {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return f.size
}

func (f *File) Remove() error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if f.removed {
		return ErrRemoved
	}
	f.removed = true
	delete(f.store.files, f.name)
	f.store.removed++
	return nil
}
