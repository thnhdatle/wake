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

// ErrRemoved reports a call on a removed file.
var ErrRemoved = errors.New("fakespill: file removed")

// Store is the fake. It is safe for concurrent use.
type Store struct {
	mu         sync.Mutex // protects every field below
	files      map[string]*File
	created    int
	removed    int
	removeAlls int
	createErr  error
	appendErr  error
}

// New returns an empty store.
func New() *Store { return &Store{files: map[string]*File{}} }

// Create implements pipeline.SpillStore.
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

// RemoveAll implements pipeline.SpillStore: it deletes every file.
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

// Seed adds a file left behind by a crashed run.
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

// Files returns the names of the files that exist.
func (s *Store) Files() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.files))
}

// Counts returns the number of files created and removed with Remove.
func (s *Store) Counts() (created, removed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.created, s.removed
}

// RemoveAlls returns the number of RemoveAll calls.
func (s *Store) RemoveAlls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removeAlls
}

// FailCreate makes Create return err; nil restores it.
func (s *Store) FailCreate(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createErr = err
}

// FailAppend makes Append return err; nil restores it.
func (s *Store) FailAppend(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendErr = err
}

// File is one in-memory spill file.
type File struct {
	store   *Store
	name    string
	records [][]byte
	size    int64
	removed bool
}

// Append implements pipeline.SpillFile; it stores a copy of rec.
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

// Replay implements pipeline.SpillFile. fn receives a scratch copy that is
// overwritten after the call, like a file reader's buffer.
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

// Size implements pipeline.SpillFile.
func (f *File) Size() int64 {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return f.size
}

// Remove implements pipeline.SpillFile.
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
