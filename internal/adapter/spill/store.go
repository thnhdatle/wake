// SPDX-License-Identifier: Apache-2.0

package spill

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	// Extension is the suffix of every spill file.
	Extension = ".spill"

	dirMode = 0o700
)

// Store keeps spill files in one directory. It is safe for concurrent use;
// every file is independent.
type Store struct {
	dir string

	mu   sync.Mutex // protects open
	open map[*File]struct{}
}

var _ pipeline.SpillStore = (*Store)(nil)

// New returns a store for dir. It touches no file; Create and RemoveAll
// create the directory.
func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty spill dir", ErrInvalidDir)
	}
	return &Store{dir: filepath.Clean(dir), open: map[*File]struct{}{}}, nil
}

// Dir returns the spill directory.
func (s *Store) Dir() string { return s.dir }

// Create makes `<dir>/<name>.spill`, truncating a file of the same name. The
// name must not be empty, "." or "..", nor contain a path separator.
func (s *Store) Create(ctx context.Context, name string) (pipeline.SpillFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("create spill file: %w", err)
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(s.dir, dirMode); err != nil {
		return nil, fmt.Errorf("create spill dir %s: %w", s.dir, err)
	}
	path := filepath.Join(s.dir, name+Extension)
	//nolint:gosec // the name is validated and joined under the spill dir
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create spill file %s: %w", path, err)
	}
	sf := newFile(f, path, s)
	s.mu.Lock()
	s.open[sf] = struct{}{}
	s.mu.Unlock()
	return sf, nil
}

func (s *Store) forget(f *File) {
	s.mu.Lock()
	delete(s.open, f)
	s.mu.Unlock()
}

// invalidateOpen marks every open file removed, as RemoveAll deletes them.
func (s *Store) invalidateOpen() {
	s.mu.Lock()
	files := make([]*File, 0, len(s.open))
	for f := range s.open {
		files = append(files, f)
	}
	clear(s.open)
	s.mu.Unlock()
	for _, f := range files {
		f.invalidate()
	}
}

// RemoveAll deletes every *.spill file of the directory and creates the
// directory if it is missing. Other files and subdirectories stay. Files
// still open through Create are closed and later report ErrRemoved.
func (s *Store) RemoveAll(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("remove spill files: %w", err)
	}
	if err := os.MkdirAll(s.dir, dirMode); err != nil {
		return fmt.Errorf("create spill dir %s: %w", s.dir, err)
	}
	s.invalidateOpen()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("list spill dir %s: %w", s.dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), Extension) {
			continue
		}
		path := filepath.Join(s.dir, e.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove spill file %s: %w", path, err)
		}
	}
	return nil
}

func validateName(name string) error {
	switch {
	case name == "" || name == "." || name == "..":
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	case strings.ContainsAny(name, `/\`+string(os.PathSeparator)) || strings.ContainsRune(name, 0):
		return fmt.Errorf("%w: %q contains a path separator", ErrInvalidName, name)
	}
	return nil
}
