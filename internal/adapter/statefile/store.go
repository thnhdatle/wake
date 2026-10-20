// SPDX-License-Identifier: Apache-2.0

package statefile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	fileMode = 0o600
	dirMode  = 0o700
	tempGlob = ".state-*.tmp"
)

// fileOps are the filesystem steps of Save that a test replaces to simulate
// a crash between them.
type fileOps struct {
	syncFile func(f *os.File) error
	rename   func(oldPath, newPath string) error
	syncDir  func(dir string) error
}

func osOps() fileOps {
	return fileOps{
		syncFile: func(f *os.File) error { return f.Sync() },
		rename:   os.Rename,
		syncDir:  syncDir,
	}
}

// Store is a pipeline.StateStore over one JSON file. It is safe for
// concurrent use; saves are serialized.
type Store struct {
	path string
	ops  fileOps
	mu   sync.Mutex // serializes Save
}

var _ pipeline.StateStore = (*Store)(nil)

// New returns a store for the state file at path. It touches no file.
func New(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty state path", ErrInvalidPath)
	}
	return &Store{path: filepath.Clean(path), ops: osOps()}, nil
}

// Path returns the state file path.
func (s *Store) Path() string { return s.path }

// Load reads the state file. It returns pipeline.ErrStateNotFound when the
// file does not exist, ErrCorrupt when it is not valid state JSON and
// ErrUnsupportedVersion when its format_version is newer than this build's.
func (s *Store) Load(ctx context.Context) (pipeline.State, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.State{}, fmt.Errorf("load state: %w", err)
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return pipeline.State{}, pipeline.ErrStateNotFound
	}
	if err != nil {
		return pipeline.State{}, fmt.Errorf("read state file %s: %w", s.path, err)
	}
	var st pipeline.State
	if err := json.Unmarshal(data, &st); err != nil {
		return pipeline.State{}, fmt.Errorf("%w: %s: %w", ErrCorrupt, s.path, err)
	}
	switch {
	case st.FormatVersion > pipeline.StateFormatVersion:
		return pipeline.State{}, fmt.Errorf("%w: %s has version %d, this build reads up to %d",
			ErrUnsupportedVersion, s.path, st.FormatVersion, pipeline.StateFormatVersion)
	case st.FormatVersion < 1:
		return pipeline.State{}, fmt.Errorf("%w: %s has no valid format_version", ErrCorrupt, s.path)
	}
	return st, nil
}

// Save replaces the state file atomically with st and creates the parent
// directory if needed. On failure the previous file is intact and the
// temporary file is removed.
func (s *Store) Save(ctx context.Context, st pipeline.State) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	// Compact on purpose: Marshal keeps TableProgress.Position byte for byte,
	// while indenting would reformat the raw JSON value.
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	dir := filepath.Dir(s.path)
	if err = os.MkdirAll(dir, dirMode); err != nil {
		return fmt.Errorf("create state dir %s: %w", dir, err)
	}
	tmp, err := s.writeTemp(dir, data)
	if err != nil {
		return err
	}
	if err := s.ops.rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename state file into place: %w", err)
	}
	if err := s.ops.syncDir(dir); err != nil {
		return fmt.Errorf("sync state dir %s: %w", dir, err)
	}
	return nil
}

// writeTemp writes data to a new synced temporary file in dir and returns
// its path; on failure it removes the file.
func (s *Store) writeTemp(dir string, data []byte) (_ string, err error) {
	f, err := os.CreateTemp(dir, tempGlob)
	if err != nil {
		return "", fmt.Errorf("create temp state file in %s: %w", dir, err)
	}
	name := f.Name()
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(name)
		}
	}()
	if err = f.Chmod(fileMode); err != nil {
		return "", fmt.Errorf("chmod temp state file: %w", err)
	}
	if _, err = f.Write(data); err != nil {
		return "", fmt.Errorf("write temp state file: %w", err)
	}
	if err = s.ops.syncFile(f); err != nil {
		return "", fmt.Errorf("sync temp state file: %w", err)
	}
	if err = f.Close(); err != nil {
		return "", fmt.Errorf("close temp state file: %w", err)
	}
	return name, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // the state directory comes from the operator's configuration
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("sync dir: %w", err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close dir: %w", err)
	}
	return nil
}
