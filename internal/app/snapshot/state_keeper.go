// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/thnhdatle/wake/internal/domain/lsn"
	"github.com/thnhdatle/wake/internal/domain/pipeline"
	"github.com/thnhdatle/wake/internal/domain/schema"
)

// StateKeeper owns the state file at run time and saves every update.
type StateKeeper struct {
	store pipeline.StateStore

	mu    sync.Mutex
	state pipeline.State

	changed chan struct{}
}

// NewStateKeeper returns a keeper whose current state is initial.
func NewStateKeeper(store pipeline.StateStore, initial *pipeline.State) (*StateKeeper, error) {
	if store == nil || initial == nil {
		return nil, fmt.Errorf("%w: nil state store or state", ErrInvalidOption)
	}
	return &StateKeeper{store: store, state: initial.Clone(), changed: make(chan struct{}, 1)}, nil
}

func (k *StateKeeper) State() pipeline.State {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.state.Clone()
}

func (k *StateKeeper) Changed() <-chan struct{} { return k.changed }

func (k *StateKeeper) update(ctx context.Context, fn func(s *pipeline.State) (bool, error)) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	next := k.state.Clone()
	changed, err := fn(&next)
	if err != nil || !changed {
		return err
	}
	if err := k.store.Save(ctx, next); err != nil {
		return fmt.Errorf("snapshot: save state: %w", err)
	}
	k.state = next
	return nil
}

func (k *StateKeeper) notify() {
	select {
	case k.changed <- struct{}{}:
	default:
	}
}

func (k *StateKeeper) SaveSchemas(ctx context.Context, rs schema.RegistryState) error {
	return k.update(ctx, func(s *pipeline.State) (bool, error) {
		setRegistryState(s, &rs)
		return true, nil
	})
}

func (k *StateKeeper) EnqueueTables(ctx context.Context, tables []string) error {
	added := false
	err := k.update(ctx, func(s *pipeline.State) (bool, error) {
		for _, t := range tables {
			if !slices.Contains(s.Snapshot.Queue, t) {
				s.Snapshot.Queue = append(s.Snapshot.Queue, t)
				added = true
			}
		}
		return added, nil
	})
	if err == nil && added {
		k.notify()
	}
	return err
}

func (k *StateKeeper) EnqueueRequests(ctx context.Context, tables []string, commit lsn.LSN) error {
	added := false
	err := k.update(ctx, func(s *pipeline.State) (bool, error) {
		if commit <= s.Snapshot.TriggerLSN || len(tables) == 0 {
			return false, nil
		}
		s.Snapshot.Queue = append(s.Snapshot.Queue, tables...)
		s.Snapshot.TriggerLSN = commit
		added = true
		return true, nil
	})
	if err == nil && added {
		k.notify()
	}
	return err
}

type work struct {
	run    *pipeline.SnapshotRun
	queued string
}

func (k *StateKeeper) nextWork() (work, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.state.Snapshot.Runs) > 0 {
		r := k.state.Clone().Snapshot.Runs[0]
		return work{run: &r}, true
	}
	if len(k.state.Snapshot.Queue) > 0 {
		return work{queued: k.state.Snapshot.Queue[0]}, true
	}
	return work{}, false
}

func (k *StateKeeper) dropQueued(ctx context.Context, table string) error {
	return k.update(ctx, func(s *pipeline.State) (bool, error) {
		if len(s.Snapshot.Queue) == 0 || s.Snapshot.Queue[0] != table {
			return false, nil
		}
		s.Snapshot.Queue = s.Snapshot.Queue[1:]
		return true, nil
	})
}

func (k *StateKeeper) startRun(ctx context.Context, table string, chunkRows int) (pipeline.SnapshotRun, error) {
	var run pipeline.SnapshotRun
	err := k.update(ctx, func(s *pipeline.State) (bool, error) {
		if len(s.Snapshot.Queue) > 0 && s.Snapshot.Queue[0] == table {
			s.Snapshot.Queue = s.Snapshot.Queue[1:]
		}
		run = pipeline.SnapshotRun{
			ID: s.NextRunID(), Kind: pipeline.SnapshotKindTable, ChunkRows: chunkRows,
			Tables: []pipeline.TableProgress{{Table: table}},
		}
		s.Snapshot.Runs = append(s.Snapshot.Runs, run)
		return true, nil
	})
	return run, err
}

func runIndex(s *pipeline.State, id uint64) (int, error) {
	for i := range s.Snapshot.Runs {
		if s.Snapshot.Runs[i].ID == id {
			return i, nil
		}
	}
	return 0, fmt.Errorf("%w: run %d", ErrUnknownRun, id)
}

func (k *StateKeeper) allocChunk(ctx context.Context, id uint64) (uint64, error) {
	var c uint64
	err := k.update(ctx, func(s *pipeline.State) (bool, error) {
		i, err := runIndex(s, id)
		if err != nil {
			return false, err
		}
		c = s.Snapshot.Runs[i].NextChunk
		s.Snapshot.Runs[i].NextChunk++
		return true, nil
	})
	return c, err
}

func (k *StateKeeper) setProgress(
	ctx context.Context, id uint64, table string, pos json.RawMessage, rows uint64, done bool,
) error {
	return k.update(ctx, func(s *pipeline.State) (bool, error) {
		i, err := runIndex(s, id)
		if err != nil {
			return false, err
		}
		tables := s.Snapshot.Runs[i].Tables
		for j := range tables {
			if tables[j].Table == table {
				tables[j].Position, tables[j].Rows, tables[j].Done = slices.Clone(pos), rows, done
				return true, nil
			}
		}
		return false, fmt.Errorf("%w: table %q in run %d", ErrUnknownRun, table, id)
	})
}

func (k *StateKeeper) finishRun(ctx context.Context, id uint64) error {
	return k.update(ctx, func(s *pipeline.State) (bool, error) {
		i, err := runIndex(s, id)
		if err != nil {
			return false, err
		}
		s.Snapshot.Runs = slices.Delete(s.Snapshot.Runs, i, i+1)
		return true, nil
	})
}
