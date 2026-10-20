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

// StateKeeper owns the state file at run time: the stream
// reader saves schema versions through it and the snapshotter its runs,
// queue and progress, so that every save writes one consistent state. Each
// update is applied to a copy, saved with StateStore.Save and only then
// becomes current; a failed save leaves the current state unchanged. It is
// safe for concurrent use.
type StateKeeper struct {
	store pipeline.StateStore

	mu    sync.Mutex // protects state and serializes saves
	state pipeline.State

	changed chan struct{} // capacity 1: coalesced signal of queue additions
}

// NewStateKeeper returns a keeper whose current state is initial (as saved
// by Prepare).
func NewStateKeeper(store pipeline.StateStore, initial *pipeline.State) (*StateKeeper, error) {
	if store == nil || initial == nil {
		return nil, fmt.Errorf("%w: nil state store or state", ErrInvalidOption)
	}
	return &StateKeeper{store: store, state: initial.Clone(), changed: make(chan struct{}, 1)}, nil
}

// State returns a copy of the current state (for `wake status`).
func (k *StateKeeper) State() pipeline.State {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.state.Clone()
}

// Changed returns a channel that receives after tables were queued.
func (k *StateKeeper) Changed() <-chan struct{} { return k.changed }

// update applies fn to a copy of the state and saves it when fn reports a
// change.
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

// SaveSchemas persists the schema versions (stream.SchemaStore).
func (k *StateKeeper) SaveSchemas(ctx context.Context, rs schema.RegistryState) error {
	return k.update(ctx, func(s *pipeline.State) (bool, error) {
		setRegistryState(s, &rs)
		return true, nil
	})
}

// EnqueueTables queues table snapshots of added tables; tables
// already queued are not queued twice.
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

// EnqueueRequests queues the tables of the wake.snapshot requests of the
// transaction committed at commit. Requests at or before the latest queued
// one are replays of an unconfirmed transaction after a restart and are
// ignored; each new request queues its table (a re-snapshot after a running
// one).
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

// work is the next unit of snapshot work: an unfinished run (resumed first)
// or a queued table.
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

// dropQueued removes the head of the queue when it is table.
func (k *StateKeeper) dropQueued(ctx context.Context, table string) error {
	return k.update(ctx, func(s *pipeline.State) (bool, error) {
		if len(s.Snapshot.Queue) == 0 || s.Snapshot.Queue[0] != table {
			return false, nil
		}
		s.Snapshot.Queue = s.Snapshot.Queue[1:]
		return true, nil
	})
}

// startRun moves the queued table into a new table snapshot run with the
// next run_id.
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

// allocChunk returns the run's next chunk counter c and persists c+1
// before the chunk is emitted.
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

// setProgress persists a table's acknowledged position and rows.
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

// finishRun removes a completed run.
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
