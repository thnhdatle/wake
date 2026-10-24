// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

type SinkRoute struct {
	Sink   pipeline.Sink
	Tables []string
}

// Router routes records to sinks and registers each delivery with the ack tracker.
type Router struct {
	tracker  *AckTracker
	sinks    []pipeline.Sink
	filters  []pipeline.TableFilter
	messages []int

	mu       sync.RWMutex
	cache    map[string][]int
	bySource map[tableKey][]int
}

type tableKey struct{ schema, table string }

// NewRouter validates the routes.
func NewRouter(tracker *AckTracker, routes []SinkRoute) (*Router, error) {
	if tracker == nil {
		return nil, fmt.Errorf("%w: nil ack tracker", ErrInvalidOption)
	}
	r := &Router{tracker: tracker, cache: map[string][]int{}, bySource: map[tableKey][]int{}}
	seen := make(map[string]bool, len(routes))
	for i, rt := range routes {
		if rt.Sink == nil {
			return nil, fmt.Errorf("%w: sink %d is nil", ErrInvalidOption, i)
		}
		name := rt.Sink.Name()
		if seen[name] {
			return nil, fmt.Errorf("%w: duplicate sink name %q", ErrInvalidOption, name)
		}
		seen[name] = true
		f, err := pipeline.NewTableFilter(rt.Tables)
		if err != nil {
			return nil, fmt.Errorf("stream: sink %q: %w", name, err)
		}
		r.sinks = append(r.sinks, rt.Sink)
		r.filters = append(r.filters, f)
		if !f.IsEmpty() {
			r.messages = append(r.messages, i)
		}
	}
	return r, nil
}

func (r *Router) SinkNames(table string) []string {
	idx := r.tableSinks(table)
	names := make([]string, len(idx))
	for i, s := range idx {
		names[i] = r.sinks[s].Name()
	}
	return names
}

// Route delivers rec to its sinks as part of tx.
func (r *Router) Route(ctx context.Context, tx TxHandle, rec pipeline.Record) (int, error) {
	return r.route(ctx, rec, func() (pipeline.AckToken, error) { return r.tracker.Add(tx) })
}

func (r *Router) RouteToGroup(ctx context.Context, g Group, rec pipeline.Record) (int, error) {
	return r.route(ctx, rec, func() (pipeline.AckToken, error) { return r.tracker.AddToGroup(g) })
}

// Dispatch registers an assembled transaction, routes its records in order and seals it.
func (r *Router) Dispatch(ctx context.Context, b *pipeline.TxBatch, readAt time.Time) (TxHandle, error) {
	tx, err := r.tracker.Begin(ctx, b.CommitLSN, b.EndLSN, readAt)
	if err != nil {
		return TxHandle{}, err
	}
	for i := range b.Records {
		if _, err := r.Route(ctx, tx, b.Records[i]); err != nil {
			return tx, err
		}
	}
	return tx, r.tracker.Seal(tx)
}

func (r *Router) route(ctx context.Context, rec pipeline.Record, add func() (pipeline.AckToken, error)) (int, error) {
	targets := r.targets(&rec)
	for _, s := range targets {
		tok, err := add()
		if err != nil {
			return 0, err
		}
		out := rec
		out.Ack = pipeline.NewAckHandle(r.tracker, tok)
		if err := r.sinks[s].Send(ctx, out); err != nil {
			return 0, fmt.Errorf("stream: send %s to sink %q: %w", rec.ID, r.sinks[s].Name(), err)
		}
	}
	return len(targets), nil
}

func (r *Router) targets(rec *pipeline.Record) []int {
	if rec.Kind == pipeline.RecordKindMessage {
		return r.messages
	}
	k := tableKey{schema: rec.Source.Schema, table: rec.Source.Table}
	r.mu.RLock()
	idx, ok := r.bySource[k]
	r.mu.RUnlock()
	if ok {
		return idx
	}
	idx = r.tableSinks(rec.Source.QualifiedName())
	r.mu.Lock()
	r.bySource[k] = idx
	r.mu.Unlock()
	return idx
}

func (r *Router) tableSinks(table string) []int {
	r.mu.RLock()
	idx, ok := r.cache[table]
	r.mu.RUnlock()
	if ok {
		return idx
	}
	for i, f := range r.filters {
		if f.Match(table) {
			idx = append(idx, i)
		}
	}
	idx = slices.Clip(idx)
	r.mu.Lock()
	r.cache[table] = idx
	r.mu.Unlock()
	return idx
}
