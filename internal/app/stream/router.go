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

// SinkRoute is one configured sink with its `tables` globs.
type SinkRoute struct {
	Sink pipeline.Sink
	// Tables are the sink's globs per pipeline.TableFilter; empty routes no
	// table.
	Tables []string
}

// Router routes records to sinks and registers each delivery
// with the ack tracker before handing the record to the sink:
//
//   - an event goes to every sink whose globs match its "<schema>.<table>";
//   - a schema-change event goes to the sinks that receive its table;
//   - a forwarded message goes to every sink with at least one glob (a sink
//     with `tables: []` receives nothing).
//
// Each sink receives its own copy of the record with its own AckHandle; the
// payload and key bytes are shared. Per-key order per sink follows call
// order: callers route records in seq order, Router hands them to every sink
// in that order, and sinks keep per-partition-key order (LaneScheduler,
// Kafka partitions, NATS subjects). Send blocks while a sink's buffer is
// full, which stops the caller (backpressure).
//
// Router is safe for concurrent use. It caches the sink list per table; the
// cache holds one entry per captured table.
type Router struct {
	tracker  *AckTracker
	sinks    []pipeline.Sink
	filters  []pipeline.TableFilter
	messages []int // sinks that receive forwarded messages

	mu    sync.RWMutex // protects cache and bySource
	cache map[string][]int
	// bySource caches cache's entries by schema and table, so routing an
	// event does not build its "<schema>.<table>" name.
	bySource map[tableKey][]int
}

// tableKey is a table by its CDC event schema and table name.
type tableKey struct{ schema, table string }

// NewRouter validates the routes: every sink non-nil with a unique name and
// valid globs.
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

// SinkNames returns the names of the sinks that receive the table's events.
func (r *Router) SinkNames(table string) []string {
	idx := r.tableSinks(table)
	names := make([]string, len(idx))
	for i, s := range idx {
		names[i] = r.sinks[s].Name()
	}
	return names
}

// Route delivers rec, outside any group, to its sinks as part of tx. It
// returns the number of sinks it was handed to; zero means unrouted. If ctx
// ends while a sink blocks, the deliveries already registered stay pending,
// so tx is never confirmed (the process is shutting down).
func (r *Router) Route(ctx context.Context, tx TxHandle, rec pipeline.Record) (int, error) {
	return r.route(ctx, rec, func() (pipeline.AckToken, error) { return r.tracker.Add(tx) })
}

// RouteToGroup delivers rec to its sinks as part of group g (a snapshot
// chunk's `r` events).
func (r *Router) RouteToGroup(ctx context.Context, g Group, rec pipeline.Record) (int, error) {
	return r.route(ctx, rec, func() (pipeline.AckToken, error) { return r.tracker.AddToGroup(g) })
}

// Dispatch registers an assembled transaction, routes its records in order
// and seals it.
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
