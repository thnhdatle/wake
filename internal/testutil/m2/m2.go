// SPDX-License-Identifier: Apache-2.0

package m2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/thnhdatle/wake/pkg/event"
)

// Table maps the canonical key JSON of each row to its image.
type Table map[string]event.Row

// State maps "<schema>.<table>" (the events' source names) to the table.
type State map[string]Table

// Stats counts what Add saw.
type Stats struct {
	// Events are the data events kept for replay (after deduplication).
	Events int
	// Duplicates are data events whose id was already added.
	Duplicates int
	// Tombstones are Kafka tombstones (empty values), skipped.
	Tombstones int
	// Other are schema-change events and forwarded messages, skipped.
	Other int
}

// Replayer collects payloads and computes the M2 final state. The zero value
// is ready to use; it is not safe for concurrent use.
type Replayer struct {
	events []entry
	seen   map[string]bool
	stats  Stats
}

type entry struct {
	seq event.Seq
	ev  *event.Event
}

// Add decodes one transport payload (a Kafka record value, a NATS message
// data or one element of a webhook array). Tombstones, schema changes and
// messages are counted and skipped; a payload that is not a valid CDC event is an
// error.
func (r *Replayer) Add(value []byte) error {
	if event.IsKafkaTombstone(value) {
		r.stats.Tombstones++
		return nil
	}
	kind, err := event.KindOf(value)
	if err != nil {
		return fmt.Errorf("m2: classify payload: %w", err)
	}
	if kind != event.KindEvent {
		r.stats.Other++
		return nil
	}
	e, err := event.Decode(value)
	if err != nil {
		return fmt.Errorf("m2: decode event: %w", err)
	}
	if r.seen == nil {
		r.seen = map[string]bool{}
	}
	if r.seen[e.ID] {
		r.stats.Duplicates++
		return nil
	}
	r.seen[e.ID] = true
	seq, err := e.Seq()
	if err != nil {
		return fmt.Errorf("m2: event %s: %w", e.ID, err)
	}
	r.events = append(r.events, entry{seq: seq, ev: e})
	r.stats.Events++
	return nil
}

// Stats returns the counters of the payloads added so far.
func (r *Replayer) Stats() Stats { return r.stats }

// State applies the collected events per key in seq order (M2) and returns
// the final state. It may be called again after more Adds.
func (r *Replayer) State() (State, error) {
	evs := slices.Clone(r.events)
	slices.SortStableFunc(evs, func(a, b entry) int { return event.Compare(a.seq, b.seq) })
	st := State{}
	for _, en := range evs {
		e := en.ev
		name := e.Source.Schema + "." + e.Source.Table
		t := st[name]
		if t == nil {
			t = Table{}
			st[name] = t
		}
		if err := apply(t, e); err != nil {
			return nil, fmt.Errorf("m2: event %s: %w", e.ID, err)
		}
	}
	return st, nil
}

// errNoKey reports an update or delete without a key.
var errNoKey = errors.New("update or delete without key")

func apply(t Table, e *event.Event) error {
	if e.Op == event.OpTruncate {
		clear(t)
		return nil
	}
	key, err := rowKey(e)
	if err != nil {
		return err
	}
	switch e.Op {
	case event.OpCreate, event.OpRead:
		t[key] = cloneRow(e.After)
	case event.OpUpdate:
		prevKey := key
		if e.KeyChanged() {
			if prevKey, err = beforeKey(e); err != nil {
				return err
			}
		}
		prev := t[prevKey]
		delete(t, prevKey)
		row := cloneRow(e.After)
		ev := *e
		ev.After = row
		event.MergeUnchanged(prev, &ev)
		t[key] = ev.After
	case event.OpDelete:
		delete(t, key)
	case event.OpTruncate:
		clear(t) // handled above; kept for exhaustiveness
	default:
		// Unknown ops are ignored, as consumers must (CDC event contract).
	}
	return nil
}

// rowKey is the canonical key of the event's row; events without a key
// (keyless tables) are keyed by their id.
func rowKey(e *event.Event) (string, error) {
	if e.Key == nil {
		if e.Op == event.OpUpdate || e.Op == event.OpDelete {
			return "", errNoKey
		}
		return "id:" + e.ID, nil
	}
	return CanonicalKey(e.Key)
}

// beforeKey is the canonical key of the before image restricted to the key
// columns.
func beforeKey(e *event.Event) (string, error) {
	k := make(event.Row, len(e.Key))
	for col := range e.Key {
		v, ok := e.Before[col]
		if !ok {
			return "", fmt.Errorf("before lacks key column %q", col)
		}
		k[col] = v
	}
	return CanonicalKey(k)
}

func cloneRow(r event.Row) event.Row {
	if r == nil {
		return nil
	}
	out := make(event.Row, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// CanonicalKey renders a key (or any row) as canonical JSON: object keys
// sorted, values compacted, so equal keys compare equal as strings.
func CanonicalKey(row event.Row) (string, error) {
	cols := make([]string, 0, len(row))
	for c := range row {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	var b bytes.Buffer
	b.WriteByte('{')
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(',')
		}
		name, err := json.Marshal(c)
		if err != nil {
			return "", fmt.Errorf("m2: column name: %w", err)
		}
		b.Write(name)
		b.WriteByte(':')
		if err := json.Compact(&b, row[c]); err != nil {
			return "", fmt.Errorf("m2: column %s: %w", c, err)
		}
	}
	b.WriteByte('}')
	return b.String(), nil
}

// FromRows builds a Table from source rows (e.g. Postgres to_jsonb), keyed
// by the given key columns.
func FromRows(keyCols []string, rows []event.Row) (Table, error) {
	t := make(Table, len(rows))
	for _, r := range rows {
		k := make(event.Row, len(keyCols))
		for _, c := range keyCols {
			v, ok := r[c]
			if !ok {
				return nil, fmt.Errorf("m2: row lacks key column %q", c)
			}
			k[c] = v
		}
		key, err := CanonicalKey(k)
		if err != nil {
			return nil, err
		}
		t[key] = r
	}
	return t, nil
}

// Diff compares the replayed table got with the source rows want and
// describes every difference: rows missing at the sink, rows the source no
// longer has, and rows whose columns differ (values compared as compact
// JSON). It returns nil when the tables are equal.
func Diff(got, want Table) []string {
	var out []string
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			out = append(out, "missing at sink: "+k)
			continue
		}
		if d := rowDiff(g, w); d != "" {
			out = append(out, "different "+k+": "+d)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			out = append(out, "extra at sink: "+k)
		}
	}
	sort.Strings(out)
	return out
}

func rowDiff(got, want event.Row) string {
	g, err := CanonicalKey(got)
	if err != nil {
		return err.Error()
	}
	w, err := CanonicalKey(want)
	if err != nil {
		return err.Error()
	}
	if g == w {
		return ""
	}
	return "sink " + g + " source " + w
}
