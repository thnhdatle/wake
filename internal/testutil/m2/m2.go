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

type Table map[string]event.Row

type State map[string]Table

type Stats struct {
	Events     int
	Duplicates int
	Tombstones int
	Other      int
}

// Replayer collects payloads and computes the final state.
type Replayer struct {
	events []entry
	seen   map[string]bool
	stats  Stats
}

type entry struct {
	seq event.Seq
	ev  *event.Event
}

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

func (r *Replayer) Stats() Stats { return r.stats }

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
		clear(t)
	default:
	}
	return nil
}

func rowKey(e *event.Event) (string, error) {
	if e.Key == nil {
		if e.Op == event.OpUpdate || e.Op == event.OpDelete {
			return "", errNoKey
		}
		return "id:" + e.ID, nil
	}
	return CanonicalKey(e.Key)
}

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

// FromRows builds a Table from source rows keyed by the given key columns.
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

// Diff compares the replayed table with the source rows and describes every difference.
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
