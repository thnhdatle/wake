// SPDX-License-Identifier: Apache-2.0

package stack

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/thnhdatle/wake/internal/testutil/m2"
	"github.com/thnhdatle/wake/pkg/event"
)

// TableName returns "<schema>.<table>" of an event.
func TableName(e *event.Event) string { return e.Source.Schema + "." + e.Source.Table }

// EventKey returns the canonical key JSON of an event ("null" without key).
func EventKey(e *event.Event) (string, error) {
	if e.Key == nil {
		return "null", nil
	}
	k, err := m2.CanonicalKey(e.Key)
	if err != nil {
		return "", fmt.Errorf("stack: event %s key: %w", e.ID, err)
	}
	return k, nil
}

// IntColumn reads an integer column of a row image; ok is false when the
// column is absent, null or not an integer.
func IntColumn(r event.Row, col string) (int64, bool) {
	raw, ok := r[col]
	if !ok {
		return 0, false
	}
	raw = bytes.TrimSpace(raw)
	if s, err := strconv.Unquote(string(raw)); err == nil {
		raw = []byte(s) // numeric is a JSON string
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// LostResult is the outcome of the M3 lost check.
type LostResult struct {
	Recorded int
	Lost     []Change
	// Unmatched counts events of the checked tables that match no recorded
	// change (snapshot reads, changes made outside the writers).
	Unmatched int
}

// changeKey identifies a change for M3 matching.
type changeKey struct {
	table, key, op string
	ver            int64
}

// FindLost implements M3 lost: every recorded change needs an event of the
// same table, key and ver (the after image's ver column for c and u, the
// before image's for d). A d event whose before image lacks the ver column
// (default replica identity) matches any recorded delete of its key.
// Duplicates (same id) are ignored. Only events of tables in changes count.
func FindLost(changes []Change, events []*event.Event, verCol string) (LostResult, error) {
	res := LostResult{Recorded: len(changes)}
	tables := map[string]bool{}
	for _, c := range changes {
		tables[c.Table] = true
	}
	idx, err := indexEvents(tables, events, verCol)
	if err != nil {
		return res, err
	}
	for _, c := range changes {
		k := changeKey{c.Table, c.Key, c.Op, c.Ver}
		dk := [2]string{c.Table, c.Key}
		switch {
		case idx.have[k] > 0:
			idx.have[k]--
		case c.Op == OpDelete && idx.deletesNoVer[dk] > 0:
			idx.deletesNoVer[dk]--
		default:
			res.Lost = append(res.Lost, c)
		}
	}
	for _, n := range idx.have {
		res.Unmatched += n
	}
	for _, n := range idx.deletesNoVer {
		res.Unmatched += n
	}
	return res, nil
}

// eventIndex counts distinct events by change identity.
type eventIndex struct {
	have         map[changeKey]int
	deletesNoVer map[[2]string]int
}

func indexEvents(tables map[string]bool, events []*event.Event, verCol string) (eventIndex, error) {
	idx := eventIndex{have: map[changeKey]int{}, deletesNoVer: map[[2]string]int{}}
	seen := map[string]bool{}
	for _, e := range events {
		table := TableName(e)
		if !tables[table] || seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		key, err := EventKey(e)
		if err != nil {
			return idx, err
		}
		op, img := string(e.Op), e.After
		switch e.Op {
		case event.OpDelete:
			img = e.Before
		case event.OpRead:
			op = OpCreate
		case event.OpCreate, event.OpUpdate, event.OpTruncate:
		}
		ver, ok := IntColumn(img, verCol)
		if !ok && e.Op == event.OpDelete {
			idx.deletesNoVer[[2]string{table, key}]++
			continue
		}
		idx.have[changeKey{table, key, op, ver}]++
	}
	return idx, nil
}

// Duplicates implements M3 duplicates: events whose id was already seen.
func Duplicates(events []*event.Event) int {
	seen := make(map[string]bool, len(events))
	n := 0
	for _, e := range events {
		if seen[e.ID] {
			n++
			continue
		}
		seen[e.ID] = true
	}
	return n
}

// OrderResult is the outcome of the M4 check.
type OrderResult struct {
	Checked    int
	Duplicates int
	Violations int
	Examples   []string
}

// OrderViolations implements M4 over events in delivery order: after
// dropping events whose id was already seen, an event whose seq is lower
// than that of an earlier delivered event of the same table and key.
func OrderViolations(events []*event.Event) (OrderResult, error) {
	var res OrderResult
	seen := make(map[string]bool, len(events))
	last := map[[2]string]event.Seq{}
	lastID := map[[2]string]string{}
	for _, e := range events {
		if seen[e.ID] {
			res.Duplicates++
			continue
		}
		seen[e.ID] = true
		seq, err := e.Seq()
		if err != nil {
			return res, fmt.Errorf("stack: event %s: %w", e.ID, err)
		}
		key, err := EventKey(e)
		if err != nil {
			return res, err
		}
		k := [2]string{TableName(e), key}
		res.Checked++
		if prev, ok := last[k]; ok && event.Compare(seq, prev) < 0 {
			res.Violations++
			if len(res.Examples) < 10 {
				res.Examples = append(res.Examples, fmt.Sprintf("%s %s: %s after %s", k[0], k[1], e.ID, lastID[k]))
			}
			continue
		}
		last[k], lastID[k] = seq, e.ID
	}
	return res, nil
}

// Pct returns 100*part/total, 100 when total is zero.
func Pct(part, total int) float64 {
	if total == 0 {
		return 100
	}
	return 100 * float64(part) / float64(total)
}
