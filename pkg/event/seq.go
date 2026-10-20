// SPDX-License-Identifier: Apache-2.0

package event

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
)

// Seq is the position of an event in the total order of a source: the
// commit LSN, then K, with a schema-change event ahead of the data
// event that has the same LSN and K.
type Seq struct {
	// LSN is the numeric commit LSN (the consistent point or the HIGH watermark
	// LSN for snapshot events).
	LSN uint64
	// K is index_in_tx for stream and schema events and the run-wide counter n
	// for snapshot events.
	K uint64
	// Schema is true for the id of a schema-change event.
	Schema bool
}

// ParseSeq extracts the Seq from an event id: "<lsn>-<k>" for stream events,
// "<lsn>-r<run>-<n>" for snapshot events (K is n) and "<lsn>-s<k>" for
// schema-change events (Schema is true).
func ParseSeq(id string) (Seq, error) {
	lsnPart, suffix, ok := strings.Cut(id, "-")
	if !ok {
		return Seq{}, fmt.Errorf("%w: id %q has no '-'", ErrInvalid, id)
	}
	lsn, err := ParseLSN(lsnPart)
	if err != nil {
		return Seq{}, fmt.Errorf("id %q: %w", id, err)
	}
	seq := Seq{LSN: lsn}
	switch {
	case strings.HasPrefix(suffix, "r"):
		run, n, found := strings.Cut(suffix[1:], "-")
		if !found {
			return Seq{}, fmt.Errorf("%w: id %q: snapshot suffix needs run and counter", ErrInvalid, id)
		}
		if _, err = parseDecimal(run); err != nil {
			return Seq{}, fmt.Errorf("id %q: run: %w", id, err)
		}
		seq.K, err = parseDecimal(n)
	case strings.HasPrefix(suffix, "s"):
		seq.Schema = true
		seq.K, err = parseDecimal(suffix[1:])
	default:
		seq.K, err = parseDecimal(suffix)
	}
	if err != nil {
		return Seq{}, fmt.Errorf("id %q: %w", id, err)
	}
	return seq, nil
}

// Compare orders two sequences: -1 when a sorts before b, 0 when equal and 1
// when a sorts after b. LSNs are compared numerically, never as strings, then
// K; for equal LSN and K a schema-change sequence sorts first.
func Compare(a, b Seq) int {
	switch {
	case a.LSN != b.LSN:
		return cmp.Compare(a.LSN, b.LSN)
	case a.K != b.K:
		return cmp.Compare(a.K, b.K)
	case a.Schema == b.Schema:
		return 0
	case a.Schema:
		return -1
	}
	return 1
}

// parseDecimal parses a non-empty run of ASCII digits.
func parseDecimal(s string) (uint64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty number", ErrInvalid)
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("%w: %q is not a decimal number", ErrInvalid, s)
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q: %w", ErrInvalid, s, err)
	}
	return n, nil
}
