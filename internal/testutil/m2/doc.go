// SPDX-License-Identifier: Apache-2.0

// Package m2 computes the final sink state of measurement M2 in
// acceptance/criteria.yaml for integration and acceptance tests: it decodes
// CDC event payloads with
// pkg/event, applies the data events per key in seq order as upserts (`d`
// deletes; a `u` whose before key differs from its key deletes the before
// key first; Kafka tombstones are skipped; unchanged_columns keep the
// previous value; `t` empties the table) and compares the result with the
// source rows.
//
// Main types: Replayer (collects payloads), State and Table (the result).
// Rows are keyed by the canonical JSON of their key (CanonicalKey); events of
// keyless tables (key null) are keyed by their id, so each insert is a row.
//
//	var r m2.Replayer
//	for _, rec := range records {
//		if err := r.Add(rec.Value); err != nil { ... }
//	}
//	state, err := r.State()
//	diff := m2.Diff(state["public.items"], want)
package m2
