// SPDX-License-Identifier: Apache-2.0

// Package statefile implements pipeline.StateStore over a JSON file at
// state.path. Save replaces the file atomically: it writes a
// temporary file in the same directory, fsyncs it, renames it over the state
// file and fsyncs the directory, so a crash leaves either the old or the new
// file. Load never overwrites a corrupt file.
//
//	st, err := statefile.New("/var/lib/wake/state.json")
//	s, err := st.Load(ctx) // pipeline.ErrStateNotFound on first start
package statefile
