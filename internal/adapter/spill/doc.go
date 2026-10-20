// SPDX-License-Identifier: Apache-2.0

// Package spill implements pipeline.SpillStore over files in
// stream.spill_dir. A spill file `<dir>/<name>.spill` holds
// length-prefixed frames: a uint32 big-endian length followed by the record
// bytes. Files are never needed after a crash, so RemoveAll deletes every
// spill file at start.
//
//	st, err := spill.New("/var/lib/wake/spill")
//	_ = st.RemoveAll(ctx)
//	f, err := st.Create(ctx, "0000000001977818-760")
//	_ = f.Append(ctx, rec)
//	err = f.Replay(ctx, func(rec []byte) error { return nil })
//	_ = f.Remove()
package spill
