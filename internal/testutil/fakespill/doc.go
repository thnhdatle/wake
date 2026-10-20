// SPDX-License-Identifier: Apache-2.0

// Package fakespill is an in-memory pipeline.SpillStore for tests: files
// live in memory, and the store records how many were created, removed and
// left behind, and can inject errors.
//
//	st := fakespill.New()
//	f, err := st.Create(ctx, "0000000001977818-760")
//	_ = f.Append(ctx, []byte("record"))
//	created, removed := st.Counts()
package fakespill
