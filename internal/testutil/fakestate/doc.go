// SPDX-License-Identifier: Apache-2.0

// Package fakestate is an in-memory pipeline.StateStore for tests. It keeps
// deep copies, so a test cannot change the stored state by accident, and
// lets the test inject Load and Save errors.
//
//	st := fakestate.New()
//	_, err := st.Load(ctx) // pipeline.ErrStateNotFound
//	_ = st.Save(ctx, pipeline.NewState("wake_app", "7300000000000000001"))
package fakestate
