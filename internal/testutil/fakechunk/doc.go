// SPDX-License-Identifier: Apache-2.0

// Package fakechunk is an in-memory pipeline.ChunkReader for tests: tables
// of rows keyed by an integer "id" column, read by keyset pagination
// (After is the JSON array [id]). ReadChunk sees the current rows and
// reports a configurable transaction snapshot; OpenExported sessions see the
// rows as they were when the session was opened, like an imported snapshot.
//
//	r := fakechunk.New()
//	r.Put("public.users", 1, `"a"`)
//	c, err := r.ReadChunk(ctx, pipeline.ChunkRequest{Schema: "public", Table: "users", Limit: 100})
package fakechunk
