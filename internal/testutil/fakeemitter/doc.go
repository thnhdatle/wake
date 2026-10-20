// SPDX-License-Identifier: Apache-2.0

// Package fakeemitter is an in-memory pipeline.MessageEmitter for tests: it
// returns increasing LSNs, records every emitted message and attempt, and can
// inject errors.
//
//	e := fakeemitter.New(0x1000)
//	at, err := e.EmitMessage(ctx, "wake.heartbeat", []byte(`{"ts":1}`)) // 0/1100
//	e.Fail(errors.New("connection reset"))
//	n, err := e.WaitAttempts(ctx, 2)
//	msgs := e.Messages()
package fakeemitter
