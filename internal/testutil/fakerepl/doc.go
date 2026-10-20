// SPDX-License-Identifier: Apache-2.0

// Package fakerepl is an in-memory replication connection for tests: it
// implements pipeline.SlotManager and pipeline.ReplicationSource, plays
// scripted XLogData and keepalive messages to Receive, and records slot
// commands, StartReplication calls and standby status updates.
//
//	src := fakerepl.New(16)
//	_ = src.Push(ctx, pipeline.ReplicationMessage{Kind: pipeline.ReplicationMessageKeepalive})
//	msg, err := src.Receive(ctx)
//	statuses, err := src.WaitStatuses(ctx, 1)
package fakerepl
