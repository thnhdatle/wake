// SPDX-License-Identifier: Apache-2.0

// Package fakecatalog is an in-memory pipeline.Catalog for tests with
// scriptable answers: fixed or queued Slot results and errors, server
// settings, publication tables and the current time. It records Slot and
// PublicationTables calls.
//
//	c := fakecatalog.New()
//	c.SetSlot(pipeline.SlotStatus{Exists: true, LagBytes: 1 << 20, WALStatus: "reserved"}, nil)
//	c.QueueSlot(fakecatalog.SlotResult{Err: errors.New("connection reset")})
//	st, err := c.Slot(ctx, "wake_app") // the queued error first, then the SetSlot answer
//	calls, err := c.WaitSlotCalls(ctx, 2)
package fakecatalog
