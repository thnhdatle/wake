// SPDX-License-Identifier: Apache-2.0

// Package fakealert is an in-memory pipeline.AlertNotifier for tests that
// records every alert and can inject Notify errors.
//
//	n := fakealert.New()
//	_ = n.Notify(ctx, pipeline.Alert{Rule: "max_ack_age", State: pipeline.AlertStateFiring})
//	alerts := n.Alerts()
package fakealert
