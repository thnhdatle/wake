// SPDX-License-Identifier: Apache-2.0

// Package lag implements lag monitoring, alerting and the idle-source
// heartbeat.
//
// Monitor polls the slot's lag and wal_status every lag.interval, computes
// the age of the oldest unacknowledged delivery and counts the sinks blocked
// by non-retriable errors, then evaluates the alert rules slot_lag_warn,
// slot_lag_critical, max_ack_age and sink_fatal. A rule fires while its value
// is above its threshold; each state change is logged, exported and passed
// to the AlertNotifier exactly once, retrying failed notifications on the
// next poll. Heartbeat emits a `wake.heartbeat` logical message when no
// captured transaction was read for heartbeat.interval, so the confirmed LSN
// advances on an idle source. LatencyRecorder turns acknowledged events'
// ts_ms into end-to-end latencies.
//
// The package exports values through the Observer and LatencyObserver
// interfaces, which the metrics adapter implements; it does not depend on a
// metrics library.
//
//	mon, _ := lag.NewMonitor(catalog, "wake_app", tracker, clock.System{},
//		lag.WithInterval(15*time.Second), lag.WithSinks(kafka),
//		lag.WithNotifier(hook), lag.WithObserver(metrics), lag.WithLogger(logger))
//	hb, _ := lag.NewHeartbeat(emitter, clock.System{})
//	g.Go(func() error { return mon.Run(ctx) })
//	g.Go(func() error { return hb.Run(ctx) })
//	// reader, per transaction with captured changes:
//	hb.Touch(readAt)
package lag
