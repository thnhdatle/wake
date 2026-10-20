// SPDX-License-Identifier: Apache-2.0

// Package metrics holds Wake's Prometheus instrumentation: the shared
// histogram buckets, a registry with the Go, process and build-info
// collectors, a label capper for labels that come from user data, and the
// collector types the app layer reaches through its own small interfaces
// (lag.Observer, lag.LatencyObserver, stream.ReaderMetrics, snapshot.Metrics)
// plus SinkMetrics for the sinks. The types satisfy those interfaces
// structurally; this package does not import internal/app.
//
//	reg, err := metrics.NewRegistry(buildinfo.Get())
//	lagMetrics, err := metrics.NewLag(reg)
//	tables := metrics.NewCapper(metrics.DefaultLabelLimit)
//	snap, err := metrics.NewSnapshot(reg, metrics.WithTableCapper(tables))
package metrics
