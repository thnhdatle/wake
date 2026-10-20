// SPDX-License-Identifier: Apache-2.0

package metrics

import "slices"

// The histogram buckets of the project are declared once, here.
var (
	latencyBuckets = []float64{
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
	}
	sizeBuckets = []float64{
		64, 256, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20, 64 << 20,
	}
)

// LatencyBuckets returns the bucket upper bounds, in seconds, of duration
// histograms: 1 ms to 60 s.
func LatencyBuckets() []float64 { return slices.Clone(latencyBuckets) }

// SizeBuckets returns the bucket upper bounds, in bytes, of size
// histograms: 64 B to 64 MiB.
func SizeBuckets() []float64 { return slices.Clone(sizeBuckets) }
